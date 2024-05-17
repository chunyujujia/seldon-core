/*
Copyright 2023 Seldon Technologies Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler/filters"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler/sorters"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

type DisabledServerScaler struct{}

func (scaler *DisabledServerScaler) Scalable(serverKey string, replicas int, model *store.ModelVersion) bool {
	return false
}

type ScalerConfig struct {
	scaleUpReplicaFilters []filters.ReplicaFilter
	replicaFilters        []filters.ReplicaFilter
	replicaSorts          []sorters.ReplicaSorter
	stabilizationWindow   time.Duration
}

func DefaultScalerConfig(stabilizationWindowSeconds uint64) ScalerConfig {
	return ScalerConfig{
		scaleUpReplicaFilters: []filters.ReplicaFilter{filters.ExplainerFilter{}},
		replicaFilters:        []filters.ReplicaFilter{filters.AvailableMemoryReplicaFilter{Affinity: false}, filters.ExplainerFilter{}, filters.ReplicaDrainingFilter{}},
		replicaSorts:          []sorters.ReplicaSorter{sorters.ReplicaIndexSorter{}, sorters.AvailableMemorySorter{}, sorters.ModelAlreadyLoadedSorter{}},
		stabilizationWindow:   time.Duration(stabilizationWindowSeconds) * time.Second,
	}
}

type SimulateRecord struct {
	server         *store.ServerSnapshot
	reservedMemory map[int]uint64
}

func (r *SimulateRecord) Add(replica *store.ServerReplica, memory uint64) {
	replica.UpdateReservedMemory(memory, true)
	replicaIdx := replica.GetReplicaIdx()
	r.reservedMemory[replicaIdx] += memory
}

func (r *SimulateRecord) ReleaseReservedMemory() {
	for replicaIdx, memory := range r.reservedMemory {
		replica := r.server.Replicas[replicaIdx]
		replica.UpdateReservedMemory(memory, false)
	}
}

type memoryServerScaler struct {
	store        store.ModelStore
	scalerConfig ScalerConfig
	logger       log.FieldLogger
}

func NewMemoryServerScaler(store store.ModelStore, scalerConfig ScalerConfig, logger log.FieldLogger) *memoryServerScaler {
	return &memoryServerScaler{
		store:        store,
		scalerConfig: scalerConfig,
		logger:       logger,
	}
}

func (scaler *memoryServerScaler) Scalable(serverKey string, replicas int, model *store.ModelVersion) bool {
	logger := scaler.logger.WithField("func", "Scalable")
	server, err := scaler.store.GetServer(serverKey, false, true)
	if err != nil {
		logger.WithError(err).Errorf("failed to get server %s", serverKey)
		return false
	}

	autoscalingEnabled := server.MinReplicas > 0 || server.MaxReplicas > 0
	if !autoscalingEnabled {
		logger.Debugf("auto scaling for server %s is disabled", serverKey)
		return false
	}

	// check whether the server is during scaling
	if len(server.Replicas) != server.ExpectedReplicas {
		logger.Debugf("server %s is not scalable as its state is not stable", server.Name)
		return false
	}

	if !isValidReplicas(server, replicas) {
		logger.Debugf("invalid server replicas %d for %s", replicas, serverKey)
		return false
	}

	if replicas > server.ExpectedReplicas {
		// check capability to scale up
		err := scaler.checkCapability(server, model)
		if err != nil {
			logger.Debugf("cann't scale up server %s for model %s because of capability mismatch", server.Name, model.GetMeta().Name)
			return false
		}
		return true
	} else {
		replica := server.Replicas[server.ExpectedReplicas-1]
		if time.Since(replica.GetCreateTime()) < scaler.scalerConfig.stabilizationWindow {
			logger.Debugf("cannt scale down server %s since its latest replica was created in the last %d seconds", server.Name, int(scaler.scalerConfig.stabilizationWindow.Seconds()))
			return false
		}

		// check avaliable memory to scale down
		err := scaler.checkAvaliableMemory(server, replicas)
		if err != nil {
			logger.WithError(err).Debugf("there is no enough memory to scale down server %s to %d replicas", server.Name, replicas)
			return false
		}
		return true
	}
}

func isValidReplicas(server *store.ServerSnapshot, replicas int) bool {
	if replicas == server.ExpectedReplicas {
		return false
	} else if replicas > server.ExpectedReplicas {
		return replicas <= server.MaxReplicas || server.MaxReplicas <= 0
	} else {
		return replicas >= server.MinReplicas && replicas > 0
	}
}

func filterReplicas(filters []filters.ReplicaFilter, model *store.ModelVersion, server *store.ServerSnapshot, logger log.FieldLogger) *sorters.CandidateServer {
	logger = logger.
		WithField("func", "filterReplicas").
		WithField("model", model.GetMeta().GetName()).
		WithField("server", server.Name)
	logger.Debug("Filtering server replicas for model")

	candidateServer := sorters.CandidateServer{Model: model, Server: server}

	for _, replica := range server.Replicas {
		ok := true
		for _, replicaFilter := range filters {
			if !replicaFilter.Filter(model, replica) {

				logger.
					WithField("filter", replicaFilter.Name()).
					WithField("replica", replica.GetReplicaIdx()).
					WithField("reason", replicaFilter.Description(model, replica)).
					Debug("Rejecting server replica for model")

				ok = false
				break
			}
		}
		if ok {
			logger.WithField("replica", replica.GetReplicaIdx()).Debug("Accepting server replica for model")
			candidateServer.ChosenReplicas = append(candidateServer.ChosenReplicas, replica)
		}
	}
	return &candidateServer
}

// simulate the process of server replicas scaling down to determine whether there is enough memory
func (scaler *memoryServerScaler) checkAvaliableMemory(server *store.ServerSnapshot, replicas int) error {
	simulateRecord := SimulateRecord{server: server, reservedMemory: map[int]uint64{}}
	defer simulateRecord.ReleaseReservedMemory()

	scaleDownReplicaIdx := server.ExpectedReplicas - 1
	for scaleDownReplicaIdx > replicas-1 {
		err := scaler.simulateDrainReplica(server, replicas, scaleDownReplicaIdx, &simulateRecord)
		if err != nil {
			return err
		}
		scaleDownReplicaIdx--
	}
	return nil
}

func (scaler *memoryServerScaler) simulateDrainReplica(
	server *store.ServerSnapshot,
	scaleToReplicas int,
	drainReplicaIdx int,
	simulateRecord *SimulateRecord,
) error {
	scaleDownReplica := server.Replicas[drainReplicaIdx]
	for _, modelVersionId := range scaleDownReplica.GetLoadedOrLoadingModelVersions() {
		err := scaler.simulateDrainModel(server, scaleToReplicas, modelVersionId, simulateRecord)
		if err != nil {
			return err
		}
	}
	return nil
}

func (scaler *memoryServerScaler) simulateDrainModel(
	server *store.ServerSnapshot,
	scaleToReplicas int,
	drainModelVersionId store.ModelVersionID,
	simulateRecord *SimulateRecord,
) error {
	model, err := scaler.store.GetModel(drainModelVersionId.Name)
	if err != nil {
		return fmt.Errorf("failed to get mdoel %s", drainModelVersionId.Name)
	}

	replicaFilters := []filters.ReplicaFilter{
		filters.MaxIndexReplicaFilter{MaxIndex: uint(scaleToReplicas - 1)},
	}
	replicaFilters = append(replicaFilters, scaler.scalerConfig.replicaFilters...)
	modelVersion := model.GetLatest()

	candidateServer := filterReplicas(replicaFilters, modelVersion, server, scaler.logger)
	if len(candidateServer.ChosenReplicas) == 0 {
		return fmt.Errorf(
			"no replicas can accept model %s after scaling down server %s to %d replicas.",
			model.Name, server.Name, scaleToReplicas)
	}

	candidateServer.SortReplicas(scaler.scalerConfig.replicaSorts)

	chosenReplica := candidateServer.ChosenReplicas[0]
	simulateRecord.Add(chosenReplica, modelVersion.GetRequiredMemory())

	return nil
}

// check the capbilities of server
func (scaler *memoryServerScaler) checkCapability(server *store.ServerSnapshot, model *store.ModelVersion) error {
	candidateServer := filterReplicas(scaler.scalerConfig.scaleUpReplicaFilters, model, server, scaler.logger)
	if len(candidateServer.ChosenReplicas) == 0 {
		return fmt.Errorf("no candidate replicas")
	}
	return nil
}
