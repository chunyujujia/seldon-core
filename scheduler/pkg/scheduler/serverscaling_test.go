/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package scheduler

import (
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"

	pba "github.com/seldonio/seldon-core/apis/go/v2/mlops/agent"
	pb "github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

// scalerMockStore implements store.ModelStore for scaler tests.
// Unlike the mockStore in scheduler_test.go, this also supports GetServer.
type scalerMockStore struct {
	models  map[string]*store.ModelSnapshot
	servers map[string]*store.ServerSnapshot
}

var _ store.ModelStore = (*scalerMockStore)(nil)

func (f *scalerMockStore) GetModel(key string) (*store.ModelSnapshot, error) {
	ms, ok := f.models[key]
	if !ok {
		return nil, fmt.Errorf("model %s not found", key)
	}
	return ms, nil
}

func (f *scalerMockStore) GetServer(serverKey string, shallow bool, modelDetails bool) (*store.ServerSnapshot, error) {
	ss, ok := f.servers[serverKey]
	if !ok {
		return nil, fmt.Errorf("server %s not found", serverKey)
	}
	return ss, nil
}

// Stub methods to satisfy the ModelStore interface
func (f *scalerMockStore) UpdateModel(config *pb.LoadModelRequest) error         { return nil }
func (f *scalerMockStore) GetModels() ([]*store.ModelSnapshot, error)            { return nil, nil }
func (f *scalerMockStore) LockModel(modelId string)                             {}
func (f *scalerMockStore) UnlockModel(modelId string)                           {}
func (f *scalerMockStore) RemoveModel(req *pb.UnloadModelRequest) error          { return nil }
func (f *scalerMockStore) GetServers(shallow bool, modelDetails bool) ([]*store.ServerSnapshot, error) {
	return nil, nil
}
func (f *scalerMockStore) UpdateLoadedModels(modelKey string, version uint32, serverKey string, replicas []*store.ServerReplica) error {
	return nil
}
func (f *scalerMockStore) UpdateGpuUsage(serverKey string, replicaIdx int, gpuUsages float64) error {
	return nil
}
func (f *scalerMockStore) UnloadVersionModels(modelKey string, version uint32) (bool, error) {
	return true, nil
}
func (f *scalerMockStore) UpdateModelState(modelKey string, version uint32, serverKey string, replicaIdx int, availableMemory *uint64, expectedState, desiredState store.ModelReplicaState, reason string) error {
	return nil
}
func (f *scalerMockStore) AddServerReplica(request *pba.AgentSubscribeRequest) error { return nil }
func (f *scalerMockStore) ServerNotify(request *pb.ServerNotifyRequest) error        { return nil }
func (f *scalerMockStore) RemoveServerReplica(serverName string, replicaIdx int) ([]string, error) {
	return nil, nil
}
func (f *scalerMockStore) DrainServerReplica(serverName string, replicaIdx int) ([]string, error) {
	return nil, nil
}
func (f *scalerMockStore) UpdateServerScaleToReplicas(serverName string, scaleToReplicas int32) {}
func (f *scalerMockStore) FailedScheduling(modelVersion *store.ModelVersion, reason string, reset bool) {
}
func (f *scalerMockStore) GetAllModels() []string { return nil }

func strPtr(s string) *string   { return &s }
func uint64Ptr(v uint64) *uint64 { return &v }

func TestDefaultScalerConfigIncludesSpreadGroupSorter(t *testing.T) {
	g := NewGomegaWithT(t)

	// Use nil store — safe because we only verify sorter composition via Name(),
	// never invoke IsLess() which would dereference the store.
	// For tests that exercise actual sorting, use a scalerMockStore (see below).
	config := DefaultScalerConfig(nil, 0, 100)

	expectedSorterNames := []string{
		"ReplicaIndexSorter",
		"AvailableResourceSorter",
		"SpreadGroupSorter",
		"ModelAlreadyLoadedSorter",
	}

	g.Expect(config.replicaSorts).To(HaveLen(len(expectedSorterNames)))

	for i, expectedName := range expectedSorterNames {
		g.Expect(config.replicaSorts[i].Name()).To(Equal(expectedName),
			"sorter at index %d should be %s", i, expectedName)
	}
}

func TestDefaultScalerAndSchedulerHaveSameReplicaSorterNames(t *testing.T) {
	g := NewGomegaWithT(t)

	// Verify scaler and scheduler have the same replica sorter names and order.
	// Note: nil store is safe here — we only call Name(), never IsLess().
	scalerConfig := DefaultScalerConfig(nil, 0, 100)
	schedulerConfig := DefaultSchedulerConfig(nil, 100)

	g.Expect(scalerConfig.replicaSorts).To(HaveLen(len(schedulerConfig.replicaSorts)),
		"scaler and scheduler should have the same number of replica sorters")

	for i := range scalerConfig.replicaSorts {
		g.Expect(scalerConfig.replicaSorts[i].Name()).To(Equal(schedulerConfig.replicaSorts[i].Name()),
			"sorter at index %d should match between scaler and scheduler", i)
	}
}

// TestSimulateDrainDistributesSpreadGroupModels verifies that when multiple models
// with the same spreadGroup are drained from a replica, the simulation distributes
// them across different target replicas instead of clustering them together.
//
// This tests the fix for stale replica state during multi-model drain: after simulating
// a model placement on a replica, the model is tracked in that replica's loadedModels
// so that subsequent SpreadGroupSorter calls see accurate state.
func TestSimulateDrainDistributesSpreadGroupModels(t *testing.T) {
	g := NewGomegaWithT(t)
	logger := log.New()

	server := store.NewServer("server1", true)

	// Models with the same spreadGroup "group-a"
	modelM1 := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelM1"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", MemoryBytes: uint64Ptr(100), SpreadGroup: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	modelM2 := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelM2"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", MemoryBytes: uint64Ptr(100), SpreadGroup: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	// modelX is already loaded on replica 0, also has tag "group-a"
	modelX := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelX"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", MemoryBytes: uint64Ptr(100), SpreadGroup: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	mvIDM1 := store.ModelVersionID{Name: "modelM1", Version: 1}
	mvIDM2 := store.ModelVersionID{Name: "modelM2", Version: 1}
	mvIDX := store.ModelVersionID{Name: "modelX", Version: 1}

	fakeStore := &scalerMockStore{
		models: map[string]*store.ModelSnapshot{
			"modelM1": {Name: "modelM1", Versions: []*store.ModelVersion{modelM1}},
			"modelM2": {Name: "modelM2", Versions: []*store.ModelVersion{modelM2}},
			"modelX":  {Name: "modelX", Versions: []*store.ModelVersion{modelX}},
		},
	}

	// Replica 0: has modelX (tag "group-a") — 1 existing spread-grouped model
	// Replica 1: empty — 0 spread-grouped models
	// Replica 2: has modelM1 and modelM2 (both tag "group-a") — being drained
	//
	// Scale down from 3 to 2 replicas (drain replica 2).
	// Expected behavior with anti-affinity:
	//   - First drain modelM1: replica 1 (0 matches) preferred over replica 0 (1 match) → M1 goes to replica 1
	//   - Second drain modelM2: replica 1 NOW has 1 match (from simulated M1), replica 0 has 1 match → tie,
	//     broken by ReplicaIndexSorter (replica 0 preferred) → M2 goes to replica 0
	// This distributes the spread-grouped models instead of clustering them on replica 1.

	serverSnapshot := &store.ServerSnapshot{
		Name:             "server1",
		ExpectedReplicas: 3,
		Replicas: map[int]*store.ServerReplica{
			0: store.NewServerReplica("", 8080, 5001, 0, server, []string{}, 10000, 10000, 0,
				map[store.ModelVersionID]bool{mvIDX: true}, 100),
			1: store.NewServerReplica("", 8080, 5001, 1, server, []string{}, 10000, 10000, 0,
				map[store.ModelVersionID]bool{}, 100),
			2: store.NewServerReplica("", 8080, 5001, 2, server, []string{}, 10000, 10000, 0,
				map[store.ModelVersionID]bool{mvIDM1: true, mvIDM2: true}, 100),
		},
	}

	scalerConfig := DefaultScalerConfig(fakeStore, 0, 100)
	scaler := &memoryServerScaler{
		store:        fakeStore,
		scalerConfig: scalerConfig,
		logger:       logger,
	}

	simulateRecord := SimulateRecord{
		server:          serverSnapshot,
		reservedMemory:  map[int]uint64{},
		simulatedModels: map[int][]store.ModelVersionID{},
	}

	// Drain replica 2 (scale from 3 down to 2)
	err := scaler.simulateDrainReplica(serverSnapshot, 2, 2, &simulateRecord)
	g.Expect(err).ToNot(HaveOccurred())

	// Verify the simulated model placements were distributed across replicas.
	// Both replicas 0 and 1 should have received one model each (anti-affinity distribution).
	replica0SimulatedCount := len(simulateRecord.simulatedModels[0])
	replica1SimulatedCount := len(simulateRecord.simulatedModels[1])

	g.Expect(replica0SimulatedCount+replica1SimulatedCount).To(Equal(2),
		"both drained models should be placed on replicas 0 and 1")

	// With anti-affinity working correctly, models should be distributed 1:1
	// instead of clustered 0:2 (which would happen with stale state)
	g.Expect(replica0SimulatedCount).To(Equal(1),
		"replica 0 should receive exactly 1 drained model (anti-affinity distribution)")
	g.Expect(replica1SimulatedCount).To(Equal(1),
		"replica 1 should receive exactly 1 drained model (anti-affinity distribution)")

	// Clean up
	simulateRecord.ReleaseReservedMemory()
}
