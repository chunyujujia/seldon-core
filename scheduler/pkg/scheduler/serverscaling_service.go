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
	"context"
	"time"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler/metrics"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
	log "github.com/sirupsen/logrus"
)

type serverScalingService struct {
	store           store.ModelStore
	metricCollector metrics.Collector
	scaler          ServerScaler
	periodSeconds   uint64
	done            chan bool
	logger          log.FieldLogger
}

func NewServerScalingService(store store.ModelStore, metricCollector metrics.Collector, scaler ServerScaler, periodSeconds uint64, logger log.FieldLogger) *serverScalingService {
	return &serverScalingService{
		store:           store,
		metricCollector: metricCollector,
		scaler:          scaler,
		periodSeconds:   periodSeconds,
		done:            make(chan bool),
		logger:          logger,
	}
}

func (ss *serverScalingService) Start() error {
	go func() {
		err := ss.start()
		if err != nil {
			ss.logger.WithError(err).Warnf("Stats analyser failed")
		}
	}()
	return nil
}

func (ss *serverScalingService) Stop() error {
	close(ss.done)
	return nil
}

func (ss *serverScalingService) start() error {
	ticker := time.NewTicker(time.Second * time.Duration(ss.periodSeconds))
	defer ticker.Stop()

	for {
		select {
		case <-ss.done:
			return nil
		case <-ticker.C:
			if err := ss.process(); err != nil {
				return err
			}
		}
	}
}
func (ss *serverScalingService) process() error {
	ss.scaleDownServerIfNeed()
	return nil
}

func (ss *serverScalingService) scaleDownServerIfNeed() error {
	servers, err := ss.store.GetServers(false, true)
	if err != nil {
		return err
	}

	for _, server := range servers {
		err := ss.metricCollector.RefreshReplicaMetrics(
			context.Background(),
			server.KubernetesMeta.GetNamespace(),
			server.Name)
		if err != nil {
			ss.logger.WithField("server", server.Name).WithError(err).Errorf("fail to refresh replica metrics")
			continue
		}

		scaleToReplicas := server.ExpectedReplicas - 1

		if ss.scaler.Scalable(server.Name, scaleToReplicas, nil) {

			ss.logger.Debugf("scale down server %s to %d replicas", server.Name, scaleToReplicas)
			ss.store.UpdateServerScaleToReplicas(server.Name, int32(scaleToReplicas))
		}
	}
	return nil
}
