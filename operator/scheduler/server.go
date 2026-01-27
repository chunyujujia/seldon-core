/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package scheduler

import (
	"context"
	"fmt"
	"io"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"

	"github.com/seldonio/seldon-core/operator/v2/apis/mlops/v1alpha1"
	"github.com/seldonio/seldon-core/operator/v2/controllers/serverscaling"
)

func (s *SchedulerClient) ServerNotify(ctx context.Context, server *v1alpha1.Server) error {
	logger := s.logger.WithName("NotifyServer")
	conn, err := s.getConnection(server.Namespace)
	if err != nil {
		return err
	}
	grcpClient := scheduler.NewSchedulerClient(conn)

	var replicas int32
	if !server.ObjectMeta.DeletionTimestamp.IsZero() {
		replicas = 0
	} else if server.Spec.Replicas != nil {
		replicas = *server.Spec.Replicas
	} else {
		return fmt.Errorf("the number fo server replicas should be non-nil here")
	}

	var minReplicas, maxReplicas int32
	if server.Spec.MinReplicas != nil {
		minReplicas = *server.Spec.MinReplicas
	}
	if server.Spec.MaxReplicas != nil {
		maxReplicas = *server.Spec.MaxReplicas
	}

	request := &scheduler.ServerNotifyRequest{
		Name:             server.GetName(),
		ExpectedReplicas: replicas,
		MinReplicas:      minReplicas,
		MaxReplicas:      maxReplicas,
		KubernetesMeta: &scheduler.KubernetesMeta{
			Namespace:  server.GetNamespace(),
			Generation: server.GetGeneration(),
		},
	}
	logger.Info("Notify server", "name", server.GetName(), "namespace", server.GetNamespace(), "replicas", replicas)
	_, err = grcpClient.ServerNotify(
		ctx,
		request,
		grpc_retry.WithMax(SchedulerConnectMaxRetries),
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(SchedulerConnectBackoffScalar)),
	)
	if err != nil {
		logger.Error(err, "Failed to send notify server to scheduler", "name", server.GetName(), "namespace", server.GetNamespace())
		return err
	}
	return nil
}

// note: namespace is not used in this function
func (s *SchedulerClient) SubscribeServerEvents(ctx context.Context, conn *grpc.ClientConn, namespace string) error {
	logger := s.logger.WithName("SubscribeServerEvents")

	grcpClient := scheduler.NewSchedulerClient(conn)

	stream, err := grcpClient.SubscribeServerStatus(
		ctx,
		&scheduler.ServerSubscriptionRequest{SubscriberName: "seldon manager"},
		grpc_retry.WithMax(1),
	)
	if err != nil {
		return err
	}

	// notify the scheduler as the scheduler might have lost the state
	go s.handleServers(ctx, namespace)

	for {
		event, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				logger.Info("the stream received EOF")
				break
			}
			logger.Error(err, "event recv failed")
			return err
		}

		logger.Info("Received event", "server", event.ServerName)
		if event.GetKubernetesMeta() == nil {
			logger.Info("Received server event with no k8s metadata so ignoring", "server", event.ServerName)
			continue
		}
		server := &v1alpha1.Server{}
		err = s.Get(ctx, client.ObjectKey{Name: event.ServerName, Namespace: event.GetKubernetesMeta().GetNamespace()}, server)
		if err != nil {
			logger.Error(err, "Failed to get server", "name", event.ServerName, "namespace", event.GetKubernetesMeta().GetNamespace())
			continue
		}

		// Try to update status
		retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			server := &v1alpha1.Server{}
			err = s.Get(ctx, client.ObjectKey{Name: event.ServerName, Namespace: event.GetKubernetesMeta().GetNamespace()}, server)
			if err != nil {
				return err
			}
			if event.GetKubernetesMeta().Generation != server.Generation {
				logger.Info("Ignoring event for old generation", "currentGeneration", server.Generation, "eventGeneration", event.GetKubernetesMeta().Generation, "server", event.ServerName)
				return nil
			}
			// Handle status update
			server.Status.LoadedModelReplicas = event.NumLoadedModelReplicas
			return s.updateServerStatus(server)
		})
		if retryErr != nil {
			logger.Error(err, "Failed to update status", "model", event.ServerName)
		}

		// Try to update server replicas
		if event.ScaleToReplicas > 0 && server.Status.Replicas != event.ScaleToReplicas {
			if err := checkDesiredNumReplicas(server, event.ScaleToReplicas); err != nil {
				logger.Error(err, "invalid number of server replicas")
				continue
			}
			s.scaleServerReplicas(server, event.ScaleToReplicas)
		}

	}
	return nil
}

func (s *SchedulerClient) updateServerStatus(server *v1alpha1.Server) error {
	if err := s.Status().Update(context.TODO(), server); err != nil {
		s.recorder.Eventf(server, v1.EventTypeWarning, "UpdateFailed",
			"Failed to update status for Server %q: %v", server.Name, err)
		return err
	}
	return nil
}

func checkDesiredNumReplicas(server *v1alpha1.Server, scaleToReplicas int32) error {
	if !autoscalingEnabled(server) {
		return fmt.Errorf("no autoscaling for server %s", server.Name)
	}
	minReplicas := int32(0)
	if server.Spec.ScalingSpec.MinReplicas != nil {
		minReplicas = *server.Spec.ScalingSpec.MinReplicas
	}

	maxReplicas := int32(0)
	if server.Spec.ScalingSpec.MaxReplicas != nil {
		maxReplicas = *server.Spec.ScalingSpec.MaxReplicas
	}
	if scaleToReplicas < minReplicas || scaleToReplicas < 1 {
		return fmt.Errorf("violating min replicas %d / %d for server %s", minReplicas, scaleToReplicas, server.Name)
	}

	if scaleToReplicas > maxReplicas && (maxReplicas > 0) {
		return fmt.Errorf("violating max replicas %d / %d for server %s", maxReplicas, scaleToReplicas, server.Name)
	}

	return nil
}

func autoscalingEnabled(server *v1alpha1.Server) bool {
	return server.Spec.ScalingSpec.MinReplicas != nil || server.Spec.ScalingSpec.MaxReplicas != nil
}

func (sc *SchedulerClient) scaleServerReplicas(server *v1alpha1.Server, replicas int32) {
	sc.serverScaleEvents <- serverscaling.ServerScaleEventMsg{
		Namespace:  server.Namespace,
		ServerName: server.Name,
		Replicas:   replicas,
	}
}

func (s *SchedulerClient) handleServers(ctx context.Context, namespace string) {
	serverList := &v1alpha1.ServerList{}

	err := s.List(
		ctx,
		serverList,
		client.InNamespace(namespace),
	)
	if err != nil {
		s.logger.Error(err, "failed to list servers in k8s")
		return
	}

	for _, server := range serverList.Items {
		if server.ObjectMeta.DeletionTimestamp.IsZero() {
			s.logger.Info("Notify the server info to the scheduler (on reconnect)", "server", server.Name)
			if err := s.ServerNotify(ctx, &server); err != nil {
				s.logger.Error(err, "failed to notify scheduler about the server", "server", server.Name)
			} else {
				s.logger.Info("server notified successfully", "server", server.Name)
			}
		} else {
			s.logger.Info("Server is being deleted, not loading", "server", server.Name)
		}
	}
}
