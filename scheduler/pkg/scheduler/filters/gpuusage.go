package filters

import (
	"fmt"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

type GpuUsageFilter struct {
	gpuUsageThreshold float64
	affinity          bool
}

func NewGpuUsageFilter(gpuUsageThreshold float64, affinity bool) GpuUsageFilter {
	return GpuUsageFilter{
		gpuUsageThreshold: gpuUsageThreshold,
		affinity:          affinity,
	}
}
func (f GpuUsageFilter) Name() string {
	return "GpuUsageFilter"
}

func (f GpuUsageFilter) Filter(model *store.ModelVersion, replica *store.ServerReplica) bool {
	filtered := replica.GetGpuUsage()+replica.GetReservedGpuUsage() < f.gpuUsageThreshold
	loaded := isModelReplicaLoadedOnServerReplica(model, replica)
	if f.affinity {
		filtered = filtered || loaded
	} else {
		filtered = filtered && !loaded
	}
	return filtered
}

func (f GpuUsageFilter) Description(model *store.ModelVersion, replica *store.ServerReplica) string {
	loaded := isModelReplicaLoadedOnServerReplica(model, replica)
	return fmt.Sprintf("gpu usage %f, reserved gpu usage: %f, loaded: %t, affinity: %t", replica.GetGpuUsage(), replica.GetReservedGpuUsage(), loaded, f.affinity)
}
