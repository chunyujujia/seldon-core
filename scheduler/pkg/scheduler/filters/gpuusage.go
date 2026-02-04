package filters

import (
	"fmt"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

type GpuUsageFilter struct {
	gpuUsageThreshold float64
}

func NewGpuUsageFilter(gpuUsageThreshold float64) GpuUsageFilter {
	return GpuUsageFilter{
		gpuUsageThreshold: gpuUsageThreshold,
	}
}
func (f GpuUsageFilter) Name() string {
	return "GpuUsageFilter"
}

func (f GpuUsageFilter) Filter(model *store.ModelVersion, replica *store.ServerReplica) bool {
	return replica.GetGpuUsage()+replica.GetReservedGpuUsage() < f.gpuUsageThreshold
}

func (f GpuUsageFilter) Description(model *store.ModelVersion, replica *store.ServerReplica) string {
	return fmt.Sprintf("model memory %d replica memory %d", model.GetRequiredMemory(), replica.GetAvailableMemory())
}
