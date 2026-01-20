package metrics

import "context"

type mockCollector struct{}

func NewMockCollector() Collector {
	return &mockCollector{}
}

func (c *mockCollector) CollectReplicaMetrics(
	ctx context.Context,
	namespace string,
	server string,
) ([]ReplicaMetrics, error) {
	return []ReplicaMetrics{}, nil
}
