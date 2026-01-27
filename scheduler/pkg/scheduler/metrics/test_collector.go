package metrics

import "context"

type mockCollector struct{}

// RefreshReplicaMetrics implements Collector.
func (c *mockCollector) RefreshReplicaMetrics(ctx context.Context, namespace string, server string) error {
	panic("unimplemented")
}

func NewMockCollector() Collector {
	return &mockCollector{}
}
