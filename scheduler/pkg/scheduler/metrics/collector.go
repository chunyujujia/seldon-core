package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/util"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultMetricStalenessThreshold is the default threshold for considering metrics stale.
	// Metrics older than this threshold are filtered out to avoid using data from terminated pods.
	// This replaces the kube-state-metrics approach with timestamp-based filtering.
	DefaultMetricStalenessThreshold = 2 * time.Minute

	// DefaultTTL is the default cache TTL for query results.
	DefaultTTL             time.Duration = 30 * time.Second
	DefaultCleanupInterval               = 1 * time.Second

	QueryGpuUtil = "DCGM_FI_DEV_GPU_UTIL"

	ServerLabelNameKey = "seldon-server-name"
)

// ReplicaMetricsCollector collects replica-level metrics for saturation analysis
// using the v2 collector infrastructure.
type ReplicaMetricsCollector struct {
	source             *PrometheusSource
	k8sClient          kubernetes.Interface
	stalenessThreshold time.Duration
	cache              *Cache[[]ReplicaMetrics]
	logger             log.FieldLogger
}

// NewReplicaMetricsCollector creates a new replica metrics collector with a custom staleness threshold.
func NewReplicaMetricsCollector(ctx context.Context, source *PrometheusSource, k8sClient kubernetes.Interface, logger log.FieldLogger) *ReplicaMetricsCollector {
	return &ReplicaMetricsCollector{
		source:             source,
		k8sClient:          k8sClient,
		stalenessThreshold: DefaultMetricStalenessThreshold,
		cache:              newCache[[]ReplicaMetrics](ctx, DefaultTTL, DefaultCleanupInterval),
		logger:             logger,
	}
}

func (c *ReplicaMetricsCollector) CollectReplicaMetrics(
	ctx context.Context,
	namespace string,
	server string,
) ([]ReplicaMetrics, error) {
	cacheKey := CacheKey(fmt.Sprintf("%s:%s", namespace, server))
	cachedValue, ok := c.cache.get(cacheKey)
	if ok && !cachedValue.IsExpired() {
		return cachedValue.Result, nil
	}

	hostGpuUsages, err := c.queryGpuUsages(ctx)
	if err != nil {
		return nil, err
	}
	replicaHosts, err := c.queryReplicaHosts(ctx, namespace, server)
	if err != nil {
		return nil, err
	}

	replicaMetrics := make([]ReplicaMetrics, 0, len(hostGpuUsages))
	for replicaIdx, hostName := range replicaHosts {
		data, ok := hostGpuUsages[hostName]
		gpuUsagePercentage := 100.0
		if !ok {
			gpuUsagePercentage = 100.0
			c.logger.Warnf("no gpu usage found for host %s, server: %s", hostName, server)
		} else {
			gpuUsagePercentage = data.gpuUsagePercentage
		}

		metric := ReplicaMetrics{
			Namespace:          namespace,
			ReplicaIdx:         int(replicaIdx),
			GpuUsagePercentage: gpuUsagePercentage,
		}

		replicaMetrics = append(replicaMetrics, metric)
	}
	c.cache.set(cacheKey, replicaMetrics, 0)
	c.logger.WithField("namespace", namespace).WithField("server", server).
		WithField("replicaCount", len(replicaMetrics)).
		Info("collected replica metrics")

	return replicaMetrics, nil
}

// hostMetricData holds per-pod metric values and timestamps
type hostMetricData struct {
	gpuUsagePercentage float64
	gpuTimestamp       time.Time
}

func (c *ReplicaMetricsCollector) queryGpuUsages(ctx context.Context) (map[string]*hostMetricData, error) {
	query := fmt.Sprintf("avg_over_time(%s[15m])", QueryGpuUtil)

	result := c.source.Query(ctx, query)

	// Extract per-pod metrics from results with timestamps
	hostData := make(map[string]*hostMetricData)
	now := time.Now()

	if result.HasError() {
		return nil, fmt.Errorf("gpu usage query failed: %w", result.Error)
	}
	for _, value := range result.Values {
		hostName := value.Labels["Hostname"]
		if hostName == "" {
			continue
		}

		// Check if metric is stale based on timestamp
		metricAge := now.Sub(value.Timestamp)
		if metricAge > c.stalenessThreshold {
			c.logger.Info("Filtering stale gpu usage metric",
				"host", hostName,
				"metricAge", metricAge.String(),
				"threshold", c.stalenessThreshold.String(),
				"timestamp", value.Timestamp)
			continue
		}

		if hostData[hostName] == nil {
			hostData[hostName] = &hostMetricData{}
		}
		hostData[hostName].gpuUsagePercentage = value.Value
		hostData[hostName].gpuTimestamp = value.Timestamp

		c.logger.WithField("host", hostName).WithField("usage", value.Value/100).
			WithField("usagePercent", value.Value).WithField("metricAge", metricAge.String()).
			Debug("gpu usage metric")
	}
	return hostData, nil
}

// query replica pod info, return a map with replica index as key and host name as value
func (c *ReplicaMetricsCollector) queryReplicaHosts(ctx context.Context, namespace, server string) (map[uint]string, error) {
	pods, err := c.k8sClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", ServerLabelNameKey, server),
	})
	if err != nil {
		return nil, fmt.Errorf("error listing pods for server %s in namespace %s: %v", server, namespace, err)
	}

	podNodeMap := make(map[uint]string)
	for _, pod := range pods.Items {
		replicaIdx, err := util.ParseReplicaIdxFromPodName(pod.Name)
		if err != nil {
			return nil, err
		}
		podNodeMap[replicaIdx] = pod.Spec.NodeName
	}
	return podNodeMap, nil
}
