package metrics

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"time"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

var (
	// Pre-compiled regex patterns for PromQL value escaping.
	backslashPattern = regexp.MustCompile(`\\`)
	quotePattern     = regexp.MustCompile(`"`)
)

// PrometheusSourceConfig contains configuration for the Prometheus source.
type PrometheusSourceConfig struct {
	// DefaultTTL is the default cache TTL for query results.
	DefaultTTL time.Duration
	// QueryTimeout is the timeout for individual Prometheus queries.
	QueryTimeout time.Duration
}

// DefaultPrometheusSourceConfig returns sensible defaults.
func DefaultPrometheusSourceConfig() PrometheusSourceConfig {
	return PrometheusSourceConfig{
		DefaultTTL:   30 * time.Second,
		QueryTimeout: 10 * time.Second,
	}
}

// PrometheusSource implements MetricsSource for Prometheus backend.
type PrometheusSource struct {
	api    promv1.API
	config PrometheusSourceConfig
	logger log.FieldLogger
}

// NewPrometheusSource creates a new Prometheus metrics source with a default query registry.
func NewPrometheusSource(api promv1.API, config PrometheusSourceConfig, logger log.FieldLogger) *PrometheusSource {
	return &PrometheusSource{
		api:    api,
		config: config,
		logger: logger.WithField("name", "PrometheusSource"),
	}
}

func EscapePromQLValue(value string) string {
	// Escape backslashes first (must be done before escaping quotes)
	value = backslashPattern.ReplaceAllString(value, `\\`)
	// Escape double quotes
	value = quotePattern.ReplaceAllString(value, `\"`)
	return value
}

func (p *PrometheusSource) Query(ctx context.Context, query string) *MetricResult {
	// Apply query timeout
	queryCtx := ctx
	if p.config.QueryTimeout > 0 {
		var cancel context.CancelFunc
		queryCtx, cancel = context.WithTimeout(ctx, p.config.QueryTimeout)
		defer cancel()
	}

	// Execute query with backoff
	val, warnings, err := QueryPrometheusWithBackoff(queryCtx, p.api, query)
	if err != nil {
		return &MetricResult{
			Query:       query,
			CollectedAt: time.Now(),
			Error:       fmt.Errorf("query execution failed: %w, query:%s", err, query),
		}
	}

	if len(warnings) > 0 {
		p.logger.Info("Prometheus query warnings",
			"query", query,
			"warnings", warnings)
	}

	// Parse the result
	values := p.parseResult(val)

	return &MetricResult{
		Query:       query,
		Values:      values,
		CollectedAt: time.Now(),
	}
}

// parseResult converts Prometheus query result to MetricValues.
func (p *PrometheusSource) parseResult(val model.Value) []MetricValue {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case model.Vector:
		return p.parseVector(v)
	case *model.Scalar:
		return p.parseScalar(v)
	case model.Matrix:
		return p.parseMatrix(v)
	default:
		return nil
	}
}

// parseVector parses a Prometheus vector result.
func (p *PrometheusSource) parseVector(vec model.Vector) []MetricValue {
	values := make([]MetricValue, 0, len(vec))
	for _, sample := range vec {
		value := float64(sample.Value)
		fixNaN(&value)

		labels := make(map[string]string)
		for k, v := range sample.Metric {
			labels[string(k)] = string(v)
		}

		values = append(values, MetricValue{
			Value:     value,
			Timestamp: sample.Timestamp.Time(),
			Labels:    labels,
		})
	}
	return values
}

// parseScalar parses a Prometheus scalar result.
func (p *PrometheusSource) parseScalar(scalar *model.Scalar) []MetricValue {
	if scalar == nil {
		return nil
	}

	value := float64(scalar.Value)
	fixNaN(&value)

	return []MetricValue{{
		Value:     value,
		Timestamp: scalar.Timestamp.Time(),
	}}
}

// parseMatrix parses a Prometheus matrix result (range query).
// Returns the latest value from each time series.
func (p *PrometheusSource) parseMatrix(matrix model.Matrix) []MetricValue {
	values := make([]MetricValue, 0, len(matrix))
	for _, stream := range matrix {
		if len(stream.Values) == 0 {
			continue
		}

		// Get the latest sample
		latest := stream.Values[len(stream.Values)-1]
		value := float64(latest.Value)
		fixNaN(&value)

		labels := make(map[string]string)
		for k, v := range stream.Metric {
			labels[string(k)] = string(v)
		}

		values = append(values, MetricValue{
			Value:     value,
			Timestamp: latest.Timestamp.Time(),
			Labels:    labels,
		})
	}
	return values
}

// --- Helpers ---

// fixNaN replaces NaN and Inf values with 0.
func fixNaN(v *float64) {
	if math.IsNaN(*v) || math.IsInf(*v, 0) {
		*v = 0
	}
}

// countSuccessful counts results without errors.
func countSuccessful(results map[string]*MetricResult) int {
	count := 0
	for _, r := range results {
		if r != nil && r.Error == nil {
			count++
		}
	}
	return count
}
