package metrics

import (
	"time"
)

// CacheKey uniquely identifies a cached query result.
// It combines the query name with the parameter values used.
type CacheKey string

// CachedValue wraps a MetricResult with cache metadata.
type CachedValue[T any] struct {
	Result   T
	CachedAt time.Time
	TTL      time.Duration
}

// IsExpired returns true if the cached value has exceeded its TTL.
func (c *CachedValue[T]) IsExpired() bool {
	if c == nil {
		return true
	}
	return time.Since(c.CachedAt) > c.TTL
}

// Age returns how long ago the value was cached.
func (c *CachedValue[T]) Age() time.Duration {
	if c == nil {
		return 0
	}
	return time.Since(c.CachedAt)
}

// IsFresh returns true if the cached value is within the freshness threshold.
func (c *CachedValue[T]) IsFresh(threshold time.Duration) bool {
	if c == nil {
		return false
	}
	return time.Since(c.CachedAt) <= threshold
}
