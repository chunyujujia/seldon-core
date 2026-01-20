package metrics

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strings"

	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	// Lightweight backoff for individual Prometheus queries (collector, etc.)
	PrometheusQueryBackoff = wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   2.0,
		Jitter:   0.1,
		Steps:    5, // 500ms, 1s, 2s, 4s = ~7.5s total
	}

	// Prometheus validation backoff with longer intervals
	// TODO: investigate why Prometheus needs longer backoff durations
	PrometheusValidationBackoff = wait.Backoff{
		Duration: 5 * time.Second,
		Factor:   2.0,
		Jitter:   0.1,
		Steps:    6, // 5s, 10s, 20s, 40s, 80s, 160s = ~5 minutes total
	}
)

type PrometheusConfig struct {
	// BaseURL is the Prometheus server URL (must use https:// scheme)
	BaseURL string `json:"baseURL"`

	// TLS configuration fields (TLS is always enabled for HTTPS-only support)
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty"` // Skip certificate verification (development/testing only)
	CACertPath         string `json:"caCertPath,omitempty"`         // Path to CA certificate for server validation
	ClientCertPath     string `json:"clientCertPath,omitempty"`     // Path to client certificate for mutual TLS authentication
	ClientKeyPath      string `json:"clientKeyPath,omitempty"`      // Path to client private key for mutual TLS authentication
	ServerName         string `json:"serverName,omitempty"`         // Expected server name for SNI (Server Name Indication)

	// Authentication fields (BearerToken takes precedence over TokenPath)
	BearerToken string `json:"bearerToken,omitempty"` // Direct bearer token string (development/testing)
	TokenPath   string `json:"tokenPath,omitempty"`   // Path to file containing bearer token (production with mounted secrets)
}

func GetPrometheusConfigFromEnv() *PrometheusConfig {
	promAddr := os.Getenv("PROMETHEUS_BASE_URL")
	if promAddr == "" {
		return nil
	}
	config := &PrometheusConfig{
		BaseURL: os.Getenv("PROMETHEUS_BASE_URL"),
	}

	// TLS is always enabled for HTTPS-only support
	config.InsecureSkipVerify = os.Getenv("PROMETHEUS_TLS_INSECURE_SKIP_VERIFY") == "true"
	config.CACertPath = os.Getenv("PROMETHEUS_CA_CERT_PATH")
	config.ClientCertPath = os.Getenv("PROMETHEUS_CLIENT_CERT_PATH")
	config.ClientKeyPath = os.Getenv("PROMETHEUS_CLIENT_KEY_PATH")
	config.ServerName = os.Getenv("PROMETHEUS_SERVER_NAME")

	// Support both direct bearer token and token path
	config.BearerToken = os.Getenv("PROMETHEUS_BEARER_TOKEN")
	config.TokenPath = os.Getenv("PROMETHEUS_TOKEN_PATH")

	return config
}

// CreatePrometheusTransport creates a custom HTTPS transport for Prometheus client with TLS support.
// TLS is always enabled for HTTPS-only support with configurable certificate validation.
func CreatePrometheusTransport(config *PrometheusConfig) (http.RoundTripper, error) {
	// Clone the default transport to get all the good defaults
	transport := http.DefaultTransport.(*http.Transport).Clone()

	// Configure TLS (always required for HTTPS-only support)
	tlsConfig, err := CreateTLSConfig(config)
	if err != nil {
		return nil, err
	}
	transport.TLSClientConfig = tlsConfig

	return transport, nil
}

func CreateTLSConfig(promConfig *PrometheusConfig) (*tls.Config, error) {
	if promConfig == nil {
		return nil, nil
	}

	config := &tls.Config{
		InsecureSkipVerify: promConfig.InsecureSkipVerify,
		ServerName:         promConfig.ServerName,
		MinVersion:         tls.VersionTLS12, // Enforce minimum TLS version - https://docs.redhat.com/en/documentation/openshift_container_platform/4.18/html/security_and_compliance/tls-security-profiles#:~:text=requires%20a%20minimum-,TLS%20version%20of%201.2,-.
	}

	// Load CA certificate if provided
	if promConfig.CACertPath != "" {
		caCert, err := os.ReadFile(promConfig.CACertPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate from %s: %w", promConfig.CACertPath, err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", promConfig.CACertPath)
		}
		config.RootCAs = caCertPool
	}

	// Load client certificate and key if provided
	if promConfig.ClientCertPath != "" && promConfig.ClientKeyPath != "" {
		cert, err := tls.LoadX509KeyPair(promConfig.ClientCertPath, promConfig.ClientKeyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate from %s and key from %s: %w",
				promConfig.ClientCertPath, promConfig.ClientKeyPath, err)
		}
		config.Certificates = []tls.Certificate{cert}
	}

	return config, nil
}

// CreatePrometheusClientConfig creates a complete Prometheus client configuration with HTTPS support.
// Supports both direct bearer tokens and token files for flexible authentication.
func CreatePrometheusClientConfig(config *PrometheusConfig) (*api.Config, error) {
	clientConfig := &api.Config{
		Address: config.BaseURL,
	}

	// Create custom HTTPS transport with TLS support
	transport, err := CreatePrometheusTransport(config)
	if err != nil {
		return nil, err
	}

	// Add bearer token authentication if provided
	bearerToken := config.BearerToken

	// If no direct bearer token but token path is provided, read from file
	if bearerToken == "" && config.TokenPath != "" {
		tokenBytes, err := os.ReadFile(config.TokenPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read bearer token from %s: %w", config.TokenPath, err)
		}
		bearerToken = strings.TrimSpace(string(tokenBytes))
	}

	if bearerToken != "" {
		// Create a custom round tripper that adds the bearer token
		transport = &bearerTokenRoundTripper{
			base:  transport,
			token: bearerToken,
		}
	}

	clientConfig.RoundTripper = transport

	return clientConfig, nil
}

type bearerTokenRoundTripper struct {
	base  http.RoundTripper
	token string
}

// RoundTrip adds the Authorization header with bearer token
func (b *bearerTokenRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(req)
}

func QueryPrometheusWithBackoff(ctx context.Context, promAPI promv1.API, query string) (val model.Value, warn promv1.Warnings, err error) {
	var lastErr error

	err = wait.ExponentialBackoffWithContext(ctx, PrometheusQueryBackoff, func() (bool, error) {
		val, warn, err = promAPI.Query(ctx, query, time.Now())
		if err != nil {
			// Record the last error so that we can surface it if the backoff is exhausted.
			lastErr = err
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		if lastErr != nil {
			return nil, nil, lastErr
		}
		return nil, nil, err
	}

	return
}

// ValidatePrometheusAPIWithBackoff validates Prometheus API connectivity with retry logic
func ValidatePrometheusAPIWithBackoff(ctx context.Context, promAPI promv1.API, backoff wait.Backoff) error {
	return wait.ExponentialBackoffWithContext(ctx, backoff, func() (bool, error) {
		// Test with a simple query that should always work
		query := "up"
		_, _, err := promAPI.Query(ctx, query, time.Now())
		if err != nil {
			ctrl.LoggerFrom(ctx).Error(err, "Prometheus API validation failed, retrying - ", "query: ", query)
			return false, nil // Retry on transient errors
		}

		ctrl.LoggerFrom(ctx).Info("Prometheus API validation successful with query", "query", query)
		return true, nil
	})
}

// ValidatePrometheusAPI validates Prometheus API connectivity using standard Prometheus backoff
func ValidatePrometheusAPI(ctx context.Context, promAPI promv1.API) error {
	return ValidatePrometheusAPIWithBackoff(ctx, promAPI, PrometheusValidationBackoff)
}
