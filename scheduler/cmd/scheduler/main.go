/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	envoyServerControlPlaneV3 "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"github.com/prometheus/client_golang/api"
	log "github.com/sirupsen/logrus"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/agent"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/coordinator"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/envoy/processor"
	envoyServer "github.com/seldonio/seldon-core/scheduler/v2/pkg/envoy/server"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/envoy/xdscache"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/kafka/config"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/kafka/dataflow"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler/cleaner"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/scheduler/metrics"
	schedulerServer "github.com/seldonio/seldon-core/scheduler/v2/pkg/server"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store/experiment"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store/pipeline"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/tracing"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/util"
)

var (
	envoyPort                  uint
	agentPort                  uint
	agentMtlsPort              uint
	schedulerPort              uint
	schedulerMtlsPort          uint
	chainerPort                uint
	namespace                  string
	pipelineGatewayHost        string
	pipelineGatewayHttpPort    int
	pipelineGatewayGrpcPort    int
	logLevel                   string
	tracingConfigPath          string
	dbPath                     string
	nodeID                     string
	allowPlaintxt              bool //scheduler server
	autoscalingDisabled        bool
	stabilizationWindowSeconds uint64
	gpuUsageCordonPercentage   float64
	scalingPeriodSeconds       uint64
	kafkaConfigPath            string
	useDeploymentsForServers   bool
)

func init() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// The envoyPort that this xDS server listens on
	flag.UintVar(&envoyPort, "envoy-port", 9002, "xDS management server port")

	// The scheduler port to listen for schedule commands
	flag.UintVar(&schedulerPort, "scheduler-port", 9004, "scheduler server port")

	// The scheduler port to listen for schedule commands using mtls
	flag.UintVar(&schedulerMtlsPort, "scheduler-mtls-port", 9044, "scheduler mtls server port")

	// The agent port to listen for agent subscriptions
	flag.UintVar(&agentPort, "agent-port", 9005, "agent server port")

	// The agent port to listen for schedule commands using mtls
	flag.UintVar(&agentMtlsPort, "agent-mtls-port", 9055, "agent mtls server port")

	// The dataflow port to listen for data flow agents
	flag.UintVar(&chainerPort, "dataflow-port", 9008, "dataflow server port")

	// Tell Envoy to use this Node ID
	flag.StringVar(&nodeID, "nodeID", "test-id", "Node ID")

	// Kubernetes namespace
	flag.StringVar(&namespace, "namespace", "", "Namespace")

	flag.StringVar(&pipelineGatewayHost, "pipeline-gateway-host", "0.0.0.0", "Pipeline gateway server host")
	flag.IntVar(&pipelineGatewayHttpPort, "pipeline-gateway-http-port", 9010, "Pipeline gateway server http port")
	flag.IntVar(&pipelineGatewayGrpcPort, "pipeline-gateway-grpc-port", 9011, "Pipeline gateway server grpc port")
	flag.StringVar(&logLevel, "log-level", "debug", "Log level - examples: debug, info, error")
	flag.StringVar(&tracingConfigPath, "tracing-config-path", "", "Tracing config path")
	flag.StringVar(&dbPath, "db-path", "", "State Db path")

	// Allow plaintext servers
	flag.BoolVar(&allowPlaintxt, "allow-plaintxt", true, "Allow plain text scheduler server")

	// Whether to enable autoscaling, default is true
	flag.BoolVar(&autoscalingDisabled, "disable-autoscaling", false, "Disable autoscaling feature")
	flag.StringVar(
		&kafkaConfigPath,
		"kafka-config-path",
		"/mnt/config/kafka.json",
		"Path to kafka configuration file",
	)
	flag.Uint64Var(&stabilizationWindowSeconds, "stabilization-window-seconds", 1800, "Stabilizaition widionw before scaling down server replica, default 1800")
	flag.Uint64Var(&scalingPeriodSeconds, "scaling-period-seconds", 60, "Scaling period in seconds, default 60")
	flag.Float64Var(&gpuUsageCordonPercentage, "gpu-usage-cordon-percentage", 50, "Don't schedule models to replicas with gpu usage over this threshold")
	flag.BoolVar(&useDeploymentsForServers, "use-deployment-for-servers", false, "Use deployment or statefulset for servers")

}

func getNamespace() string {
	nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		log.Warn("Using namespace from command line argument")
		return namespace
	}
	ns := string(nsBytes)
	log.Info("Namespace is ", ns)
	return ns
}

func makeSignalHandler(logger *log.Logger, done chan<- bool) {
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	<-exit

	logger.Info("shutting down due to SIGTERM or SIGINT")
	close(done)
}

func main() {
	logger := log.New()
	flag.Parse()
	logIntLevel, err := log.ParseLevel(logLevel)
	if err != nil {
		logger.WithError(err).Fatalf("Failed to set log level %s", logLevel)
	}
	logger.Infof("Setting log level to %s", logLevel)
	logger.SetLevel(logIntLevel)

	done := make(chan bool, 1)

	namespace = getNamespace()

	// Create event Hub
	eventHub, err := coordinator.NewEventHub(logger)
	if err != nil {
		log.WithError(err).Fatal("Unable to create event hub")
	}
	defer eventHub.Close()
	go makeSignalHandler(logger, done)

	// Start xDS server
	// Create a cache
	xdsCache := cache.NewSnapshotCache(false, cache.IDHash{}, logger)
	ctx := context.Background()
	srv := envoyServerControlPlaneV3.NewServer(ctx, xdsCache, nil)
	xdsServer := envoyServer.NewXDSServer(srv, logger)
	err = xdsServer.StartXDSServer(envoyPort)
	if err != nil {
		log.WithError(err).Fatalf("Failed to start envoy xDS server")
	}

	tracer, err := tracing.NewTraceProvider("seldon-scheduler", &tracingConfigPath, logger)
	if err != nil {
		logger.WithError(err).Fatalf("Failed to configure otel tracer")
	} else {
		defer tracer.Stop()
	}

	ss := store.NewMemoryStore(logger, store.NewLocalSchedulerStore(), eventHub)
	ps := pipeline.NewPipelineStore(logger, eventHub, ss)
	es := experiment.NewExperimentServer(logger, eventHub, ss, ps)
	cleaner := cleaner.NewVersionCleaner(ss, logger)

	pipelineGatewayDetails := xdscache.PipelineGatewayDetails{
		Host:     pipelineGatewayHost,
		HttpPort: pipelineGatewayHttpPort,
		GrpcPort: pipelineGatewayGrpcPort,
	}

	_, err = processor.NewIncrementalProcessor(xdsCache, nodeID, logger, ss, es, ps, eventHub, &pipelineGatewayDetails, cleaner)
	if err != nil {
		log.WithError(err).Fatalf("Failed to create incremental processor")
	}
	var scaler scheduler.ServerScaler
	if autoscalingDisabled {
		scaler = &scheduler.DisabledServerScaler{}
	} else {
		scaler = scheduler.NewMemoryServerScaler(ss, scheduler.DefaultScalerConfig(stabilizationWindowSeconds, gpuUsageCordonPercentage), logger)
	}

	// Configure Prometheus client using flexible configuration with TLS support
	promConfig := metrics.GetPrometheusConfigFromEnv()

	// ensure we have a valid configuration
	if promConfig == nil {
		log.WithError(err).Fatal("failed to get Prometheus configuration")
	}

	// Create Prometheus client with TLS support
	promClientConfig, err := metrics.CreatePrometheusClientConfig(promConfig)
	if err != nil {
		log.WithError(err).Fatal("failed to create prometheus client config")
	}

	promClient, err := api.NewClient(*promClientConfig)
	if err != nil {
		log.WithError(err).Fatal("failed to create prometheus client")
	}

	promAPI := promv1.NewAPI(promClient)

	// Validate that the API is working by testing a simple query with retry logic
	if err := metrics.ValidatePrometheusAPI(context.Background(), promAPI); err != nil {
		log.WithError(err).Fatal("CRITICAL: Failed to connect to Prometheus - WVA requires Prometheus connectivity for autoscaling decisions")
	}
	log.Info("Prometheus client and API wrapper initialized and validated successfully")

	prometheusSource := metrics.NewPrometheusSource(promAPI, metrics.DefaultPrometheusSourceConfig(), logger)
	k8sClient, err := util.CreateClientset()
	if err != nil {
		log.WithError(err).Fatal("CRITICAL: Failed to create k8s client")
	}
	metricCollector := metrics.NewReplicaMetricsCollector(
		ctx, namespace, ss, prometheusSource, k8sClient, useDeploymentsForServers, logger,
	)

	sched := scheduler.NewSimpleScheduler(
		logger,
		ss,
		metricCollector,
		scheduler.DefaultSchedulerConfig(ss, gpuUsageCordonPercentage),
		scaler,
	)

	logger.Infof("Autoscaling service is set to %t", !autoscalingDisabled)
	as := agent.NewAgentServer(logger, ss, sched, eventHub, !autoscalingDisabled)

	dataFlowLoadBalancer := util.NewRingLoadBalancer(1)
	kafkaConfigMap, err := config.NewKafkaConfig(kafkaConfigPath)
	if err != nil {
		logger.WithError(err).Fatal("Failed to load Kafka config")
	}
	cs, err := dataflow.NewChainerServer(logger, eventHub, ps, namespace, dataFlowLoadBalancer, kafkaConfigMap)
	if err != nil {
		logger.WithError(err).Fatal("Failed to start data engine chainer server")
	}
	go func() {
		err := cs.StartGrpcServer(chainerPort)
		if err != nil {
			log.WithError(err).Fatalf("Chainer server start error")
		}
	}()

	// Load pipelines and experiments from DB
	// Do here after other services created so eventHub events will be handled on pipeline/experiment load
	// If we start earlier events will be sent but not received by services that start listening "late" to eventHub
	if dbPath != "" {
		err := ps.InitialiseOrRestoreDB(dbPath)
		if err != nil {
			log.WithError(err).Fatalf("Failed to initialise pipeline db at %s", dbPath)
		}
		err = es.InitialiseOrRestoreDB(dbPath)
		if err != nil {
			log.WithError(err).Fatalf("Failed to initialise experiment db at %s", dbPath)
		}
	} else {
		log.Warn("Not running with scheduler local DB")
	}

	serverScalingService := scheduler.NewServerScalingService(
		ss, metricCollector, scaler, scalingPeriodSeconds, logger)
	err = serverScalingService.Start()
	if err != nil {
		log.WithError(err).Fatalf("start server scaling service error")
	}
	defer func() { _ = serverScalingService.Stop() }()

	s := schedulerServer.NewSchedulerServer(logger, ss, es, ps, sched, eventHub)
	err = s.StartGrpcServers(allowPlaintxt, schedulerPort, schedulerMtlsPort)
	if err != nil {
		log.WithError(err).Fatalf("Failed to start server gRPC servers")
	}

	err = as.StartGrpcServer(allowPlaintxt, agentPort, agentMtlsPort)
	if err != nil {
		log.WithError(err).Fatalf("Failed to start agent gRPC server")
	}

	// Wait for completion
	<-done

	log.Info("Shutting down services")

	s.StopSendModelEvents()
	s.StopSendServerEvents()
	s.StopSendExperimentEvents()
	s.StopSendPipelineEvents()
	cs.StopSendPipelineEvents()
	as.StopAgentStreams()
	time.Sleep(time.Second * 1)

	log.Info("Shutdown services")
}
