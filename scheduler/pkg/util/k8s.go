package util

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"k8s.io/client-go/kubernetes"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

func CreateClientset() (kubernetes.Interface, error) {
	config, err := controllerruntime.GetConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func ParseReplicaIdxFromPodName(podName string) (uint, error) {
	if podName == "" {
		return 0, errors.New("invalid empty pod name")
	}
	tokens := strings.Split(podName, "-")

	useDeploymentsForServers := len(tokens) == 3

	if useDeploymentsForServers {
		return parseReplicaIdxFromDeploymentPodName(podName)
	} else {
		return parseReplicaIdxFromStatefulSetPodName(podName)
	}
}

func parseReplicaIdxFromStatefulSetPodName(podName string) (uint, error) {
	lastDashIdx := strings.LastIndex(podName, "-")
	if lastDashIdx == -1 {
		return 0, fmt.Errorf("Can't decypher pod name to find last dash and index. %s", podName)
	}
	serverIdxStr := podName[lastDashIdx+1:]
	var err error
	serverIdx, err := strconv.Atoi(serverIdxStr)
	if err != nil {
		return 0, fmt.Errorf("Failed to parse to integer %s with value %s", podName, serverIdxStr)
	} else {
		return uint(serverIdx), nil
	}
}

func stringToUint(s string) uint {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint(h.Sum32())
}

func parseReplicaIdxFromDeploymentPodName(podName string) (uint, error) {
	// pod name is in the format deploymentName-replicaSetHash-randomTermination
	tokens := strings.Split(podName, "-")
	if len(tokens) < 3 {
		return 0, fmt.Errorf("Can't decypher pod name to find server name and index. %s", podName)
	} else {
		replicaIdx := stringToUint(strings.Join(tokens[len(tokens)-2:], "-"))
		return replicaIdx, nil
	}
}
