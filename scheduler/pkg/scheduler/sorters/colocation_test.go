/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package sorters

import (
	"fmt"
	"sort"
	"testing"

	. "github.com/onsi/gomega"

	pba "github.com/seldonio/seldon-core/apis/go/v2/mlops/agent"
	pb "github.com/seldonio/seldon-core/apis/go/v2/mlops/scheduler"
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

// fakeModelStore is a minimal ModelStore implementation for testing.
// Only GetModel is functional; all other methods are stubs.
type fakeModelStore struct {
	models map[string]*store.ModelSnapshot
}

var _ store.ModelStore = (*fakeModelStore)(nil)

func (f *fakeModelStore) GetModel(key string) (*store.ModelSnapshot, error) {
	ms, ok := f.models[key]
	if !ok {
		return nil, fmt.Errorf("model %s not found", key)
	}
	return ms, nil
}

func (f *fakeModelStore) UpdateModel(config *pb.LoadModelRequest) error { return nil }
func (f *fakeModelStore) GetModels() ([]*store.ModelSnapshot, error)    { return nil, nil }
func (f *fakeModelStore) LockModel(modelId string)                     {}
func (f *fakeModelStore) UnlockModel(modelId string)                   {}
func (f *fakeModelStore) RemoveModel(req *pb.UnloadModelRequest) error  { return nil }
func (f *fakeModelStore) GetServers(shallow bool, modelDetails bool) ([]*store.ServerSnapshot, error) {
	return nil, nil
}
func (f *fakeModelStore) GetServer(serverKey string, shallow bool, modelDetails bool) (*store.ServerSnapshot, error) {
	return nil, nil
}
func (f *fakeModelStore) UpdateLoadedModels(modelKey string, version uint32, serverKey string, replicas []*store.ServerReplica) error {
	return nil
}
func (f *fakeModelStore) UpdateGpuUsage(serverKey string, replicaIdx int, gpuUsages float64) error {
	return nil
}
func (f *fakeModelStore) UnloadVersionModels(modelKey string, version uint32) (bool, error) {
	return true, nil
}
func (f *fakeModelStore) UpdateModelState(modelKey string, version uint32, serverKey string, replicaIdx int, availableMemory *uint64, expectedState, desiredState store.ModelReplicaState, reason string) error {
	return nil
}
func (f *fakeModelStore) AddServerReplica(request *pba.AgentSubscribeRequest) error { return nil }
func (f *fakeModelStore) ServerNotify(request *pb.ServerNotifyRequest) error        { return nil }
func (f *fakeModelStore) RemoveServerReplica(serverName string, replicaIdx int) ([]string, error) {
	return nil, nil
}
func (f *fakeModelStore) DrainServerReplica(serverName string, replicaIdx int) ([]string, error) {
	return nil, nil
}
func (f *fakeModelStore) UpdateServerScaleToReplicas(serverName string, scaleToReplicas int32) {}
func (f *fakeModelStore) FailedScheduling(modelVersion *store.ModelVersion, reason string, reset bool) {
}
func (f *fakeModelStore) GetAllModels() []string { return nil }

func strPtr(s string) *string {
	return &s
}

func TestCoLocationAntiAffinitySorter(t *testing.T) {
	g := NewGomegaWithT(t)

	type test struct {
		name     string
		replicas []*CandidateReplica
		ordering []int // expected replica indices after sorting
	}

	server := store.NewServer("server1", true)

	// Create models with co-location tags
	modelA := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelA"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", CoLocationTag: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	modelB := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelB"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", CoLocationTag: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	modelC := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelC"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", CoLocationTag: strPtr("group-b")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	// Model without tag — scheduling this model should not be affected by sorter
	modelNoTag := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "modelNoTag"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test"},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	// The model we are scheduling — has tag "group-a"
	schedulingModel := store.NewModelVersion(
		&pb.Model{
			Meta:      &pb.MetaData{Name: "schedulingModel"},
			ModelSpec: &pb.ModelSpec{Uri: "gs://test", CoLocationTag: strPtr("group-a")},
		},
		1, "server1",
		map[int]store.ReplicaStatus{},
		false,
		store.ModelProgressing,
	)

	fakeStore := &fakeModelStore{
		models: map[string]*store.ModelSnapshot{
			"modelA": {
				Name:     "modelA",
				Versions: []*store.ModelVersion{modelA},
			},
			"modelB": {
				Name:     "modelB",
				Versions: []*store.ModelVersion{modelB},
			},
			"modelC": {
				Name:     "modelC",
				Versions: []*store.ModelVersion{modelC},
			},
			"modelNoTag": {
				Name:     "modelNoTag",
				Versions: []*store.ModelVersion{modelNoTag},
			},
		},
	}

	// Replica 1: has modelA (group-a) and modelB (group-a) loaded — 2 matches
	// Replica 2: has modelC (group-b) loaded — 0 matches
	// Replica 3: has modelA (group-a) loaded — 1 match
	// Expected order after sort: replica 2 (0 matches), replica 3 (1 match), replica 1 (2 matches)

	mvIDA1 := store.ModelVersionID{Name: "modelA", Version: 1}
	mvIDB1 := store.ModelVersionID{Name: "modelB", Version: 1}
	mvIDC1 := store.ModelVersionID{Name: "modelC", Version: 1}

	tests := []test{
		{
			name: "PreferReplicaWithFewerMatchingTags",
			replicas: []*CandidateReplica{
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 1, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDA1: true, mvIDB1: true}, 100),
				},
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 2, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDC1: true}, 100),
				},
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 3, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDA1: true}, 100),
				},
			},
			ordering: []int{2, 3, 1},
		},
		{
			name: "NoTagModelUnaffected",
			replicas: []*CandidateReplica{
				{
					Model:   modelNoTag,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 1, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDA1: true, mvIDB1: true}, 100),
				},
				{
					Model:   modelNoTag,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 2, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDC1: true}, 100),
				},
				{
					Model:   modelNoTag,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 3, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{}, 100),
				},
			},
			// No tag means sorter returns false for all comparisons — order is preserved (stable sort)
			ordering: []int{1, 2, 3},
		},
		{
			name: "AllReplicasEqualMatchingTags",
			replicas: []*CandidateReplica{
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 1, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDA1: true}, 100),
				},
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 2, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDB1: true}, 100),
				},
			},
			// Both replicas have 1 matching tag — order is preserved (stable sort)
			ordering: []int{1, 2},
		},
		{
			name: "EmptyReplica",
			replicas: []*CandidateReplica{
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 1, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{mvIDA1: true}, 100),
				},
				{
					Model:   schedulingModel,
					Server:  &store.ServerSnapshot{Name: "server1"},
					Replica: store.NewServerReplica("", 8080, 5001, 2, server, []string{}, 100, 100, 0, map[store.ModelVersionID]bool{}, 100),
				},
			},
			// Replica 2 has 0 matches, replica 1 has 1 match — replica 2 preferred
			ordering: []int{2, 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sorter := NewCoLocationAntiAffinitySorter(fakeStore)
			sort.SliceStable(test.replicas, func(i, j int) bool {
				return sorter.IsLess(test.replicas[i], test.replicas[j])
			})
			for idx, expected := range test.ordering {
				g.Expect(test.replicas[idx].Replica.GetReplicaIdx()).To(Equal(expected))
			}
		})
	}
}
