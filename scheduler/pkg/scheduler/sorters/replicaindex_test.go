/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package sorters

import (
	"sort"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

func TestReplicaIndexSorter(t *testing.T) {
	g := NewGomegaWithT(t)

	type test struct {
		name     string
		replicas []*CandidateReplica
		ordering []int
	}

	model := store.NewModelVersion(
		nil,
		1,
		"server1",
		map[int]store.ReplicaStatus{3: {State: store.Loading}},
		false,
		store.ModelProgressing)
	tests := []test{
		{
			name: "OrderByIndex",
			replicas: []*CandidateReplica{
				{Model: model, Replica: store.NewServerReplica("", 8080, 5001, 20, store.NewServer("dummy", true), []string{}, 100, 200, 0, map[store.ModelVersionID]bool{}, 100)},
				{Model: model, Replica: store.NewServerReplica("", 8080, 5001, 10, store.NewServer("dummy", true), []string{}, 100, 100, 0, map[store.ModelVersionID]bool{}, 100)},
				{Model: model, Replica: store.NewServerReplica("", 8080, 5001, 30, store.NewServer("dummy", true), []string{}, 100, 150, 0, map[store.ModelVersionID]bool{}, 100)},
			},
			ordering: []int{10, 20, 30},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sorter := ReplicaIndexSorter{}
			sort.SliceStable(test.replicas, func(i, j int) bool { return sorter.IsLess(test.replicas[i], test.replicas[j]) })
			for idx, expected := range test.ordering {
				g.Expect(test.replicas[idx].Replica.GetReplicaIdx()).To(Equal(expected))
			}
		})
	}
}
