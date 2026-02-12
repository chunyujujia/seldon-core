/*
Copyright (c) 2024 Seldon Technologies Ltd.

Use of this software is governed by
(1) the license included in the LICENSE file or
(2) if the license included in the LICENSE file is the Business Source License 1.1,
the Change License after the Change Date as each is defined in accordance with the LICENSE file.
*/

package sorters

import (
	"github.com/seldonio/seldon-core/scheduler/v2/pkg/store"
)

// SpreadGroupSorter sorts candidate replicas so that replicas with
// fewer models sharing the same spreadGroup are preferred. This provides soft
// anti-affinity: models in the same spread group are distributed
// across different replicas when possible.
type SpreadGroupSorter struct {
	store store.ModelStore
}

func NewSpreadGroupSorter(store store.ModelStore) SpreadGroupSorter {
	return SpreadGroupSorter{store: store}
}

func (c SpreadGroupSorter) Name() string {
	return "SpreadGroupSorter"
}

// IsLess returns true if candidate i is preferred over candidate j.
// A replica with fewer spread group matches is preferred (lower count = better).
// If the model being scheduled has no spreadGroup (nil or empty string),
// this sorter is a no-op and returns false (equal preference).
func (c SpreadGroupSorter) IsLess(i *CandidateReplica, j *CandidateReplica) bool {
	tag := i.Model.GetModelSpec().GetSpreadGroup()
	if tag == "" {
		return false
	}

	iCount := c.countMatchingTags(i.Replica, tag)
	jCount := c.countMatchingTags(j.Replica, tag)
	return iCount < jCount
}

// countMatchingTags counts how many models loaded (or loading) on the given
// replica share the specified spreadGroup.
func (c SpreadGroupSorter) countMatchingTags(replica *store.ServerReplica, tag string) int {
	count := 0
	for _, mvID := range replica.GetLoadedOrLoadingModelVersions() {
		modelSnapshot, err := c.store.GetModel(mvID.Name)
		if err != nil || modelSnapshot == nil {
			continue
		}
		mv := modelSnapshot.GetVersion(mvID.Version)
		if mv == nil {
			continue
		}
		otherTag := mv.GetModelSpec().GetSpreadGroup()
		if otherTag == tag {
			count++
		}
	}
	return count
}
