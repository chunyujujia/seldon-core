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

// CoLocationAntiAffinitySorter sorts candidate replicas so that replicas with
// fewer models sharing the same coLocationTag are preferred. This provides soft
// anti-affinity: models that are frequently called together (same tag) are
// spread across different replicas when possible.
type CoLocationAntiAffinitySorter struct {
	store store.ModelStore
}

func NewCoLocationAntiAffinitySorter(store store.ModelStore) CoLocationAntiAffinitySorter {
	return CoLocationAntiAffinitySorter{store: store}
}

func (c CoLocationAntiAffinitySorter) Name() string {
	return "CoLocationAntiAffinitySorter"
}

// IsLess returns true if candidate i is preferred over candidate j.
// A replica with fewer co-located tag matches is preferred (lower count = better).
// If the model being scheduled has no coLocationTag (nil or empty string),
// this sorter is a no-op and returns false (equal preference).
func (c CoLocationAntiAffinitySorter) IsLess(i *CandidateReplica, j *CandidateReplica) bool {
	tag := i.Model.GetModelSpec().GetCoLocationTag()
	if tag == "" {
		return false
	}

	iCount := c.countMatchingTags(i.Replica, tag)
	jCount := c.countMatchingTags(j.Replica, tag)
	return iCount < jCount
}

// countMatchingTags counts how many models loaded (or loading) on the given
// replica share the specified coLocationTag.
func (c CoLocationAntiAffinitySorter) countMatchingTags(replica *store.ServerReplica, tag string) int {
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
		otherTag := mv.GetModelSpec().GetCoLocationTag()
		if otherTag == tag {
			count++
		}
	}
	return count
}
