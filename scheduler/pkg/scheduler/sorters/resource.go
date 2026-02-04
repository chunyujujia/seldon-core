package sorters

import "math"

type AvailableResourceSorter struct{}

func (s AvailableResourceSorter) Name() string {
	return "AvailableResourceSorter"
}

func (s AvailableResourceSorter) IsLess(i *CandidateReplica, j *CandidateReplica) bool {
	// Calculate available memory ratio (0-1 range)
	// This normalizes memory to a percentage of total capacity
	iAvailMem := math.Max(0, float64(i.Replica.GetAvailableMemory()-i.Replica.GetReservedMemory()))
	jAvailMem := math.Max(0, float64(j.Replica.GetAvailableMemory()-j.Replica.GetReservedMemory()))

	iTotalMem := float64(i.Replica.GetMemory())
	jTotalMem := float64(j.Replica.GetMemory())

	// Avoid division by zero
	iAvailMemRatio := 0.0
	if iTotalMem > 0 {
		iAvailMemRatio = iAvailMem / iTotalMem
	}
	jAvailMemRatio := 0.0
	if jTotalMem > 0 {
		jAvailMemRatio = jAvailMem / jTotalMem
	}

	// Calculate available GPU ratio (0-1 range)
	iAvailGpuRatio := (100.0 - i.Replica.GetGpuUsage() - i.Replica.GetReservedGpuUsage()) / 100.0
	jAvailGpuRatio := (100.0 - j.Replica.GetGpuUsage() - j.Replica.GetReservedGpuUsage()) / 100.0

	// Composite score using weighted average
	const memoryWeight = 0.3
	const gpuWeight = 1 - memoryWeight

	iScore := iAvailMemRatio*memoryWeight + iAvailGpuRatio*gpuWeight
	jScore := jAvailMemRatio*memoryWeight + jAvailGpuRatio*gpuWeight

	return iScore > jScore
}
