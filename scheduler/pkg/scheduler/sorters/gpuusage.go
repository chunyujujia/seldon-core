package sorters

type GpuUsageSorter struct{}

func (s GpuUsageSorter) Name() string {
	return "GpuUsageSorter"
}

func (s GpuUsageSorter) IsLess(i *CandidateReplica, j *CandidateReplica) bool {
	return i.Replica.GetGpuUsage()+ i.Replica.GetReservedGpuUsage() < j.Replica.GetGpuUsage() + j.Replica.GetReservedGpuUsage()
}
