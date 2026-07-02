package domain

// SystemMetrics worker 上报的机器指标(对齐 PowerJob SystemMetrics 结构)。
// 选址时按 Score 降序取首(PowerJob 约定:分数越高越空闲)。
type SystemMetrics struct {
	CpuLoad        float64 `json:"cpuLoad"`
	CpuProcessors  int     `json:"cpuProcessors"`
	DiskTotal      float64 `json:"diskTotal"`
	DiskUsage      float64 `json:"diskUsage"`
	DiskUsed       float64 `json:"diskUsed"`
	JvmMaxMemory   float64 `json:"jvmMaxMemory"`
	JvmMemoryUsage float64 `json:"jvmMemoryUsage"`
	JvmUsedMemory  float64 `json:"jvmUsedMemory"`
	Extra          string  `json:"extra"`
	Score          int     `json:"score"`
}
