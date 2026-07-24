package runner

import (
	"testing"

	"github.com/docker/docker/api/types/container"
)

func TestComputeStats(t *testing.T) {
	// 1 core-second used out of 8 core-seconds system time on an 8-core box =
	// exactly 1 core; 600MB usage minus 100MB reclaimable cache = 500MB.
	s := container.StatsResponse{}
	s.CPUStats.CPUUsage.TotalUsage = 2_000_000_000
	s.PreCPUStats.CPUUsage.TotalUsage = 1_000_000_000
	s.CPUStats.SystemUsage = 80_000_000_000
	s.PreCPUStats.SystemUsage = 72_000_000_000
	s.CPUStats.OnlineCPUs = 8
	s.MemoryStats.Usage = 600 * 1024 * 1024
	s.MemoryStats.Stats = map[string]uint64{"inactive_file": 100 * 1024 * 1024}

	got := computeStats(s)
	if got.CPUCores < 0.999 || got.CPUCores > 1.001 {
		t.Errorf("CPUCores = %v, want ~1.0", got.CPUCores)
	}
	if want := int64(500 * 1024 * 1024); got.MemBytes != want {
		t.Errorf("MemBytes = %d, want %d", got.MemBytes, want)
	}
}

func TestComputeStatsFallsBackToPercpuAndV1Cache(t *testing.T) {
	// OnlineCPUs absent -> core count comes from the percpu slice length; only
	// the v1 "cache" key present.
	s := container.StatsResponse{}
	s.CPUStats.CPUUsage.TotalUsage = 4_000_000_000
	s.PreCPUStats.CPUUsage.TotalUsage = 2_000_000_000
	s.CPUStats.SystemUsage = 16_000_000_000
	s.PreCPUStats.SystemUsage = 8_000_000_000
	s.CPUStats.CPUUsage.PercpuUsage = []uint64{0, 0, 0, 0} // 4 cores
	s.MemoryStats.Usage = 300 * 1024 * 1024
	s.MemoryStats.Stats = map[string]uint64{"cache": 50 * 1024 * 1024}

	got := computeStats(s)
	if got.CPUCores < 0.999 || got.CPUCores > 1.001 { // (2e9/8e9)*4 = 1.0
		t.Errorf("CPUCores = %v, want ~1.0", got.CPUCores)
	}
	if want := int64(250 * 1024 * 1024); got.MemBytes != want {
		t.Errorf("MemBytes = %d, want %d", got.MemBytes, want)
	}
}

func TestComputeStatsZeroDeltaNoPanic(t *testing.T) {
	// First reading (precpu == cpu): no window to divide over -> 0 cores, and
	// never a divide-by-zero. Memory with no cache key is reported as-is.
	s := container.StatsResponse{}
	s.MemoryStats.Usage = 42
	got := computeStats(s)
	if got.CPUCores != 0 {
		t.Errorf("CPUCores = %v, want 0 on a first reading", got.CPUCores)
	}
	if got.MemBytes != 42 {
		t.Errorf("MemBytes = %d, want 42", got.MemBytes)
	}
}
