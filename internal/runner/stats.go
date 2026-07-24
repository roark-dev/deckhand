package runner

import (
	"context"
	"encoding/json"

	"github.com/docker/docker/api/types/container"
)

// Stats is one container's live resource use: CPU as a fraction of cores
// (1.5 means a core and a half) and resident memory in bytes.
type Stats struct {
	CPUCores float64
	MemBytes int64
}

// SampleStats reads one container's current CPU/memory usage. It uses the
// stats endpoint in non-stream mode, which makes the daemon compute the CPU
// delta over a ~1s window — a one-shot read cannot, having no prior sample to
// diff against. It therefore blocks for ~1s, so callers sample off the hot
// path (the broker does this from a background ticker, not the status call).
func (p *Provider) SampleStats(ctx context.Context, containerID string) (Stats, error) {
	resp, err := p.cli.ContainerStats(ctx, containerID, false)
	if err != nil {
		return Stats{}, err
	}
	defer resp.Body.Close()
	var s container.StatsResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return Stats{}, err
	}
	return computeStats(s), nil
}

// computeStats derives cores-in-use and memory-in-use the way `docker stats`
// does. Pure, so the arithmetic is unit-testable without a live daemon.
func computeStats(s container.StatsResponse) Stats {
	var cores float64
	cpuDelta := float64(s.CPUStats.CPUUsage.TotalUsage) - float64(s.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(s.CPUStats.SystemUsage) - float64(s.PreCPUStats.SystemUsage)
	if cpuDelta > 0 && sysDelta > 0 {
		onlineCPUs := float64(s.CPUStats.OnlineCPUs)
		if onlineCPUs == 0 {
			onlineCPUs = float64(len(s.CPUStats.CPUUsage.PercpuUsage))
		}
		// cpuDelta/sysDelta is the fraction of ALL cores this container used
		// over the window; scaling by the core count yields cores-in-use.
		cores = (cpuDelta / sysDelta) * onlineCPUs
	}

	// Memory-in-use is usage minus reclaimable page cache, matching the CLI:
	// cgroup v2 reports it as inactive_file, v1 as cache.
	mem := int64(s.MemoryStats.Usage)
	if cache, ok := s.MemoryStats.Stats["inactive_file"]; ok {
		mem -= int64(cache)
	} else if cache, ok := s.MemoryStats.Stats["cache"]; ok {
		mem -= int64(cache)
	}
	if mem < 0 {
		mem = 0
	}
	return Stats{CPUCores: cores, MemBytes: mem}
}

// HostMem reports the docker host's total physical memory in bytes.
func (p *Provider) HostMem(ctx context.Context) (int64, error) {
	info, err := p.cli.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.MemTotal, nil
}
