package runner

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"path"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/volume"
	dockerclient "github.com/docker/docker/client"
)

// ToolCachePath is where the persistent toolchain cache mounts inside job
// containers; RUNNER_TOOL_CACHE points at it so actions/setup-* reuse
// toolchains instead of re-downloading per job.
const ToolCachePath = "/opt/hostedtoolcache"

const LabelCachePath = "deckhand.cachePath"

// cacheVolumeName derives a stable per-slot volume name for a container path.
// Per-slot (not shared) so concurrent jobs never populate one cache at once.
func cacheVolumeName(scaleSet string, slot int, containerPath string) string {
	sum := sha1.Sum([]byte(containerPath))
	return fmt.Sprintf("deckhand-%s-s%d-%s-%s", scaleSet, slot, path.Base(containerPath), hex.EncodeToString(sum[:4]))
}

// ensureCacheVolume creates the volume if missing and fixes its ownership for
// the runner user (uid 1001) — a fresh named volume mounted at a path absent
// from the image is root-owned and unwritable otherwise.
func (p *Provider) ensureCacheVolume(ctx context.Context, name, containerPath string) error {
	if _, err := p.cli.VolumeInspect(ctx, name); err == nil {
		return nil
	} else if !dockerclient.IsErrNotFound(err) {
		return err
	}
	_, err := p.cli.VolumeCreate(ctx, volume.CreateOptions{
		Name: name,
		Labels: map[string]string{
			LabelManaged:   "true",
			LabelScaleSet:  p.scaleSet,
			LabelCachePath: containerPath,
		},
	})
	if err != nil {
		return err
	}
	// One-shot chown via the (already pulled) runner image as root.
	created, err := p.cli.ContainerCreate(ctx, &container.Config{
		Image:      p.image,
		User:       "root",
		Entrypoint: []string{"chown"},
		Cmd:        []string{"1001:1001", "/deckhand-cache"},
		Labels:     map[string]string{LabelManaged: "true", LabelScaleSet: p.scaleSet},
	}, &container.HostConfig{
		Binds: []string{name + ":/deckhand-cache"},
	}, nil, nil, "")
	if err != nil {
		return err
	}
	defer p.cli.ContainerRemove(context.WithoutCancel(ctx), created.ID, container.RemoveOptions{Force: true})
	if err := p.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return err
	}
	if _, err := p.Wait(ctx, created.ID); err != nil {
		return err
	}
	return nil
}

// cacheBinds ensures and returns the volume binds for one slot.
func (p *Provider) cacheBinds(ctx context.Context, slot int) ([]string, error) {
	var binds []string
	paths := p.cachePaths
	if p.toolCache {
		paths = append([]string{ToolCachePath}, paths...)
	}
	for _, cp := range paths {
		name := cacheVolumeName(p.scaleSet, slot, cp)
		if err := p.ensureCacheVolume(ctx, name, cp); err != nil {
			return nil, fmt.Errorf("cache volume for %s: %w", cp, err)
		}
		binds = append(binds, name+":"+cp)
	}
	return binds, nil
}

// CacheVolume describes one persistent cache volume.
type CacheVolume struct {
	Name string
	Path string
	Slot int
}

// ListCacheVolumes returns this scale set's cache volumes.
func (p *Provider) ListCacheVolumes(ctx context.Context) ([]CacheVolume, error) {
	res, err := p.cli.VolumeList(ctx, volume.ListOptions{Filters: filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
		filters.Arg("label", LabelScaleSet+"="+p.scaleSet),
	)})
	if err != nil {
		return nil, err
	}
	var out []CacheVolume
	for _, v := range res.Volumes {
		cv := CacheVolume{Name: v.Name, Path: v.Labels[LabelCachePath], Slot: -1}
		// Slot index is recoverable from the name (deckhand-<set>-s<N>-...).
		var n int
		if _, err := fmt.Sscanf(v.Name, "deckhand-"+p.scaleSet+"-s%d-", &n); err == nil {
			cv.Slot = n
		}
		out = append(out, cv)
	}
	return out, nil
}

// WipeCacheVolumes removes cache volumes (all slots, or one). Volumes bound
// to a running container refuse removal — that error surfaces to the caller.
func (p *Provider) WipeCacheVolumes(ctx context.Context, slot int) (int, error) {
	vols, err := p.ListCacheVolumes(ctx)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, v := range vols {
		if slot >= 0 && v.Slot != slot {
			continue
		}
		if err := p.cli.VolumeRemove(ctx, v.Name, false); err != nil {
			return removed, fmt.Errorf("%s: %w (slot mid-job? drain first)", v.Name, err)
		}
		removed++
	}
	return removed, nil
}
