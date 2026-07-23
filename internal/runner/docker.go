// Package runner spawns and supervises ephemeral GitHub Actions runner
// containers over the Docker API. All lease truth lives in container labels —
// a restarted daemon re-learns its fleet from `docker ps`, never from memory.
package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	dockerclient "github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	LabelManaged    = "deckhand.managed"
	LabelSlot       = "deckhand.slot"
	LabelRunnerName = "deckhand.runnerName"
	LabelScaleSet   = "deckhand.scaleSet"
)

type Provider struct {
	cli      *dockerclient.Client
	image    string
	scaleSet string
	// exposeDockerSocket bind-mounts the docker socket into job containers
	// (root-equivalent on the docker host; off unless configured).
	exposeDockerSocket bool
	extraEnv           []string
	memoryBytes        int64
	pidsLimit          int64
	toolCache          bool
	cachePaths         []string
	allowPrivEsc       bool
}

type Options struct {
	Image              string
	ScaleSetName       string
	ExposeDockerSocket bool
	Env                map[string]string
	// MemoryBytes caps each job container's memory (0 = unlimited).
	MemoryBytes int64
	// PidsLimit caps processes per job container (0 = unlimited).
	PidsLimit int64
	// ToolCache mounts a persistent per-slot RUNNER_TOOL_CACHE volume.
	ToolCache bool
	// CachePaths are additional absolute container paths persisted per slot.
	CachePaths []string
	// AllowPrivilegeEscalation drops the no-new-privileges flag so the
	// image's sudo works (needed by workflows that apt-get provision tools).
	AllowPrivilegeEscalation bool
}

func New(opts Options) (*Provider, error) {
	clientOpts := []dockerclient.Opt{dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation()}
	// Honor the docker CLI's current context (Colima, OrbStack, ...) — the
	// SDK alone would silently target /var/run/docker.sock.
	if host := resolveDockerHost(); host != "" {
		clientOpts = append(clientOpts, dockerclient.WithHost(host))
	}
	cli, err := dockerclient.NewClientWithOpts(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	var env []string
	for k, v := range opts.Env {
		env = append(env, k+"="+v)
	}
	return &Provider{
		cli:                cli,
		image:              opts.Image,
		scaleSet:           opts.ScaleSetName,
		exposeDockerSocket: opts.ExposeDockerSocket,
		extraEnv:           env,
		memoryBytes:        opts.MemoryBytes,
		pidsLimit:          opts.PidsLimit,
		toolCache:          opts.ToolCache,
		cachePaths:         opts.CachePaths,
		allowPrivEsc:       opts.AllowPrivilegeEscalation,
	}, nil
}

func (p *Provider) Ping(ctx context.Context) error {
	_, err := p.cli.Ping(ctx)
	return err
}

// NCPU reports the docker host's CPU count (the denominator for
// oversubscription math: slots × cpus_per_slot should not exceed it).
func (p *Provider) NCPU(ctx context.Context) (int, error) {
	info, err := p.cli.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.NCPU, nil
}

// EnsureImage pulls the runner image if it isn't present locally.
func (p *Provider) EnsureImage(ctx context.Context) error {
	_, err := p.cli.ImageInspect(ctx, p.image)
	if err == nil {
		return nil
	}
	rc, err := p.cli.ImagePull(ctx, p.image, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull %s: %w", p.image, err)
	}
	defer rc.Close()
	if _, err := io.Copy(io.Discard, rc); err != nil {
		return fmt.Errorf("pull %s: %w", p.image, err)
	}
	return nil
}

// Spawn creates and starts one ephemeral runner container. The JIT config is
// passed via env only — it is the single secret a job container ever holds.
func (p *Provider) Spawn(ctx context.Context, slot int, runnerName, cpuset, jitConfig string) (string, error) {
	env := []string{"ACTIONS_RUNNER_INPUT_JITCONFIG=" + jitConfig}
	if p.toolCache {
		env = append(env, "RUNNER_TOOL_CACHE="+ToolCachePath)
	}
	cfg := &container.Config{
		Image: p.image,
		User:  "runner",
		Cmd:   []string{"/home/runner/run.sh"},
		Env:   append(env, p.extraEnv...),
		Labels: map[string]string{
			LabelManaged:    "true",
			LabelSlot:       strconv.Itoa(slot),
			LabelRunnerName: runnerName,
			LabelScaleSet:   p.scaleSet,
		},
	}
	host := &container.HostConfig{}
	if !p.allowPrivEsc {
		// Job code must not escalate via setuid binaries. Opt-out exists for
		// workflows that need the image's sudo (apt-get provisioning).
		host.SecurityOpt = []string{"no-new-privileges"}
	}
	if cpuset != "" {
		host.Resources.CpusetCpus = cpuset
	}
	if p.memoryBytes > 0 {
		host.Resources.Memory = p.memoryBytes
		host.Resources.MemorySwap = p.memoryBytes // no extra swap beyond the cap
	}
	if p.pidsLimit > 0 {
		limit := p.pidsLimit
		host.Resources.PidsLimit = &limit
	}
	if p.exposeDockerSocket {
		host.Binds = append(host.Binds, "/var/run/docker.sock:/var/run/docker.sock")
	}
	cacheBinds, err := p.cacheBinds(ctx, slot)
	if err != nil {
		return "", err
	}
	host.Binds = append(host.Binds, cacheBinds...)
	created, err := p.cli.ContainerCreate(ctx, cfg, host, nil, nil, runnerName)
	if err != nil {
		return "", fmt.Errorf("create %s: %w", runnerName, err)
	}
	if err := p.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		_ = p.cli.ContainerRemove(ctx, created.ID, container.RemoveOptions{Force: true})
		return "", fmt.Errorf("start %s: %w", runnerName, err)
	}
	return created.ID, nil
}

// Wait blocks until the container exits and returns its exit code.
func (p *Provider) Wait(ctx context.Context, containerID string) (int64, error) {
	waitC, errC := p.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case res := <-waitC:
		return res.StatusCode, nil
	case err := <-errC:
		return 0, err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Remove force-removes a container. Callers are responsible for the
// never-kill-a-mid-job-runner rule; use HasWorker first where that matters.
func (p *Provider) Remove(ctx context.Context, containerID string) error {
	err := p.cli.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})
	if err != nil && dockerclient.IsErrNotFound(err) {
		return nil
	}
	return err
}

// Exists reports whether the container is known to docker (running or not).
func (p *Provider) Exists(ctx context.Context, containerID string) (bool, error) {
	_, err := p.cli.ContainerInspect(ctx, containerID)
	if err == nil {
		return true, nil
	}
	if dockerclient.IsErrNotFound(err) {
		return false, nil
	}
	return false, err
}

// HasWorker reports whether a Runner.Worker process (an executing job) is
// alive inside the container. The error return matters: callers deciding
// whether it is safe to remove a container MUST treat an error as "assume a
// worker is alive" — a probe failure must never license killing a job.
func (p *Provider) HasWorker(ctx context.Context, containerID string) (bool, error) {
	top, err := p.cli.ContainerTop(ctx, containerID, nil)
	if err != nil {
		if dockerclient.IsErrNotFound(err) {
			return false, nil // no container, definitively no worker
		}
		return false, err
	}
	for _, proc := range top.Processes {
		for _, field := range proc {
			if strings.Contains(field, "Runner.Worker") {
				return true, nil
			}
		}
	}
	return false, nil
}

// LogsTail returns the last n lines of a container's output (demuxed).
func (p *Provider) LogsTail(ctx context.Context, containerID string, n int) (string, error) {
	rc, err := p.cli.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true, ShowStderr: true, Tail: strconv.Itoa(n),
	})
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := stdcopy.StdCopy(&buf, &buf, rc); err != nil && err != io.EOF {
		return buf.String(), err
	}
	return buf.String(), nil
}

// Managed describes a container found via labels.
type Managed struct {
	ContainerID string
	Slot        int
	RunnerName  string
	Running     bool
}

// ListManaged finds all deckhand containers for this scale set, running or
// exited — the crash-recovery source of truth.
func (p *Provider) ListManaged(ctx context.Context) ([]Managed, error) {
	f := filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
		filters.Arg("label", LabelScaleSet+"="+p.scaleSet),
	)
	list, err := p.cli.ContainerList(ctx, container.ListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}
	var out []Managed
	for _, c := range list {
		slot, err := strconv.Atoi(c.Labels[LabelSlot])
		if err != nil {
			continue
		}
		out = append(out, Managed{
			ContainerID: c.ID,
			Slot:        slot,
			RunnerName:  c.Labels[LabelRunnerName],
			Running:     c.State == container.StateRunning,
		})
	}
	return out, nil
}

// PruneExited removes exited managed containers older than the given age.
func (p *Provider) PruneExited(ctx context.Context, olderThan time.Duration) (int, error) {
	list, err := p.ListManaged(ctx)
	if err != nil {
		return 0, err
	}
	pruned := 0
	for _, m := range list {
		if m.Running {
			continue
		}
		inspect, err := p.cli.ContainerInspect(ctx, m.ContainerID)
		if err != nil {
			continue
		}
		finished, err := time.Parse(time.RFC3339Nano, inspect.State.FinishedAt)
		if err != nil || time.Since(finished) < olderThan {
			continue
		}
		if err := p.Remove(ctx, m.ContainerID); err == nil {
			pruned++
		}
	}
	return pruned, nil
}
