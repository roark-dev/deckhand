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
}

type Options struct {
	Image              string
	ScaleSetName       string
	ExposeDockerSocket bool
	Env                map[string]string
}

func New(opts Options) (*Provider, error) {
	cli, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
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
	}, nil
}

func (p *Provider) Ping(ctx context.Context) error {
	_, err := p.cli.Ping(ctx)
	return err
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
	cfg := &container.Config{
		Image: p.image,
		User:  "runner",
		Cmd:   []string{"/home/runner/run.sh"},
		Env: append([]string{
			"ACTIONS_RUNNER_INPUT_JITCONFIG=" + jitConfig,
		}, p.extraEnv...),
		Labels: map[string]string{
			LabelManaged:    "true",
			LabelSlot:       strconv.Itoa(slot),
			LabelRunnerName: runnerName,
			LabelScaleSet:   p.scaleSet,
		},
	}
	host := &container.HostConfig{}
	if cpuset != "" {
		host.Resources.CpusetCpus = cpuset
	}
	if p.exposeDockerSocket {
		host.Binds = append(host.Binds, "/var/run/docker.sock:/var/run/docker.sock")
	}
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

// HasWorker reports whether a Runner.Worker process (an executing job) is
// alive inside the container.
func (p *Provider) HasWorker(ctx context.Context, containerID string) bool {
	top, err := p.cli.ContainerTop(ctx, containerID, nil)
	if err != nil {
		return false
	}
	for _, proc := range top.Processes {
		for _, field := range proc {
			if strings.Contains(field, "Runner.Worker") {
				return true
			}
		}
	}
	return false
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

// Managed describes an adopted container.
type Managed struct {
	ContainerID string
	Slot        int
	RunnerName  string
	Running     bool
	HasWorker   bool
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
		m := Managed{
			ContainerID: c.ID,
			Slot:        slot,
			RunnerName:  c.Labels[LabelRunnerName],
			Running:     c.State == container.StateRunning,
		}
		if m.Running {
			m.HasWorker = p.HasWorker(ctx, c.ID)
		}
		out = append(out, m)
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
