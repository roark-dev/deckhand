# deckhand

Self-hosted GitHub Actions runners on your own machine — **one registration in
GitHub, load-balanced across local container slots, with a live TUI dashboard.**

```
$ deckhand dash

deckhand  active
scale set "deckhand" on https://github.com/me/repo — session 42m

  SLOT  STATE     ELAPSED  JOB
  0     busy      3m12s    test-shard-1  me/repo
  1     busy      1m40s    lint          me/repo
  2     ready     12s      waiting for a job
  3     idle      55m

  2/4 busy   jobs 148 ok / 3 failed   zombies reclaimed 1

 12:04:11 slot 0  job started: test-shard-1 (me/repo)
 12:05:02 slot 1  job started: lint (me/repo)

[+/-] slots  [p] pause/resume  [d] drain  [s] stop daemon  [q] quit
```

## Why

Hosted runner minutes are expensive; a laptop or spare box is usually idle.
Classic self-hosted setups make you register and babysit N runner entities.
deckhand uses GitHub's [Runner Scale Set APIs](https://github.com/actions/scaleset)
(the protocol behind Actions Runner Controller) instead:

- **One scale set entity** registered with GitHub — GitHub queues jobs against
  it, deckhand accepts as many as it has capacity for.
- **Slots, tunable at runtime** — `deckhand scale 6` (or `+`/`-` in the TUI).
  A slot is capacity for one concurrent job; each accepted job runs in a fresh
  ephemeral container ([`actions-runner`](https://github.com/actions/runner)
  image) that is destroyed afterwards. No state leaks between jobs.
- **A real dashboard** — slots, jobs, elapsed time, an event feed, and
  controls, over a local unix socket.

## Quick start

```sh
brew install go && go install github.com/roark-dev/deckhand/cmd/deckhand@latest  # or grab a release binary

deckhand init      # asks for your repo/org URL, token env var, slot count
export DECKHAND_GITHUB_TOKEN=<fine-grained PAT>
deckhand doctor    # verifies docker + GitHub connectivity
deckhand up        # the daemon (foreground; see templates/ for launchd/systemd)
deckhand dash      # dashboard, from another terminal
```

Then point workflows at it:

```yaml
jobs:
  test:
    runs-on: deckhand   # the scale set name from `deckhand init`
```

**Token**: a fine-grained PAT with **Administration: read/write** on the target
repo (repo-level), or **Self-hosted runners: read/write** on the org
(org-level). GitHub Apps are also supported (`github.auth.app` in the config)
and recommended for long-lived installs — see `examples/config.yaml`.

**Requirements**: a Docker daemon (Colima, OrbStack, Docker Desktop, or native
Linux) and Go 1.26+ if building from source. Runners are linux containers on
whatever architecture your docker host runs.

## Configuration

`~/.deckhand/config.yaml` (see [`examples/config.yaml`](examples/config.yaml)
for every field):

```yaml
github:
  url: https://github.com/me/repo        # or an org URL
  auth:
    token_env: DECKHAND_GITHUB_TOKEN     # or token_file / token / app
scale_set:
  name: deckhand                          # what `runs-on:` references
slots:
  count: 4        # max concurrent jobs; also tunable at runtime
  warm: 0         # keep N runners pre-registered for instant job pickup
  cpus_per_slot: 2 # optional cpuset pinning (0 = off)
runner:
  image: ghcr.io/actions/actions-runner:latest  # pin a digest! (daemon + doctor warn on tags)
  memory_mb: 4096   # per-job memory cap (0 = unlimited)
  pids_limit: 2048  # per-job process cap (fork-bomb protection)
metrics:
  listen: 127.0.0.1:9642   # optional Prometheus endpoint
```

## How it works

```
GitHub (one runner scale set)
   │  long-poll message session: job available / started / completed
   ▼
deckhand daemon ── control API (unix socket) ── TUI / CLI
   │  per accepted job: mint a single-job JIT config,
   ▼  run one ephemeral runner container, destroy it after
docker (Colima / OrbStack / Docker Desktop / Linux)
```

- The daemon holds the **only** credential (PAT or App key). A job container
  receives exactly one secret: its single-job JIT runner config, via env.
- Containers are labeled; if the daemon restarts it **adopts** the running
  fleet from `docker ps` — a mid-job runner is never orphaned or killed.
- Laptop-aware: sleep/wake is detected and the GitHub session resyncs;
  docker-down means deckhand stops accepting jobs (they queue on GitHub)
  rather than accepting work it can't run.
- A runner whose registration GitHub reaped (network blip mid-handshake) is
  detected — jobless container + gone from GitHub, debounced — and reclaimed
  automatically.

## Security model

Self-hosted runners execute whatever code lands in your workflows. Baseline
rules, enforced or defaulted by deckhand:

- Job containers get **no host mounts** and no credentials beyond their
  single-job JIT config. They run with `no-new-privileges`, a pids limit
  (default 2048) and an optional memory cap (`runner.memory_mb`).
  `runner.expose_docker_socket` exists for docker-in-workflow needs but hands
  job code root-equivalent control of the docker host — leave it off unless
  you need it and trust every workflow.
- Workflow-controlled text (job names, logs) is stripped of terminal escape
  sequences before it reaches your terminal, the event stream, or the log.
- The control socket is owner-only from creation and every connection's peer
  uid is verified (`SO_PEERCRED`/`LOCAL_PEERCRED`).
- Pin `runner.image` by digest (`image@sha256:...`) — a mutable tag is a
  supply-chain risk; the daemon and `deckhand doctor` warn if you don't.
- **Never** attach deckhand to a public repo that accepts fork PRs; fork code
  would execute on your machine.
- On macOS + Colima, keep the VM's mounts restricted (`templates/colima.yaml`)
  so container escapes via an exposed docker socket can't reach `~/.aws`,
  `~/.ssh`, etc. `deckhand doctor` checks this posture.
- Put the GitHub credential in `token_file` (0600) rather than env vars when
  running under launchd/systemd — see the comments in `templates/`.

## Operations

| Task | Command |
|---|---|
| Dashboard | `deckhand dash` |
| One-shot status | `deckhand status` |
| Change capacity | `deckhand scale 6` |
| Stop taking jobs (queue on GitHub) | `deckhand pause` / `resume` |
| Finish jobs, take nothing new | `deckhand drain` |
| Stop daemon (drains first) | `deckhand stop` (`--now --force` to kill) |
| A slot's container logs | `deckhand logs 2 --tail 200` |
| Free a wedged slot | `deckhand reclaim 2` |
| Health checks | `deckhand doctor` |

Run at login: `templates/com.deckhand.daemon.plist` (macOS launchd) or
`templates/deckhand.service` (systemd).

## Status

Early. The scale-set API is GitHub **public preview**; deckhand pins
`actions/scaleset` and may need updates as that API evolves. Issues and PRs
welcome.

## License

MIT
