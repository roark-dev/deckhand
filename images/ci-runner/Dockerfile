# deckhand's own CI runner image: the official GitHub Actions runner plus the
# Go build toolchain deckhand's CI needs. The stock actions-runner image ships
# git/curl/tar but NOT make or a C compiler, so `make lint`/`make cover` and the
# race detector (which needs cgo → gcc) can't run on it. Go itself still comes
# from actions/setup-go at job time; only the native toolchain is baked in.
#
# Baking the tools in (rather than apt-get in the workflow) keeps job containers
# running with no_new_privileges — nothing escalates at run time.
FROM ghcr.io/actions/actions-runner:latest

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*
USER runner
