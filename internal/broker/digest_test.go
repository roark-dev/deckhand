package broker

import (
	"strings"
	"testing"

	"github.com/roark-dev/deckhand/internal/config"
)

func TestIsDigestPinned(t *testing.T) {
	cases := []struct {
		image string
		want  bool
	}{
		{"ghcr.io/actions/actions-runner@sha256:" + strings.Repeat("a", 64), true},
		{config.DefaultRunnerImage, false}, // the default ":latest" image — must NOT panic
		{"", false},
		{"@sha256:", true},
		{"short", false},
		// A 37-char non-pinned image reproduces the original out-of-bounds panic.
		{strings.Repeat("x", 37), false},
	}
	for _, c := range cases {
		if got := isDigestPinned(c.image); got != c.want {
			t.Errorf("isDigestPinned(%q) = %v, want %v", c.image, got, c.want)
		}
	}
}
