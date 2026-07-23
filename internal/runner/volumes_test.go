package runner

import (
	"fmt"
	"strings"
	"testing"
)

func TestCacheVolumeNameStableAndPerSlot(t *testing.T) {
	a := cacheVolumeName("deckhand", 0, "/home/runner/.npm")
	b := cacheVolumeName("deckhand", 0, "/home/runner/.npm")
	if a != b {
		t.Fatalf("volume name must be deterministic: %q vs %q", a, b)
	}
	if cacheVolumeName("deckhand", 1, "/home/runner/.npm") == a {
		t.Fatal("different slots must get different volumes (concurrent-populate races)")
	}
	if cacheVolumeName("deckhand", 0, ToolCachePath) == a {
		t.Fatal("different paths must get different volumes")
	}
	if !strings.HasPrefix(a, "deckhand-deckhand-s0-") {
		t.Fatalf("name should carry scale set + slot for operability: %q", a)
	}
	// The Slot recovery in ListCacheVolumes depends on this exact prefix shape.
	var n int
	if _, err := fmt.Sscanf(cacheVolumeName("deckhand", 3, "/x"), "deckhand-deckhand-s%d-", &n); err != nil || n != 3 {
		t.Fatalf("slot not recoverable: n=%d err=%v", n, err)
	}
}
