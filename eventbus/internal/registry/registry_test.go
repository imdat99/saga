package registry

import (
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

func route(g string, attempts int) *Route {
	return &Route{Group: g, Suffix: "." + g, Attempts: attempts, Backoff: bullmq.ExponentialBackoff(time.Second)}
}

func TestUnion(t *testing.T) {
	a := &Table{Version: 3, Epoch: "new", ByEvent: map[string][]*Route{}, Groups: map[string]*Route{}}
	b := &Table{Version: 9, Epoch: "old", ByEvent: map[string][]*Route{}, Groups: map[string]*Route{}}
	a.Groups["x"] = route("x", 7)
	a.ByEvent["e1"] = []*Route{a.Groups["x"]}
	b.Groups["x"] = route("x", 1)
	b.Groups["y"] = route("y", 2)
	b.ByEvent["e1"] = []*Route{b.Groups["x"], b.Groups["y"]}
	b.ByEvent["e2"] = []*Route{b.Groups["y"]}

	u := Union(a, b)
	if u.Version != 3 || u.Epoch != "new" {
		t.Fatalf("version/epoch = %d/%s, want a's", u.Version, u.Epoch)
	}
	if len(u.ByEvent["e1"]) != 2 || len(u.ByEvent["e2"]) != 1 {
		t.Fatalf("routes = %v", u.ByEvent)
	}
	for _, r := range u.ByEvent["e1"] {
		if r.Group == "x" && r.Attempts != 7 {
			t.Fatalf("group x policy from b, want a's (attempts 7)")
		}
	}
}
