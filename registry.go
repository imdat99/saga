package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// compensator undoes a completed step from its persisted compensation data.
// It is looked up by step name, so recovery can compensate a run without
// re-executing its workflow function.
type compensator func(ctx context.Context, k Key, compData json.RawMessage) error

// workflowRunner executes a workflow function from its persisted input.
type workflowRunner func(c *runCtx, input json.RawMessage) (json.RawMessage, error)

var registry = struct {
	sync.RWMutex
	compensators map[string]compensator
	steps        map[string]struct{}
	workflows    map[string]workflowRunner
}{
	compensators: map[string]compensator{},
	steps:        map[string]struct{}{},
	workflows:    map[string]workflowRunner{},
}

func registerStep(name string, comp compensator) {
	registry.Lock()
	defer registry.Unlock()
	if _, dup := registry.steps[name]; dup {
		panic(fmt.Sprintf("saga: step %q defined twice", name))
	}
	registry.steps[name] = struct{}{}
	if comp != nil {
		registry.compensators[name] = comp
	}
}

func lookupCompensator(name string) (compensator, bool) {
	registry.RLock()
	defer registry.RUnlock()
	c, ok := registry.compensators[name]
	return c, ok
}

func workflowKey(name string, version int) string { return fmt.Sprintf("%s@%d", name, version) }

func registerWorkflow(name string, version int, run workflowRunner) {
	registry.Lock()
	defer registry.Unlock()
	key := workflowKey(name, version)
	if _, dup := registry.workflows[key]; dup {
		panic(fmt.Sprintf("saga: workflow %s defined twice", key))
	}
	registry.workflows[key] = run
}

func lookupWorkflow(name string, version int) (workflowRunner, bool) {
	registry.RLock()
	defer registry.RUnlock()
	w, ok := registry.workflows[workflowKey(name, version)]
	return w, ok
}
