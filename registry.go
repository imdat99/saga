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

// registeredComp is a compensator with its retry policy.
type registeredComp struct {
	fn     compensator
	policy RetryPolicy
}

var registry = struct {
	sync.RWMutex
	compensators map[string]registeredComp
	steps        map[string]struct{}
	workflows    map[workflowID]workflowRunner
}{
	compensators: map[string]registeredComp{},
	steps:        map[string]struct{}{},
	workflows:    map[workflowID]workflowRunner{},
}

// workflowID keys the registry without formatting a string per lookup (one
// lookup per run).
type workflowID struct {
	name    string
	version int
}

func registerStep(name string, comp compensator, policy RetryPolicy) {
	registry.Lock()
	defer registry.Unlock()
	if _, dup := registry.steps[name]; dup {
		panic(fmt.Sprintf("saga: step %q defined twice", name))
	}
	registry.steps[name] = struct{}{}
	if comp != nil {
		registry.compensators[name] = registeredComp{fn: comp, policy: policy}
	}
}

func lookupCompensator(name string) (registeredComp, bool) {
	registry.RLock()
	defer registry.RUnlock()
	c, ok := registry.compensators[name]
	return c, ok
}

func workflowKey(name string, version int) string { return fmt.Sprintf("%s@%d", name, version) }

func registerWorkflow(name string, version int, run workflowRunner) {
	registry.Lock()
	defer registry.Unlock()
	key := workflowID{name: name, version: version}
	if _, dup := registry.workflows[key]; dup {
		panic(fmt.Sprintf("saga: workflow %s defined twice", workflowKey(name, version)))
	}
	registry.workflows[key] = run
}

func lookupWorkflow(name string, version int) (workflowRunner, bool) {
	registry.RLock()
	defer registry.RUnlock()
	w, ok := registry.workflows[workflowID{name: name, version: version}]
	return w, ok
}
