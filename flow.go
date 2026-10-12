package wiggle

import (
	"errors"
	"fmt"
	"time"
)

// Define defines a workflow: body chains its steps on f, and Define compiles them into the FlowSpec
// you register. Steps are named, and a worker binds their handlers by those names; nothing in body
// runs a step. version 0 publishes the graph under a hash of its content.
//
//	order, err := wiggle.Define("order", 1, func(f *wiggle.Flow) {
//		f.ThenApply("validate").
//			ThenFilter("in-stock").
//			ThenApply("charge", wiggle.OnQueue("payments")).
//			ThenAccept("notify")
//	})
//
// It returns an error for an invalid topology: a duplicate step name, an empty arm or body, a
// ThenAllOf or ThenForEach without its Combine, a bad choice, an escalation without a timeout.
func Define(name string, version int, body func(f *Flow)) (*FlowSpec, error) {
	if body == nil {
		return nil, fmt.Errorf("workflow %q has no body", name)
	}
	f := &Flow{name: name, version: version}
	f.root = f
	body(f)
	if err := f.Err(); err != nil {
		return nil, err
	}
	return compile(f)
}

// MustDefine is Define that panics on error -- for a topology known to be valid when it is written.
func MustDefine(name string, version int, body func(f *Flow)) *FlowSpec {
	spec, err := Define(name, version, body)
	if err != nil {
		panic(err)
	}
	return spec
}

// Flow is the chain of steps Define records. Each Then method appends a step and returns the flow;
// the arms of ThenAllOf, ThenOneOf and the bodies of ThenForEach and RepeatWhile are flows of their
// own.
type Flow struct {
	root  *Flow
	steps []node
	err   error
	// pending names the ThenAllOf or ThenForEach still waiting for its Combine.
	pending string

	// root only
	name         string
	version      int
	defaultQueue string
	attributes   []dispatchAttribute
}

// DefaultQueue sets the queue of every step that does not name its own (default: the workflow name).
func (f *Flow) DefaultQueue(queue string) *Flow {
	f.root.defaultQueue = queue
	return f
}

// Attribute declares a value queue criteria can test (criteria.Attr(name)): path is a dotted path into
// the context a step receives, read as typ -- STRING, LONG, DECIMAL or BOOLEAN. The server extracts it
// each time a step becomes ready; a value that is missing or does not fit the type is absent.
func (f *Flow) Attribute(name, typ, path string) *Flow {
	f.root.attributes = append(f.root.attributes, dispatchAttribute{Name: name, Type: typ, Path: path})
	return f
}

// ThenApply chains a task: its handler (Worker.Handle) returns the new context.
func (f *Flow) ThenApply(name string, opts ...StepOption) *Flow {
	return f.task(name, false, opts)
}

// ThenApplyCompensable is ThenApply for a step whose undo runs in the reverse pass if the instance
// later fails. Bind the undo with Worker.HandleCompensation, or a Compensate<Step> method.
func (f *Flow) ThenApplyCompensable(name string, opts ...StepOption) *Flow {
	return f.task(name, true, opts)
}

// ThenAccept chains a side effect (Worker.HandleEffect): the context is left unchanged.
func (f *Flow) ThenAccept(name string, opts ...StepOption) *Flow {
	return f.task(name, false, opts)
}

// ThenFilter chains a gate (Worker.HandleGate): false ends the instance as gated:<name>, or, inside a
// ThenAllOf arm, ends that arm at the join.
func (f *Flow) ThenFilter(name string, opts ...StepOption) *Flow {
	o := options(opts)
	return f.add(gateNode{name: name, queue: o.queue, retry: o.retryOr()})
}

// Combine makes the task just chained a spawning step: its handler may create branches
// (StepRun.Create), and the combine name receives their results through StepRun.Results. The combine
// always runs, with no results when nothing was created. Bind it with Worker.HandleCombineStep.
func (f *Flow) Combine(name string) *Flow {
	n := len(f.steps)
	if n == 0 {
		return f.fail(fmt.Errorf("Combine(%q) must directly follow a ThenApply or ThenAccept", name))
	}
	t, ok := f.steps[n-1].(taskNode)
	if !ok || t.combine != "" || name == "" {
		return f.fail(fmt.Errorf("Combine(%q) must directly follow a ThenApply or ThenAccept", name))
	}
	t.combine = name
	f.steps[n-1] = t
	return f
}

// ThenSleep waits d on a server-side timer, without holding a worker.
func (f *Flow) ThenSleep(d time.Duration) *Flow {
	return f.add(sleepNode{name: fmt.Sprintf("sleep-%dms", d.Milliseconds()), d: d})
}

// ThenAwait waits, without holding a worker, for signal (Client.Signal), whose payload merges into the
// context. A positive timeout fails the instance when it passes -- or, with escalation, runs it
// instead and goes on after it.
func (f *Flow) ThenAwait(signal string, timeout time.Duration, escalation func(*Flow)) *Flow {
	var onTimeout []node
	if escalation != nil {
		steps, err := f.chain(escalation)
		if err != nil {
			return f.fail(err)
		}
		if len(steps) == 0 {
			return f.fail(fmt.Errorf("the escalation of %q defines no steps", signal))
		}
		onTimeout = steps
	}
	return f.add(signalNode{name: signal, timeout: timeout, escalation: onTimeout})
}

// ThenSubFlow runs the registered workflow as a child; its result merges back. node names the step.
func (f *Flow) ThenSubFlow(node, workflow string) *Flow {
	if workflow == "" {
		return f.fail(errors.New("ThenSubFlow needs a workflow"))
	}
	if node == "" {
		node = workflow
	}
	return f.add(subFlowNode{name: node, workflow: workflow})
}

// ThenAllOf runs arms in parallel, each on its own isolated copy of the context. The mandatory
// ForkStage.Combine that follows receives each arm's result under the arm's name -- its last step's,
// which must be a task or a gate -- and returns the complete post-join context.
//
//	f.ThenAllOf(
//		func(a *wiggle.Flow) { a.ThenApply("charge") },
//		func(a *wiggle.Flow) { a.ThenApply("reserve").ThenApply("label") },
//	).Combine("settle")
func (f *Flow) ThenAllOf(arms ...func(*Flow)) *ForkStage {
	if len(arms) < 2 {
		f.fail(errors.New("ThenAllOf needs at least two arms"))
		return &ForkStage{f: f}
	}
	built := make([]armNode, 0, len(arms))
	for i, arm := range arms {
		steps, err := f.chain(arm)
		if err != nil {
			f.fail(err)
			return &ForkStage{f: f}
		}
		if len(steps) == 0 {
			f.fail(fmt.Errorf("arm %d of ThenAllOf defines no steps", i))
			return &ForkStage{f: f}
		}
		var name string
		switch last := steps[len(steps)-1].(type) {
		case taskNode:
			name = last.name
		case gateNode:
			name = last.name
		default:
			f.fail(fmt.Errorf("arm %d of ThenAllOf must end in a task or a gate", i))
			return &ForkStage{f: f}
		}
		built = append(built, armNode{name: name, steps: steps})
	}
	f.pending = "ThenAllOf"
	return &ForkStage{f: f, arms: built}
}

// ForkStage is the arms of a ThenAllOf, waiting for the Combine that merges them.
type ForkStage struct {
	f    *Flow
	arms []armNode
}

// Combine names the mandatory merge after ThenAllOf (Worker.HandleCombine); the flow goes on with its
// return.
func (s *ForkStage) Combine(name string) *Flow {
	if s.arms == nil {
		return s.f
	}
	s.f.pending = ""
	if name == "" {
		return s.f.fail(errors.New("ThenAllOf needs a combine step name"))
	}
	return s.f.add(forkNode{arms: s.arms, combine: name})
}

// ThenForEach fans out one isolated branch per element of the list (or map) at itemsKey; the element
// is the item's context. Body steps are bound with Worker.HandleItem: each receives the item's value
// and its return replaces it. The mandatory ForEachStage.Combine receives every item's final value
// collected under name -- a list in item order, or a map keyed like the input -- and returns the
// complete post-join context. An empty collection skips the body and the combine.
func (f *Flow) ThenForEach(name, itemsKey string, body func(*Flow)) *ForEachStage {
	steps, err := f.chain(body)
	if err != nil {
		f.fail(err)
		return &ForEachStage{f: f}
	}
	if name == "" {
		name = itemsKey
	}
	if len(steps) == 0 {
		f.fail(fmt.Errorf("the body of ThenForEach(%q) defines no steps", name))
		return &ForEachStage{f: f}
	}
	f.pending = "ThenForEach"
	return &ForEachStage{f: f, each: &forEachNode{name: name, itemsKey: itemsKey, body: steps}}
}

// ForEachStage is a ThenForEach, waiting for the Combine that collects its items.
type ForEachStage struct {
	f    *Flow
	each *forEachNode
}

// Combine names the mandatory merge after ThenForEach (Worker.HandleCombine); the flow goes on with
// its return.
func (s *ForEachStage) Combine(name string) *Flow {
	if s.each == nil {
		return s.f
	}
	s.f.pending = ""
	if name == "" {
		return s.f.fail(errors.New("ThenForEach needs a combine step name"))
	}
	s.each.combine = name
	return s.f.add(*s.each)
}

// ThenOneOf runs the first arm whose guard holds, or the Otherwise arm, on the one context; the flow
// goes on from whichever arm ran. Without an Otherwise, a choice where no guard held goes on past it.
//
//	f.ThenOneOf(
//		wiggle.When("vip", func(a *wiggle.Flow) { a.ThenApply("concierge") }),
//		wiggle.Otherwise(func(a *wiggle.Flow) { a.ThenApply("thanks") }),
//	)
func (f *Flow) ThenOneOf(arms ...Arm[*Flow]) *Flow {
	if len(arms) == 0 || arms[0].guard == "" {
		return f.fail(errors.New("ThenOneOf needs an arm opened with When"))
	}
	cases := make([]caseNode, 0, len(arms))
	for i, arm := range arms {
		if arm.guard == "" && i != len(arms)-1 {
			return f.fail(errors.New("Otherwise must be the last arm of ThenOneOf"))
		}
		steps, err := f.chain(arm.chain)
		if err != nil {
			return f.fail(err)
		}
		cases = append(cases, caseNode{when: arm.guard, then: steps})
	}
	return f.add(chooseNode{cases: cases})
}

// RepeatWhile runs body, then again while the gate condition holds (body runs at least once). The
// condition may hold at most maxIterations times (0 = the engine default, 10,000) before the
// instance fails.
func (f *Flow) RepeatWhile(condition string, maxIterations int, body func(*Flow)) *Flow {
	steps, err := f.chain(body)
	if err != nil {
		return f.fail(err)
	}
	return f.add(loopNode{while: condition, maxIterations: maxIterations, body: steps})
}

// Err is the first mistake made building this flow, if any; Define returns it.
func (f *Flow) Err() error {
	if f.err == nil && f.pending != "" {
		return fmt.Errorf("%s needs its Combine", f.pending)
	}
	return f.err
}

func (f *Flow) task(name string, compensate bool, opts []StepOption) *Flow {
	o := options(opts)
	return f.add(taskNode{name: name, queue: o.queue, retry: o.retryOr(), compensate: compensate})
}

func (f *Flow) add(n node) *Flow {
	if f.pending != "" {
		return f.fail(fmt.Errorf("%s needs its Combine before the next step", f.pending))
	}
	f.steps = append(f.steps, n)
	return f
}

func (f *Flow) fail(err error) *Flow {
	if f.err == nil {
		f.err = err
	}
	return f
}

// chain records build on a flow of its own, sharing this flow's workflow.
func (f *Flow) chain(build func(*Flow)) ([]node, error) {
	sub := &Flow{root: f.root}
	if build != nil {
		build(sub)
	}
	if err := sub.Err(); err != nil {
		return nil, err
	}
	return sub.steps, nil
}
