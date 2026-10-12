package wiggle

import (
	"errors"
	"fmt"
	"time"
)

// CreatedBranch is a branch a running step created with StepRun.Create: it runs its own chain of steps on
// an isolated context once the step's report is committed, and its result reaches the combine
// after the step. Steps are named as in the topology -- by step name, bound on the worker by that
// name -- and nothing here runs them.
//
//	for _, item := range order["items"].([]any) {
//		s.Create(item).ThenApply("reserve").ThenApply("ship", OnQueue("shipping"))
//	}
//
// A step without its own queue or retry policy takes the creating step's. A step on a queue no
// graph mentions runs only on a worker that polls that queue. A chain built wrongly fails the step
// that built it, without a retry.
type CreatedBranch struct {
	input any
	key   string
	keyed bool
	steps []branchStep
	err   error
}

type branchStep struct {
	name        string
	kind        string
	compensable bool
	queue       string
	retry       *Retry
	sleepMillis int64
	combine     string
	workflow    string
	escalation  []branchStep
	arms        [][]branchStep
	body        []branchStep
	loopBudget  int32
	cases       []branchCase
}

type branchCase struct {
	guard string
	steps []branchStep
}

// BranchOption tunes one step of a branch.
type BranchOption func(*branchStep)

// OnQueue runs the step on queue instead of the creating step's.
func OnQueue(queue string) BranchOption { return func(s *branchStep) { s.queue = queue } }

// WithRetry gives the step its own retry policy instead of the creating step's.
func WithRetry(r Retry) BranchOption { return func(s *branchStep) { s.retry = &r } }

// Compensable declares the step's undo, which runs in the reverse pass if the instance later fails.
func Compensable() BranchOption { return func(s *branchStep) { s.compensable = true } }

// ThenApply chains a task: its handler's return becomes the branch's context.
func (b *CreatedBranch) ThenApply(name string, opts ...BranchOption) *CreatedBranch {
	return b.add(name, "TASK", opts)
}

// ThenAccept chains a side effect: the branch's context is left unchanged.
func (b *CreatedBranch) ThenAccept(name string, opts ...BranchOption) *CreatedBranch {
	return b.add(name, "TASK", opts)
}

// ThenFilter chains a gate: false ends this branch at the join, as a false gate ends a fork arm.
func (b *CreatedBranch) ThenFilter(name string, opts ...BranchOption) *CreatedBranch {
	return b.add(name, "PREDICATE", opts)
}

// Combine makes the task just chained create branches of its own: its handler calls Create, and the
// combine name receives their results. The branch goes on with the combine's return.
func (b *CreatedBranch) Combine(name string) *CreatedBranch {
	n := len(b.steps)
	if n == 0 || b.steps[n-1].kind != "TASK" || b.steps[n-1].combine != "" || name == "" {
		return b.fail(fmt.Errorf("Combine(%q) must directly follow the step that creates the branches "+
			"it combines: a ThenApply or ThenAccept", name))
	}
	b.steps[n-1].combine = name
	return b
}

// ThenSleep waits d on a server-side timer, without holding a worker.
func (b *CreatedBranch) ThenSleep(d time.Duration) *CreatedBranch {
	if d < 0 {
		return b.fail(fmt.Errorf("a sleep cannot be negative: %s", d))
	}
	b.steps = append(b.steps, branchStep{kind: "SLEEP", sleepMillis: d.Milliseconds()})
	return b
}

// ThenAwait waits, without holding a worker, for signal. A positive timeout fails the instance when
// it passes -- or, with escalation, runs it instead and goes on after it.
func (b *CreatedBranch) ThenAwait(signal string, timeout time.Duration, escalation func(*CreatedBranch)) *CreatedBranch {
	if signal == "" {
		return b.fail(errors.New("ThenAwait needs a signal name"))
	}
	if timeout < 0 || (escalation != nil && timeout <= 0) {
		return b.fail(fmt.Errorf("a wait's timeout must be positive: %s", timeout))
	}
	var onTimeout []branchStep
	if escalation != nil {
		var err error
		if onTimeout, err = chain(escalation, fmt.Sprintf("the escalation of %q", signal)); err != nil {
			return b.fail(err)
		}
	}
	b.steps = append(b.steps, branchStep{name: signal, kind: "SIGNAL", sleepMillis: timeout.Milliseconds(),
		escalation: onTimeout})
	return b
}

// ThenSubFlow runs the registered workflow as a child on this branch's context; its final context
// becomes the branch's. node names the step.
func (b *CreatedBranch) ThenSubFlow(node, workflow string) *CreatedBranch {
	if node == "" || workflow == "" {
		return b.fail(errors.New("ThenSubFlow needs a node name and a workflow"))
	}
	b.steps = append(b.steps, branchStep{name: node, kind: "SUB_WORKFLOW", workflow: workflow})
	return b
}

// ThenAllOf runs arms in parallel, each on its own copy of this branch's context; the combine
// receives each arm's result under the arm's name -- its last step's, which must be a task or a gate.
func (b *CreatedBranch) ThenAllOf(combine string, arms ...func(*CreatedBranch)) *CreatedBranch {
	if len(arms) < 2 {
		return b.fail(errors.New("ThenAllOf needs at least two arms"))
	}
	if combine == "" {
		return b.fail(errors.New("ThenAllOf needs a combine step name"))
	}
	chains := make([][]branchStep, 0, len(arms))
	seen := map[string]bool{}
	for i, arm := range arms {
		steps, err := chain(arm, fmt.Sprintf("arm %d", i))
		if err != nil {
			return b.fail(err)
		}
		last := steps[len(steps)-1]
		if last.kind != "TASK" && last.kind != "PREDICATE" {
			return b.fail(fmt.Errorf("arm %d of ThenAllOf must end in a task or a gate", i))
		}
		if seen[last.name] {
			return b.fail(fmt.Errorf("two arms of ThenAllOf end in %q; an arm is named after its last step", last.name))
		}
		seen[last.name] = true
		chains = append(chains, steps)
	}
	b.steps = append(b.steps, branchStep{kind: "FORK", combine: combine, arms: chains})
	return b
}

// RepeatWhile runs body, then again while the gate condition holds; past maxIterations (0 = the
// engine's default) the instance fails.
func (b *CreatedBranch) RepeatWhile(condition string, maxIterations int, body func(*CreatedBranch)) *CreatedBranch {
	if condition == "" {
		return b.fail(errors.New("RepeatWhile needs a condition step name"))
	}
	if maxIterations < 0 {
		return b.fail(fmt.Errorf("maxIterations must be positive: %d", maxIterations))
	}
	steps, err := chain(body, fmt.Sprintf("the body of RepeatWhile(%s)", condition))
	if err != nil {
		return b.fail(err)
	}
	b.steps = append(b.steps, branchStep{name: condition, kind: "PREDICATE", body: steps,
		loopBudget: int32(maxIterations)})
	return b
}

// Arm is one arm of ThenOneOf: When(guard, ...) or, last, Otherwise(...).
type Arm struct {
	guard string
	chain func(*CreatedBranch)
}

// When opens an arm of ThenOneOf, run when the gate guard holds.
func When(guard string, chain func(*CreatedBranch)) Arm { return Arm{guard: guard, chain: chain} }

// Otherwise is the last arm of ThenOneOf, run when no guard held.
func Otherwise(chain func(*CreatedBranch)) Arm { return Arm{chain: chain} }

// ThenOneOf runs the first arm whose guard holds, or the Otherwise arm. Without one, a choice where
// no guard held goes on past it.
func (b *CreatedBranch) ThenOneOf(arms ...Arm) *CreatedBranch {
	if len(arms) == 0 || arms[0].guard == "" {
		return b.fail(errors.New("ThenOneOf needs an arm opened with When"))
	}
	cases := make([]branchCase, 0, len(arms))
	for i, arm := range arms {
		if arm.guard == "" && i != len(arms)-1 {
			return b.fail(errors.New("Otherwise must be the last arm of ThenOneOf"))
		}
		sub := &CreatedBranch{}
		if arm.chain != nil {
			arm.chain(sub)
		}
		if sub.err != nil {
			return b.fail(sub.err)
		}
		cases = append(cases, branchCase{guard: arm.guard, steps: sub.steps})
	}
	b.steps = append(b.steps, branchStep{kind: "PREDICATE", cases: cases})
	return b
}

// Err is the first mistake made building this branch, if any.
func (b *CreatedBranch) Err() error { return b.err }

func (b *CreatedBranch) add(name, kind string, opts []BranchOption) *CreatedBranch {
	if name == "" {
		return b.fail(errors.New("a branch step needs a name"))
	}
	s := branchStep{name: name, kind: kind}
	for _, o := range opts {
		o(&s)
	}
	if s.compensable && kind != "TASK" {
		return b.fail(fmt.Errorf("only a task can be compensable, not gate %q", name))
	}
	b.steps = append(b.steps, s)
	return b
}

func (b *CreatedBranch) fail(err error) *CreatedBranch {
	if b.err == nil {
		b.err = err
	}
	return b
}

func chain(build func(*CreatedBranch), what string) ([]branchStep, error) {
	sub := &CreatedBranch{}
	if build != nil {
		build(sub)
	}
	if sub.err != nil {
		return nil, sub.err
	}
	if len(sub.steps) == 0 {
		return nil, fmt.Errorf("%s has no steps", what)
	}
	return sub.steps, nil
}
