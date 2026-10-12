package wiggle

import (
	"fmt"
	"strings"
)

// The engine stages a join's results under reserved keys so they cannot collide with the
// workflow's own data: a fork arm under armScratchPrefix+arm, a forEach's collected results under
// the combine's collectKey, and the step that produced each result under stepsScratchPrefix+collectKey.
const (
	armScratchPrefix     = "__arm__"
	forEachScratchPrefix = "__forEach__"
	stepsScratchPrefix   = "__steps__"
)

// unstage rewrites a combine activation's context into the shape a combine handler takes: each
// fork arm's result under the arm's name, or a forEach's results under the forEach's name. It
// fails when such a name is also a key of the workflow's own context, which the handler could not
// tell apart.
func unstage(task *Task) (Context, error) {
	if len(task.ArmNames) == 0 && task.CollectKey == "" {
		return task.Context, nil
	}
	out := Context{}
	for k, v := range task.Context {
		out[k] = v
	}
	place := func(scratch, name string) error {
		v, staged := out[scratch]
		if !staged {
			return nil
		}
		delete(out, scratch)
		if _, taken := out[name]; taken {
			return fmt.Errorf("combine %q: %q is both a result to combine and a key of the context; "+
				"rename the branch or the key", task.StepName, name)
		}
		out[name] = v
		return nil
	}
	for _, arm := range task.ArmNames {
		if err := place(armScratchPrefix+arm, arm); err != nil {
			return nil, err
		}
	}
	if ck := task.CollectKey; ck != "" {
		delete(out, stepsScratchPrefix+ck)
		if err := place(ck, strings.TrimPrefix(ck, forEachScratchPrefix)); err != nil {
			return nil, err
		}
	}
	return out, nil
}
