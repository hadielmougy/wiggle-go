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
	spawnScratchPrefix   = "__spawn__"
	stepsScratchPrefix   = "__steps__"
)

// unstage rewrites a combine activation's context into the shape a combine handler takes, and
// returns it with the base (the context without the staged results) and the results, which
// StepRun.Results exposes. A fork's arms appear under their names and a forEach's results under its
// name; created branches' results appear only through Results, since they have no name of their
// own. It fails when an arm or forEach name is also a key of the workflow's own context, which the
// handler could not tell apart. ok is false for a step that is not a combine.
func unstage(task *Task) (ctx, base Context, results any, ok bool, err error) {
	if len(task.ArmNames) == 0 && task.CollectKey == "" {
		return task.Context, nil, nil, false, nil
	}
	base = Context{}
	for k, v := range task.Context {
		base[k] = v
	}
	placed := map[string]any{}
	place := func(scratch, name string) error {
		v, staged := base[scratch]
		if !staged {
			return nil
		}
		delete(base, scratch)
		if _, taken := base[name]; taken {
			return fmt.Errorf("combine %q: %q is both a result to combine and a key of the context; "+
				"rename the branch or the key", task.StepName, name)
		}
		placed[name] = v
		return nil
	}
	withPlaced := func() Context {
		out := Context{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range placed {
			out[k] = v
		}
		return out
	}
	if len(task.ArmNames) > 0 {
		for _, arm := range task.ArmNames {
			if err := place(armScratchPrefix+arm, arm); err != nil {
				return nil, nil, nil, false, err
			}
		}
		return withPlaced(), base, placed, true, nil
	}
	ck := task.CollectKey
	delete(base, stepsScratchPrefix+ck)
	if strings.HasPrefix(ck, spawnScratchPrefix) {
		results = base[ck]
		delete(base, ck)
		if results == nil {
			results = []any{}
		}
		return base, base, results, true, nil
	}
	name := strings.TrimPrefix(ck, forEachScratchPrefix)
	if err := place(ck, name); err != nil {
		return nil, nil, nil, false, err
	}
	return withPlaced(), base, placed[name], true, nil
}

// spawns reports whether node nodeID of graph may create branches: a task followed by a dynamic
// join and the combine collecting its branches, or that combine.
func spawns(graph map[string]any, nodeID string) bool {
	nodes := map[string]map[string]any{}
	for _, n := range asList(graph["nodes"]) {
		if node, ok := n.(map[string]any); ok {
			id, _ := node["id"].(string)
			nodes[id] = node
		}
	}
	node := nodes[nodeID]
	if node == nil {
		return false
	}
	if ck, _ := node["collectKey"].(string); strings.HasPrefix(ck, spawnScratchPrefix) {
		return true
	}
	if k, _ := node["kind"].(string); k != "TASK" || isCombine(node) {
		return false
	}
	next, _ := node["next"].(string)
	join := nodes[next]
	if join == nil {
		return false
	}
	if k, _ := join["kind"].(string); k != "JOIN" {
		return false
	}
	if expected, _ := join["expected"].(float64); expected != 0 {
		return false
	}
	after, _ := join["next"].(string)
	combine := nodes[after]
	name, _ := node["name"].(string)
	ck, _ := combine["collectKey"].(string)
	return combine != nil && ck == spawnScratchPrefix+name
}
