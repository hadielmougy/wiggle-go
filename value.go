package wiggle

import (
	"reflect"

	"google.golang.org/protobuf/types/known/structpb"
)

// toValue converts a Go JSON-ish value (nil, bool, numbers, string, []any, map[string]any) to a
// protobuf Value. Integers become float64 numbers, matching JSON.
func toValue(v any) (*structpb.Value, error) {
	if v == nil {
		return structpb.NewNullValue(), nil
	}
	return structpb.NewValue(v)
}

// fromValue converts a protobuf Value back to Go. Objects become map[string]any, arrays []any,
// numbers float64.
func fromValue(v *structpb.Value) any {
	if v == nil {
		return nil
	}
	return v.AsInterface()
}

func toStruct(m map[string]any) (*structpb.Struct, error) {
	return structpb.NewStruct(m)
}

func fromStruct(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

// asMap coerces a fromValue result to a context map (nil-safe).
func asMap(v any) Context {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return Context{}
}

// asList coerces a value to a JSON array (nil-safe).
func asList(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}
	return nil
}

// cloneContext makes a shallow copy of the top-level keys, so a task handler that mutates the
// context in place can still be diffed against the value it was given.
func cloneContext(c Context) Context {
	out := make(Context, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}

// taskHandler wraps a user Activity into the internal handler: snapshot the input, run the step, and
// return only the changed top-level keys (works whether the handler mutates in place or returns a
// new map).
func taskHandler(fn Activity) activityHandler {
	return func(ctx Context) (any, error) {
		before := cloneContext(ctx)
		out, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		return shallowDiff(before, out), nil
	}
}

// combineHandler wraps a combine step's Activity: unlike taskHandler there is NO diff -- the
// return is the COMPLETE post-join context and is sent verbatim, because the engine REPLACES the
// context with it (keys the handler omits do not survive the join). A nil return leaves the
// context untouched.
func combineHandler(fn Activity) activityHandler {
	return func(ctx Context) (any, error) {
		out, err := fn(ctx)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		return out, nil
	}
}

// shallowDiff returns only the keys of after that differ from before; a key present in before but
// absent from after is set to nil (a dropped key becomes null), matching the engine's merge. This is
// what a task handler sends back so parallel branches touching different fields merge cleanly.
func shallowDiff(before, after Context) Context {
	diff := Context{}
	for k, av := range after {
		if bv, ok := before[k]; !ok || !reflect.DeepEqual(bv, av) {
			diff[k] = av
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			diff[k] = nil
		}
	}
	return diff
}
