package wiggle

import (

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

// taskHandler wraps a user Activity into the internal handler. The return is the step's COMPLETE
// next context: it is sent whole and REPLACES the previous value server-side (no diff, no merge).
// A nil return leaves the context untouched.
func taskHandler(fn Activity) activityHandler {
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


