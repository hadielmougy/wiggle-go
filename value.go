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
