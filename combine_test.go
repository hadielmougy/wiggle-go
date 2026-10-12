package wiggle

import (
	"reflect"
	"strings"
	"testing"
)

func TestUnstageForkArms(t *testing.T) {
	got, base, results, ok, err := unstage(&Task{StepName: "join", ArmNames: []string{"left", "right"}, Context: Context{
		"n": 1.0, "__arm__left": Context{"side": "L"}, "__arm__right": Context{"side": "R"}}})
	if err != nil || !ok {
		t.Fatal(err)
	}
	want := Context{"n": 1.0, "left": Context{"side": "L"}, "right": Context{"side": "R"}}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(base, Context{"n": 1.0}) {
		t.Fatalf("got %v base %v", got, base)
	}
	if !reflect.DeepEqual(results, map[string]any{"left": Context{"side": "L"}, "right": Context{"side": "R"}}) {
		t.Fatalf("results %v", results)
	}
}

func TestUnstageForEachResults(t *testing.T) {
	got, _, results, _, err := unstage(&Task{StepName: "sum", CollectKey: "__forEach__doubled", Context: Context{
		"xs": []any{1.0}, "__forEach__doubled": []any{2.0}, "__steps____forEach__doubled": []any{"double"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := (Context{"xs": []any{1.0}, "doubled": []any{2.0}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
	if !reflect.DeepEqual(results, []any{2.0}) {
		t.Fatalf("results %v", results)
	}
}

func TestUnstageHandsCreatedBranchesResultsOverOnlyAsResults(t *testing.T) {
	got, base, results, ok, err := unstage(&Task{StepName: "packed", CollectKey: "__spawn__lines", Context: Context{
		"lines": []any{1.0}, "__spawn__lines": []any{Context{"x": 1.0}}, "__steps____spawn__lines": []any{"pick"}}})
	if err != nil || !ok {
		t.Fatal(err)
	}
	want := Context{"lines": []any{1.0}}
	if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(base, want) {
		t.Fatalf("got %v base %v", got, base)
	}
	if !reflect.DeepEqual(results, []any{Context{"x": 1.0}}) {
		t.Fatalf("results %v", results)
	}
}

func TestUnstageRefusesACollidingName(t *testing.T) {
	_, _, _, _, err := unstage(&Task{StepName: "join", ArmNames: []string{"payment"}, Context: Context{
		"payment": "mine", "__arm__payment": Context{}}})
	if err == nil || !strings.Contains(err.Error(), `"payment" is both`) {
		t.Fatalf("err = %v", err)
	}
}

func TestUnstageLeavesOtherStepsAlone(t *testing.T) {
	ctx := Context{"__arm__x": 1.0}
	got, _, _, ok, _ := unstage(&Task{Context: ctx})
	if !reflect.DeepEqual(got, ctx) || ok {
		t.Fatalf("got %v", got)
	}
}
