package wiggle

import (
	"reflect"
	"strings"
	"testing"
)

func TestUnstageForkArms(t *testing.T) {
	got, err := unstage(&Task{StepName: "join", ArmNames: []string{"left", "right"}, Context: Context{
		"n": 1.0, "__arm__left": Context{"side": "L"}, "__arm__right": Context{"side": "R"}}})
	if err != nil {
		t.Fatal(err)
	}
	want := Context{"n": 1.0, "left": Context{"side": "L"}, "right": Context{"side": "R"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestUnstageForEachResults(t *testing.T) {
	got, err := unstage(&Task{StepName: "sum", CollectKey: "__forEach__doubled", Context: Context{
		"xs": []any{1.0}, "__forEach__doubled": []any{2.0}, "__steps____forEach__doubled": []any{"double"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := (Context{"xs": []any{1.0}, "doubled": []any{2.0}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v", got)
	}
}

func TestUnstageRefusesACollidingName(t *testing.T) {
	_, err := unstage(&Task{StepName: "join", ArmNames: []string{"payment"}, Context: Context{
		"payment": "mine", "__arm__payment": Context{}}})
	if err == nil || !strings.Contains(err.Error(), `"payment" is both`) {
		t.Fatalf("err = %v", err)
	}
}

func TestUnstageLeavesOtherStepsAlone(t *testing.T) {
	ctx := Context{"__arm__x": 1.0}
	got, _ := unstage(&Task{Context: ctx})
	if !reflect.DeepEqual(got, ctx) {
		t.Fatalf("got %v", got)
	}
}
