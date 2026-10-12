package criteria

import (
	"strings"
	"testing"
	"time"
)

func TestValidCriteria(t *testing.T) {
	_, err := New("pay", And(
		Equal(Workflow, "orders"),
		Not(In(Attempt, 3, 4)),
		Or(Between(Attr("amount"), Decimal("1.50"), 10.25), Equal(Attr("vip"), true)),
		GreaterThan(InstanceCreatedAt, time.UnixMilli(5)),
		IsNull(CorrelationID),
		Like(Step, "ship%")))
	if err == nil || !strings.Contains(err.Error(), "STEP supports") {
		t.Fatalf("STEP with like must be refused, got %v", err)
	}
	c, err := New("pay", And(Equal(Workflow, "orders"), Not(In(Attempt, 3, 4)),
		Or(Between(Attr("amount"), Decimal("1.50"), 10.25), Equal(Attr("vip"), true)),
		GreaterThan(InstanceCreatedAt, time.UnixMilli(5)), IsNull(CorrelationID)))
	if err != nil {
		t.Fatal(err)
	}
	terms := c.Where.(AllOf).Terms
	if got := terms[1].(Negated).Term.(Comparison).Args; got[0] != int64(3) || got[1] != int64(4) {
		t.Fatalf("integers = %#v", got)
	}
	if got := terms[2].(AnyOf).Terms[0].(Comparison).Args; got[0] != Decimal("1.50") || got[1] != Decimal("10.25") {
		t.Fatalf("decimals = %#v", got)
	}
	if got := terms[3].(Comparison).Args[0]; got != int64(5) {
		t.Fatalf("a time is its epoch millis, got %#v", got)
	}
}

func TestInvalidCriteria(t *testing.T) {
	deep := IsNull(Step)
	for i := 0; i < 8; i++ {
		deep = Not(deep)
	}
	long := strings.Repeat("x", MaxText+1)
	for name, p := range map[string]Predicate{
		"text field, integer literal": Equal(Workflow, 3),
		"integer field, text literal": LessThan(Attempt, "3"),
		"like on an integer":          Like(Attempt, "1%"),
		"arity":                       Comparison{Field: Version, Op: "BETWEEN", Args: []any{int64(1)}},
		"mixed attribute types":       In(Attr("x"), "a", 1),
		"ordering a boolean":          LessThan(Attr("flag"), true),
		"past the decimal scale":      Equal(Attr("x"), Decimal("1.12345678901")),
		"text too long":               Equal(Workflow, long),
		"bad attribute name":          Equal(Attr("Bad"), "x"),
		"deeper than the limit":       deep,
		"an empty and":                And(),
		"an unsupported literal":      Equal(Attr("x"), []string{"a"}),
	} {
		if _, err := New("q", p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(" ", nil); err == nil {
		t.Error("a blank queue was accepted")
	}
}
