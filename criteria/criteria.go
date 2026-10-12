// Package criteria builds queue criteria: a predicate over a ready task's properties, so a worker
// claims only the tasks of a queue it should run -- the JPA criteria shape, evaluated by the server
// when it claims.
//
//	payments, err := criteria.New("payments", criteria.And(
//		criteria.Equal(criteria.Workflow, "orders"),
//		criteria.LessThan(criteria.Attempt, 3)))
//
// A comparison on an absent value is false (and Not of it true); only IsNull and IsNotNull test
// for absence. New checks a predicate against the server's limits, so a malformed one fails where
// it is built.
package criteria

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Limits the server enforces on one queue's criteria.
const (
	MaxDepth         = 8
	MaxNodes         = 64
	MaxIn            = 256
	MaxText          = 256
	MaxScale         = 10
	MaxIntegerDigits = 28
)

// Field is a task property a comparison tests. Attribute names a declared attribute (field
// ATTRIBUTE) and is empty for every other field.
type Field struct {
	Name      string
	Type      string // TEXT | INTEGER | ANY
	Attribute string
}

// The task properties criteria can test.
var (
	Workflow          = Field{Name: "WORKFLOW", Type: "TEXT"}
	Version           = Field{Name: "VERSION", Type: "INTEGER"}
	Step              = Field{Name: "STEP", Type: "TEXT"} // Equal, NotEqual and In only
	NodeID            = Field{Name: "NODE_ID", Type: "TEXT"}
	Kind              = Field{Name: "KIND", Type: "TEXT"} // TASK or PREDICATE
	Attempt           = Field{Name: "ATTEMPT", Type: "INTEGER"}
	CorrelationID     = Field{Name: "CORRELATION_ID", Type: "TEXT"}
	InstanceCreatedAt = Field{Name: "INSTANCE_CREATED_AT", Type: "INTEGER"} // epoch millis, or a time.Time
)

var attributeName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// Attr is a value the workflow declares (wiggle.Graph{Attributes: ...}).
func Attr(name string) Field { return Field{Name: "ATTRIBUTE", Type: "ANY", Attribute: name} }

// Decimal is an exact decimal literal for an attribute comparison, as its decimal text ("12.50").
type Decimal string

// Predicate is a comparison, or comparisons joined by And, Or and Not.
type Predicate interface{ isPredicate() }

// Comparison tests one field. Args hold string, int64, Decimal or bool literals.
type Comparison struct {
	Field Field
	Op    string
	Args  []any
}

// AllOf holds when every term does.
type AllOf struct{ Terms []Predicate }

// AnyOf holds when some term does.
type AnyOf struct{ Terms []Predicate }

// Negated holds when its term does not.
type Negated struct{ Term Predicate }

func (Comparison) isPredicate() {}
func (AllOf) isPredicate()      {}
func (AnyOf) isPredicate()      {}
func (Negated) isPredicate()    {}

func cmp(f Field, op string, values ...any) Predicate {
	args := make([]any, len(values))
	for i, v := range values {
		args[i] = literal(v)
	}
	return Comparison{Field: f, Op: op, Args: args}
}

func literal(v any) any {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case int16:
		return int64(x)
	case int8:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case time.Time:
		return x.UnixMilli()
	case float64:
		return Decimal(strconv.FormatFloat(x, 'f', -1, 64))
	case float32:
		return Decimal(strconv.FormatFloat(float64(x), 'f', -1, 32))
	}
	return v
}

func Equal(f Field, v any) Predicate              { return cmp(f, "EQUAL", v) }
func NotEqual(f Field, v any) Predicate           { return cmp(f, "NOT_EQUAL", v) }
func In(f Field, vs ...any) Predicate             { return cmp(f, "IN", vs...) }
func LessThan(f Field, v any) Predicate           { return cmp(f, "LESS_THAN", v) }
func LessThanOrEqual(f Field, v any) Predicate    { return cmp(f, "LESS_THAN_OR_EQUAL", v) }
func GreaterThan(f Field, v any) Predicate        { return cmp(f, "GREATER_THAN", v) }
func GreaterThanOrEqual(f Field, v any) Predicate { return cmp(f, "GREATER_THAN_OR_EQUAL", v) }
func Between(f Field, low, high any) Predicate    { return cmp(f, "BETWEEN", low, high) }

// Like matches with % and _ wildcards and \ escapes; case-sensitive.
func Like(f Field, pattern string) Predicate { return cmp(f, "LIKE", pattern) }
func IsNull(f Field) Predicate               { return cmp(f, "IS_NULL") }
func IsNotNull(f Field) Predicate            { return cmp(f, "IS_NOT_NULL") }
func And(terms ...Predicate) Predicate       { return AllOf{Terms: terms} }
func Or(terms ...Predicate) Predicate        { return AnyOf{Terms: terms} }
func Not(term Predicate) Predicate           { return Negated{Term: term} }

// QueueCriteria is what a poll may claim from Queue: its ready tasks for which Where holds (all of
// them when Where is nil). A worker serving several queues holds one per queue.
type QueueCriteria struct {
	Queue string
	Where Predicate
}

// New validates where against the server's rules and returns the criteria for queue.
func New(queue string, where Predicate) (QueueCriteria, error) {
	if strings.TrimSpace(queue) == "" {
		return QueueCriteria{}, fmt.Errorf("criteria need a queue")
	}
	if where != nil {
		nodes := 0
		if err := validate(where, 1, &nodes); err != nil {
			return QueueCriteria{}, fmt.Errorf("criteria on queue %q: %w", queue, err)
		}
	}
	return QueueCriteria{Queue: queue, Where: where}, nil
}

// MustNew is New that panics on an invalid predicate.
func MustNew(queue string, where Predicate) QueueCriteria {
	c, err := New(queue, where)
	if err != nil {
		panic(err)
	}
	return c
}

var arity = map[string][2]int{"EQUAL": {1, 1}, "NOT_EQUAL": {1, 1}, "IN": {1, MaxIn}, "LESS_THAN": {1, 1},
	"LESS_THAN_OR_EQUAL": {1, 1}, "GREATER_THAN": {1, 1}, "GREATER_THAN_OR_EQUAL": {1, 1},
	"BETWEEN": {2, 2}, "LIKE": {1, 1}, "IS_NULL": {0, 0}, "IS_NOT_NULL": {0, 0}}

var ordering = map[string]bool{"LESS_THAN": true, "LESS_THAN_OR_EQUAL": true, "GREATER_THAN": true,
	"GREATER_THAN_OR_EQUAL": true, "BETWEEN": true}

func validate(p Predicate, depth int, nodes *int) error {
	if depth > MaxDepth {
		return fmt.Errorf("criteria nest deeper than %d", MaxDepth)
	}
	*nodes++
	if *nodes > MaxNodes {
		return fmt.Errorf("criteria have more than %d nodes", MaxNodes)
	}
	switch x := p.(type) {
	case Comparison:
		return validateComparison(x)
	case AllOf:
		return validateTerms("And", x.Terms, depth, nodes)
	case AnyOf:
		return validateTerms("Or", x.Terms, depth, nodes)
	case Negated:
		if x.Term == nil {
			return fmt.Errorf("Not needs a term")
		}
		return validate(x.Term, depth+1, nodes)
	}
	return fmt.Errorf("not a criteria predicate: %T", p)
}

func validateTerms(what string, terms []Predicate, depth int, nodes *int) error {
	if len(terms) == 0 {
		return fmt.Errorf("%s needs at least one term", what)
	}
	for _, t := range terms {
		if err := validate(t, depth+1, nodes); err != nil {
			return err
		}
	}
	return nil
}

func label(f Field) string {
	if f.Attribute != "" {
		return fmt.Sprintf("attribute %q", f.Attribute)
	}
	return f.Name
}

func validateComparison(c Comparison) error {
	f := c.Field
	if f.Name == "ATTRIBUTE" && !attributeName.MatchString(f.Attribute) {
		return fmt.Errorf("attribute %q is not a valid attribute name", f.Attribute)
	}
	bounds, known := arity[c.Op]
	if !known {
		return fmt.Errorf("unknown operator %q", c.Op)
	}
	if n := len(c.Args); n < bounds[0] || n > bounds[1] {
		return fmt.Errorf("%s %s takes %d..%d argument(s), got %d", label(f), c.Op, bounds[0], bounds[1], n)
	}
	if f.Name == "STEP" && c.Op != "EQUAL" && c.Op != "NOT_EQUAL" && c.Op != "IN" {
		return fmt.Errorf("STEP supports Equal, NotEqual and In only, not %s", c.Op)
	}
	if c.Op == "LIKE" && f.Type == "INTEGER" {
		return fmt.Errorf("Like needs a text field, not %s", f.Name)
	}
	kinds := map[string]bool{}
	for _, a := range c.Args {
		var kind string
		switch x := a.(type) {
		case string:
			kind = "text"
			if len(x) > MaxText {
				return fmt.Errorf("%s: a text argument is longer than %d characters", label(f), MaxText)
			}
		case int64:
			kind = "number"
			if f.Type == "INTEGER" {
				kind = "integer"
			}
		case Decimal:
			if err := checkDecimal(string(x)); err != nil {
				return fmt.Errorf("%s: %w", label(f), err)
			}
			kind = "number"
		case bool:
			kind = "flag"
		default:
			return fmt.Errorf("%s takes text, an integer, a Decimal or a bool, got %T", label(f), a)
		}
		if f.Type == "TEXT" && kind != "text" {
			return fmt.Errorf("%s takes text, got %T", f.Name, a)
		}
		if f.Type == "INTEGER" && kind != "integer" {
			return fmt.Errorf("%s takes an integer, got %T", f.Name, a)
		}
		kinds[kind] = true
	}
	if f.Type == "ANY" {
		if len(kinds) > 1 {
			return fmt.Errorf("%s: arguments of mixed types", label(f))
		}
		if c.Op == "LIKE" && !kinds["text"] {
			return fmt.Errorf("%s: Like needs text", label(f))
		}
		if kinds["flag"] && (ordering[c.Op] || c.Op == "LIKE") {
			return fmt.Errorf("%s: a boolean supports Equal, NotEqual and In only", label(f))
		}
	}
	return nil
}

var decimalText = regexp.MustCompile(`^-?(\d+)(?:\.(\d+))?$`)

func checkDecimal(s string) error {
	m := decimalText.FindStringSubmatch(s)
	if m == nil {
		if f, err := strconv.ParseFloat(s, 64); err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return fmt.Errorf("%q is not a decimal", s)
		}
		return fmt.Errorf("%q is not plain decimal text", s)
	}
	integer := strings.TrimLeft(m[1], "0")
	fraction := strings.TrimRight(m[2], "0")
	if len(fraction) > MaxScale || len(integer) > MaxIntegerDigits {
		return fmt.Errorf("%s has more than %d fraction or %d integer digits", s, MaxScale, MaxIntegerDigits)
	}
	return nil
}
