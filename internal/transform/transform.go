// Package transform changes point values with expressions (expr-lang) after a poll and
// before the points are routed to the outputs.
package transform

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/internal/datasource"
)

// ErrInvalidExpression is returned by New for an expression that does not compile or
// references an unknown point or lookup.
var ErrInvalidExpression = errors.New("transform: invalid expression")

// Settings configures the Transformer of one device.
type Settings struct {
	Device string
	// Points are all point names of the device; expressions may only reference these.
	Points []string
	// Exprs maps a point name to its expression; points without one are not changed.
	Exprs map[string]string
	// Lookups are named tables (integer code → text) shared by all expressions.
	Lookups map[string]map[int]string
}

// env is what an expression sees. Lookup values are `any`, so a missing code is nil
// and `?? "unknown"` works (a typed map would return "").
type env struct {
	Value   any                    `expr:"value"`
	Points  map[string]any         `expr:"points"`
	Lookups map[string]map[int]any `expr:"lookups"`
}

// Transformer evaluates the expressions of one device. It is used by that device's
// router goroutine only and is not safe for concurrent use.
type Transformer struct {
	programs map[string]*vm.Program
	lookups  map[string]map[int]any
	logger   zerolog.Logger

	failedPoints map[string]bool // points whose last evaluation failed
}

// New compiles every expression of s. An expression that does not compile or names a
// point or lookup that does not exist is an error, so mistakes are found at startup.
func New(s Settings, log zerolog.Logger) (*Transformer, error) {
	t := &Transformer{
		programs: make(map[string]*vm.Program, len(s.Exprs)),
		lookups:  make(map[string]map[int]any, len(s.Lookups)),
		logger: log.With().Str("component", "transform").
			Str("device", s.Device).Logger(),
		failedPoints: make(map[string]bool),
	}
	for name, table := range s.Lookups {
		t.lookups[name] = make(map[int]any, len(table))
		for code, text := range table {
			t.lookups[name][code] = text
		}
	}
	for point, code := range s.Exprs {
		program, err := compile(code, s.Points, s.Lookups)
		if err != nil {
			return nil, fmt.Errorf("%w: point %s: %w", ErrInvalidExpression, point, err)
		}
		t.programs[point] = program
	}
	return t, nil
}

// compile compiles code against env and checks the referenced points and lookups.
func compile(code string, points []string, lookups map[string]map[int]string) (*vm.Program, error) {
	program, err := expr.Compile(code, expr.Env(env{}))
	if err != nil {
		return nil, err
	}
	check := &referenceCheck{points: points, lookups: lookups}
	node := program.Node()
	ast.Walk(&node, check)
	if check.err != nil {
		return nil, check.err
	}
	return program, nil
}

// referenceCheck finds `points.x` and `lookups.x` with an x that does not exist; expr
// cannot check map keys at compile time.
type referenceCheck struct {
	points  []string
	lookups map[string]map[int]string
	err     error
}

// Visit implements ast.Visitor.
func (c *referenceCheck) Visit(node *ast.Node) {
	if c.err != nil {
		return
	}
	switch variable, key := memberKey(*node); variable {
	case "points":
		if !slices.Contains(c.points, key) {
			c.err = fmt.Errorf("unknown point %q", key)
		}
	case "lookups":
		if _, found := c.lookups[key]; !found {
			c.err = fmt.Errorf("unknown lookup %q", key)
		}
	}
}

// memberKey returns the variable and the key of a constant member access on a variable
// (`points.x`, `points["x"]`); otherwise both are empty.
func memberKey(node ast.Node) (variable, key string) {
	member, ok := node.(*ast.MemberNode)
	if !ok {
		return "", ""
	}
	ident, ok := member.Node.(*ast.IdentifierNode)
	if !ok {
		return "", ""
	}
	property, ok := member.Property.(*ast.StringNode)
	if !ok {
		return "", ""
	}
	return ident.Value, property.Value
}

// Apply returns the points of one poll with their expressions applied. Every
// expression sees the values of the poll before any expression ran. A point whose
// expression fails or returns nil is left out.
func (t *Transformer) Apply(points []datasource.DataPoint) []datasource.DataPoint {
	if len(t.programs) == 0 {
		return points
	}
	values := make(map[string]any, len(points))
	for _, dp := range points {
		values[dp.PointName] = dp.Value
	}

	out := make([]datasource.DataPoint, 0, len(points))
	for _, dp := range points {
		program, found := t.programs[dp.PointName]
		if !found {
			out = append(out, dp)
			continue
		}
		value, err := t.eval(program, dp.Value, values)
		if err != nil {
			t.pointFailed(dp.PointName, err)
			continue
		}
		t.pointOK(dp.PointName)
		if value == nil {
			t.logger.Debug().Str("point", dp.PointName).Msg("Expression returned nil, skipping")
			continue
		}
		dp.Value = value
		out = append(out, dp)
	}
	return out
}

// eval runs program and normalises its result.
func (t *Transformer) eval(program *vm.Program, value any, values map[string]any) (any, error) {
	result, err := expr.Run(program, env{Value: value, Points: values, Lookups: t.lookups})
	if err != nil {
		return nil, fmt.Errorf("transform: %w", err)
	}
	return normalize(result)
}

// normalize turns every number into float64, as the readers deliver them, so a field
// keeps one type in InfluxDB; bools, strings and nil are kept. Other results (lists,
// maps) are an error.
func normalize(v any) (any, error) {
	switch v.(type) {
	case nil, bool, string:
		return v, nil
	}
	rv := reflect.ValueOf(v)
	switch {
	case rv.CanFloat():
		return rv.Float(), nil
	case rv.CanInt():
		return float64(rv.Int()), nil
	case rv.CanUint():
		return float64(rv.Uint()), nil
	default:
		return nil, fmt.Errorf("transform: unsupported result type %T", v)
	}
}

// pointFailed warns once per point until its expression works again; repeats go to
// debug, so a value that keeps failing does not flood the log.
func (t *Transformer) pointFailed(name string, err error) {
	if t.failedPoints[name] {
		t.logger.Debug().Str("point", name).Err(err).Msg("Expression failed")
		return
	}
	t.failedPoints[name] = true
	t.logger.Warn().Str("point", name).Err(err).
		Msg("Expression failed, point skipped; further failures of this point are logged at debug")
}

// pointOK logs at info when a point whose expression failed before works again.
func (t *Transformer) pointOK(name string) {
	if t.failedPoints[name] {
		delete(t.failedPoints, name)
		t.logger.Info().Str("point", name).Msg("Expression works again")
	}
}
