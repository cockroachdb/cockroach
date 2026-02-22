// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txntest

import (
	"time"
)

// Vars represents captured variables within a transaction template execution.
type Vars map[string]interface{}

// Bindings represents per-transaction parameter bindings produced by ParamGen.
type Bindings map[string]interface{}

// Expr produces a value at runtime given the current Vars and Bindings.
type Expr interface {
	// eval computes the value of the expression.
	eval(vars Vars, bindings Bindings) (interface{}, error)
}

// literalExpr returns a constant value.
type literalExpr struct {
	value interface{}
}

func (l literalExpr) eval(_ Vars, _ Bindings) (interface{}, error) { return l.value, nil }

// varExpr references a named variable from Vars or Bindings.
type varExpr struct {
	name string
}

func (v varExpr) eval(vars Vars, bindings Bindings) (interface{}, error) {
	if val, ok := bindings[v.name]; ok {
		return val, nil
	}
	if val, ok := vars[v.name]; ok {
		return val, nil
	}
	return nil, nil
}

// Lit returns a literal expression that always evaluates to value.
func Lit(value interface{}) Expr { return literalExpr{value: value} }

// Var references a named variable from Vars or Bindings.
func Var(name string) Expr { return varExpr{name: name} }

// Step is one operation within a transaction template.
type Step interface{ isStep() }

// Exec issues a statement that does not return rows.
type execStep struct {
	sql  string
	args []Expr
}

func (execStep) isStep() {}

// Exec constructs an execStep.
func Exec(sql string, args ...Expr) *execStep { return &execStep{sql: sql, args: args} }

// QueryRow issues a query expected to return at most a single row and captures
// values into named Vars.
type queryRowStep struct {
	sql     string
	args    []Expr
	capture []string
}

func (queryRowStep) isStep() {}

// QueryRow constructs a queryRowStep. Use .Capture to declare destination var names.
func QueryRow(sql string, args ...Expr) *queryRowStep { return &queryRowStep{sql: sql, args: args} }

// Capture binds the scanned columns to the given variable names.
func (q *queryRowStep) Capture(names ...string) *queryRowStep {
	q.capture = append([]string(nil), names...)
	return q
}

// SleepStep sleeps for a duration inside a transaction to widen interleavings.
type sleepStep struct{ d time.Duration }

func (sleepStep) isStep() {}

// Sleep constructs a sleepStep.
func Sleep(d time.Duration) *sleepStep { return &sleepStep{d: d} }

// SavepointRestartStep is a marker to indicate where a restart savepoint is placed or used.
// Currently a no-op placeholder; retries are handled at the transaction level.
type savepointRestartStep struct{}

func (savepointRestartStep) isStep() {}

// SavepointRestart constructs a savepointRestartStep.
func SavepointRestart() *savepointRestartStep { return &savepointRestartStep{} }

// Steps is a helper to build step slices.
func Steps(s ...Step) []Step { return s }

// RetryPolicy controls how transaction retries are performed.
type RetryPolicy int

const (
	// RetryNone executes a transaction once without automatic retry.
	RetryNone RetryPolicy = iota
	// RetrySerializable retries the full transaction on retriable errors.
	RetrySerializable
)

// TxnTemplate describes a multi-statement transaction.
type TxnTemplate struct {
	Name     string
	Steps    []Step
	ParamGen func(rng RNG, vars Vars) Bindings
	Weight   int
	Retry    RetryPolicy
}

// RNG abstracts rand.Rand for easier testing.
type RNG interface {
	Intn(n int) int
}
