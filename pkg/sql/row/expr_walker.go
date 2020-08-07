// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// reseedRandEveryN is the number of calls before reseeding happens.
// TODO (anzoteh96): setting reseedRandEveryN presents the tradeoff
// between the frequency of re-seeding and the number of calls to
// Float64() needed upon every resume. Therefore it will be useful to
// tune this parameter.
const reseedRandEveryN = 1000

type importRand struct {
	*rand.Rand
	pos int64
}

func newImportRand(pos int64) *importRand {
	adjPos := (pos / reseedRandEveryN) * reseedRandEveryN
	rnd := rand.New(rand.NewSource(adjPos))
	for i := int(pos % reseedRandEveryN); i > 0; i-- {
		_ = rnd.Float64()
	}
	return &importRand{rnd, pos}
}

func (r *importRand) advancePos() {
	r.pos++
	if r.pos%reseedRandEveryN == 0 {
		// Time to reseed.
		r.Rand = rand.New(rand.NewSource(r.pos))
	}
}

func (r *importRand) Float64() float64 {
	randNum := r.Rand.Float64()
	r.advancePos()
	return randNum
}

func (r *importRand) Int63() int64 {
	randNum := r.Rand.Int63()
	r.advancePos()
	return randNum
}

func getSeedForImportRand(rowID int64, sourceID int32, numInstances int) int64 {
	// We expect r.pos to increment by numInstances for each row.
	// Therefore, assuming that rowID increments by 1 for every row,
	// we will initialize the position as rowID * numInstances + sourceID << rowIDBits.
	rowIDWithMultiplier := int64(numInstances) * rowID
	return (int64(sourceID) << rowIDBits) ^ rowIDWithMultiplier
}

// For some functions (specifically the volatile ones), we do
// not want to use the provided builtin. Instead, we opt for
// our own function definition, which produces deterministic results.
func makeBuiltinOverride(
	builtin *tree.FunctionDefinition, overloads ...tree.Overload,
) *tree.FunctionDefinition {
	props := builtin.FunctionProperties
	return tree.NewFunctionDefinition(
		"import."+builtin.Name, &props, overloads)
}

type overrideVolatility bool

const (
	// The following constants are the override volatility constants to
	// decide whether a default expression can be evaluated at the new
	// datum converter stage. Note that overrideErrorTerm is a placeholder
	// to be returned when an error is returned at sanitizeExprForImport.
	overrideErrorTerm overrideVolatility = false
	overrideImmutable overrideVolatility = false
	overrideVolatile  overrideVolatility = true
)

// cellInfoAddr is the address used to store relevant information
// in the Annotation field of evalCtx when evaluating expressions.
const cellInfoAddr tree.AnnotationIdx = iota + 1

type cellInfoAnnotation struct {
	sourceID            int32
	rowID               int64
	uniqueRowIDInstance int
	uniqueRowIDTotal    int
	randSource          *importRand
	randInstancePerRow  int
}

func getCellInfoAnnotation(t *tree.Annotations) *cellInfoAnnotation {
	return t.Get(cellInfoAddr).(*cellInfoAnnotation)
}

func (c *cellInfoAnnotation) Reset(sourceID int32, rowID int64) {
	c.sourceID = sourceID
	c.rowID = rowID
	c.uniqueRowIDInstance = 0
}

// We don't want to call unique_rowid() for columns with such default expressions
// because it is not idempotent and has unfortunate overlapping of output
// spans since it puts the uniqueness-ensuring per-generator part (nodeID)
// in the low-bits. Instead, make our own IDs that attempt to keep each
// generator (sourceID) writing to its own key-space with sequential
// rowIndexes mapping to sequential unique IDs. This is done by putting the
// following as the lower bits, in order to handle the case where there are
// multiple columns with default as `unique_rowid`:
//
// #default_rowid_cols * rowIndex + colPosition (among those with default unique_rowid)
//
// To avoid collisions with the SQL-genenerated IDs (at least for a
// very long time) we also flip the top bit to 1.
//
// Producing sequential keys in non-overlapping spans for each source yields
// observed improvements in ingestion performance of ~2-3x and even more
// significant reductions in required compactions during IMPORT.
//
// TODO(dt): Note that currently some callers (e.g. CSV IMPORT, which can be
// used on a table more than once) offset their rowIndex by a wall-time at
// which their overall job is run, so that subsequent ingestion jobs pick
// different row IDs for the i'th row and don't collide. However such
// time-offset rowIDs mean each row imported consumes some unit of time that
// must then elapse before the next IMPORT could run without colliding e.g.
// a 100m row file would use 10µs/row or ~17min worth of IDs. For now it is
// likely that IMPORT's write-rate is still the limiting factor, but this
// scheme means rowIndexes are very large (1 yr in 10s of µs is about 2^42).
// Finding an alternative scheme for avoiding collisions (like sourceID *
// fileIndex*desc.Version) could improve on this. For now, if this
// best-effort collision avoidance scheme doesn't work in some cases we can
// just recommend an explicit PK as a workaround.
//
// TODO(anzoteh96): As per the issue in #51004, having too many columns with
// default expression unique_rowid() could cause collisions when IMPORTs are run
// too close to each other. It will therefore be nice to fix this problem.
func importUniqueRowID(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	avoidCollisionsWithSQLsIDs := uint64(1 << 63)
	shiftedIndex := int64(c.uniqueRowIDTotal)*c.rowID + int64(c.uniqueRowIDInstance)
	returnIndex := (uint64(c.sourceID) << rowIDBits) ^ uint64(shiftedIndex)
	c.uniqueRowIDInstance++
	evalCtx.Annotations.Set(cellInfoAddr, c)
	return tree.NewDInt(tree.DInt(avoidCollisionsWithSQLsIDs | returnIndex)), nil
}

func importRandom(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	if c.randSource == nil {
		c.randSource = newImportRand(getSeedForImportRand(
			c.rowID, c.sourceID, c.randInstancePerRow))
	}
	return tree.NewDFloat(tree.DFloat(c.randSource.Float64())), nil
}

func importGenUUID(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	c := getCellInfoAnnotation(evalCtx.Annotations)
	if c.randSource == nil {
		c.randSource = newImportRand(getSeedForImportRand(
			c.rowID, c.sourceID, c.randInstancePerRow))
	}
	gen := c.randSource.Int63()
	id := uuid.MakeV4()
	id.DeterministicV4(uint64(gen), uint64(1<<63))
	return tree.NewDUuid(tree.DUuid{UUID: id}), nil
}

// Besides overriding, there are also counters that we want to keep track
// of as we walk through the expressions in a row (at datumRowConverter creation
// time). This will be handled by the visitorSideEffect field: it will be
// called with an annotation passed in that changes the counter. In the case of
// unique_rowid, for example, we want to keep track of the total number of
// unique_rowid occurrences in a row.
type customFunc struct {
	visitorSideEffect func(annotations *tree.Annotations)
	override          *tree.FunctionDefinition
}

var useDefaultBuiltin *customFunc

// Given that imports can be retried and resumed, we want to
// ensure that the default functions return the same value given
// the same arguments, even on retries. Therfore we decide to support
// only a limited subset of non-immutable functions, which are
// all listed here.
var supportedImportFuncOverrides = map[string]*customFunc{
	// These methods can be supported given that we set the statement
	// and transaction timestamp to be equal, i.e. the write timestamp.
	"current_date":          useDefaultBuiltin,
	"current_timestamp":     useDefaultBuiltin,
	"localtimestamp":        useDefaultBuiltin,
	"now":                   useDefaultBuiltin,
	"statement_timestamp":   useDefaultBuiltin,
	"timeofday":             useDefaultBuiltin,
	"transaction_timestamp": useDefaultBuiltin,
	"unique_rowid": {
		visitorSideEffect: func(annot *tree.Annotations) {
			getCellInfoAnnotation(annot).uniqueRowIDTotal++
		},
		override: makeBuiltinOverride(
			tree.FunDefs["unique_rowid"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Int),
				Fn:         importUniqueRowID,
				Info:       "Returns a unique rowid based on row position and time",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
	"random": {
		visitorSideEffect: func(annot *tree.Annotations) {
			getCellInfoAnnotation(annot).randInstancePerRow++
		},
		override: makeBuiltinOverride(
			tree.FunDefs["random"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Float),
				Fn:         importRandom,
				Info:       "Returns a random number between 0 and 1 based on row position and time.",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
	"gen_random_uuid": {
		visitorSideEffect: func(annot *tree.Annotations) {
			getCellInfoAnnotation(annot).randInstancePerRow++
		},
		override: makeBuiltinOverride(
			tree.FunDefs["gen_random_uuid"],
			tree.Overload{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Uuid),
				Fn:         importGenUUID,
				Info: "Generates a random UUID based on row position and time, " +
					"and returns it as a value of UUID type.",
				Volatility: tree.VolatilityVolatile,
			},
		),
	},
}

func unsafeExpressionError(err error, msg string, expr string) error {
	return errors.Wrapf(err, "default expression %q is unsafe for import: %s", expr, msg)
}

// unsafeErrExpr is a wrapper for errors arising from unsafe default
// expression created at row converter stage so that the appropriate
// error can be returned at the Row() stage.
type unsafeErrExpr struct {
	tree.TypedExpr
	err error
}

var _ tree.TypedExpr = &unsafeErrExpr{}

// Eval implements the TypedExpr interface.
func (e *unsafeErrExpr) Eval(_ *tree.EvalContext) (tree.Datum, error) {
	return nil, e.err
}

// importDefaultExprVisitor must be invoked on a typed expression. This
// visitor walks the tree and ensures that any expression in the tree
// that's not immutable is what we explicitly support.
type importDefaultExprVisitor struct {
	err         error
	ctx         context.Context
	annotations *tree.Annotations
	semaCtx     *tree.SemaContext
	// The volatility flag will be set if there's at least one volatile
	// function appearing in the default expression.
	volatility overrideVolatility
}

// VisitPre implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return v.err == nil, expr
}

// VisitPost implements tree.Visitor interface.
func (v *importDefaultExprVisitor) VisitPost(expr tree.Expr) (newExpr tree.Expr) {
	if v.err != nil {
		return expr
	}
	fn, ok := expr.(*tree.FuncExpr)
	if !ok || fn.ResolvedOverload().Volatility <= tree.VolatilityImmutable {
		// If an expression is not a function, or is an immutable function, then
		// we can use it as it is.
		return expr
	}
	resolvedFnName := fn.Func.FunctionReference.(*tree.FunctionDefinition).Name
	custom, isSafe := supportedImportFuncOverrides[resolvedFnName]
	if !isSafe {
		v.err = errors.Newf(`function %s unsupported by IMPORT INTO`, resolvedFnName)
		return expr
	}
	if custom == useDefaultBuiltin {
		// No override exists, means it's okay to use the definitions given in
		// builtin.go.
		return expr
	}
	// Override exists, so we turn the volatility flag of the visitor to true.
	// In addition, the visitorSideEffect function needs to be called to update
	// any relevant counter (e.g. the total number of occurrences of the
	// unique_rowid function in an expression).
	v.volatility = overrideVolatile
	if custom.visitorSideEffect != nil {
		custom.visitorSideEffect(v.annotations)
	}
	funcExpr := &tree.FuncExpr{
		Func:  tree.ResolvableFunctionReference{FunctionReference: custom.override},
		Type:  fn.Type,
		Exprs: fn.Exprs,
	}
	// The override must have appropriate overload defined.
	overrideExpr, err := funcExpr.TypeCheck(v.ctx, v.semaCtx, fn.ResolvedType())
	if err != nil {
		v.err = errors.Wrapf(err, "error overloading function")
	}
	return overrideExpr
}

// sanitizeExprsForImport checks whether default expressions are supported
// for import.
func sanitizeExprsForImport(
	ctx context.Context, evalCtx *tree.EvalContext, expr tree.Expr, targetType *types.T,
) (tree.TypedExpr, overrideVolatility, error) {
	semaCtx := tree.MakeSemaContext()

	// If we have immutable expressions, then we can just return it right away.
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		ctx, expr, targetType, "import_default", &semaCtx, tree.VolatilityImmutable)
	if err == nil {
		return typedExpr, overrideImmutable, nil
	}
	// Now that the expressions are not immutable, we first check that they
	// are of the correct type before checking for any unsupported functions
	// for import.
	typedExpr, err = tree.TypeCheck(ctx, expr, &semaCtx, targetType)
	if err != nil {
		return nil, overrideErrorTerm,
			unsafeExpressionError(err, "type checking error", expr.String())
	}
	v := &importDefaultExprVisitor{annotations: evalCtx.Annotations}
	newExpr, _ := tree.WalkExpr(v, typedExpr)
	if v.err != nil {
		return nil, overrideErrorTerm,
			unsafeExpressionError(v.err, "expr walking error", expr.String())
	}
	return newExpr.(tree.TypedExpr), v.volatility, nil
}
