// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"cmp"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ErrCaseNotPossible is a sentinel indicating that an elected case could not
// occur. For example, picking any element from an empty slice.
var ErrCaseNotPossible = errors.New("case not possible")

// PickOne returns a random element from options. If options is empty, it
// returns ErrCaseNotPossible.
func PickOne[T any](rng *rand.Rand, options []T) (T, error) {
	if len(options) < 1 {
		var zero T
		return zero, errors.WithStack(ErrCaseNotPossible)
	}
	return options[rng.Intn(len(options))], nil
}

// PickAtLeast returns [`atLeast`, `len(options)`] elements from options in a
// random order. It returns ErrCaseNotPossible, if options does not contain
// `atLeast` elements. It panics if `atLeast` is less than zero.
func PickAtLeast[T any](rng *rand.Rand, atLeast int, options []T) ([]T, error) {
	if atLeast > len(options) {
		return nil, errors.WithStack(ErrCaseNotPossible)
	}
	return PickBetween(rng, atLeast, len(options), options)
}

// PickBetween returns [`atLeast`, `atMost`] elements from options. It panics
// if `atLeast` or `atMost` is less than zero. It returns ErrCaseNotPossible,
// if options does not contain `atLeast` elements,
func PickBetween[T any](rng *rand.Rand, atLeast, atMost int, options []T) ([]T, error) {
	if atLeast < 0 || atMost < 0 || atLeast > atMost {
		panic(errors.AssertionFailedf(
			"bad arguments to PickBetween: [%d, %d]", atLeast, atMost,
		))
	}

	if len(options) < atLeast {
		return nil, errors.WithStack(ErrCaseNotPossible)
	}

	if atMost > len(options) {
		atMost = len(options)
	}

	cloned := slices.Clone(options)

	rng.Shuffle(len(cloned), func(i, j int) {
		cloned[i], cloned[j] = cloned[j], cloned[i]
	})

	length := atLeast + rng.Intn(atMost-atLeast+1)
	return options[:length], nil
}

// Values is a helper for formatting a list of SQL values within a
// [GenerationCase]. It delegates to [tree.AsString] and joins the resultant
// strings with commas.
// Example Template Usage: `INSERT INTO table VALUES ({HelperThatReturnsValues})
type Values []tree.NodeFormatter

func AsValues[T tree.NodeFormatter](values []T, err error) (Values, error) {
	if err != nil {
		return nil, err
	}

	nfs := make([]tree.NodeFormatter, len(values))
	for i := range values {
		nfs[i] = values[i]
	}
	return nfs, nil
}

// String implements fmt.Stringer.
func (v Values) String() string {
	var b strings.Builder
	for i, nf := range v {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(tree.AsString(nf))
	}
	return b.String()
}

// GenerationCase is an "interesting" form of a specific [tree.Statement] and
// the [pgcode.Code] it is expected to return.
type GenerationCase struct {
	// Code is the expected [pgcode.Code] of this case. Successful cases should
	// use [pgcode.SuccessfulCompletion]
	Code pgcode.Code
	// Template is a text/template using `{` and `}` as delimiters that should be
	// parsable as SQL when executed.
	// Example: `CREATE TABLE {TableName} ({Columns})`
	Template string
}

type compiledCase struct {
	Code         pgcode.Code
	TemplateName string
}

// Generate is a helper to produce a psuedo-random case for a given
// [tree.Statement]. It returns the selected case, parsed into a
// [tree.Statement], and the expected [pgcode.Code].
//
// Functions may return ErrCaseNotPossible to indicate that they can't return
// an appropriate value. Closures may be used to track state of generation. For
// example, a "Table" function may set a local variable so that a "Columns"
// function knows which table to select columns from rather than buffering all
// possible columns into memory.
//
// Usage:
//
//	Generate[*tree.AlterType](og.params.rng, og.produceError(), []GenerationCase{
//		{pgcode.SuccessfulCompletion, `ALTER TYPE {Type} ADD VALUE {Value}`},
//		{pgcode.FileAlreadyExists, `ALTER TYPE "NeverExists" ADD VALUE "Irrelevant"`},
//	}, template.FuncMap{
//		"Type": func() *tree.Name {
//			// Types that implement tree.NodeFormatter should be used to properly escape values.
//			return PickOne(og.params.rng, existingTypes)
//		},
//		"Value": func() string {
//			return "foo"
//		},
//	})
func Generate[T tree.Statement](
	rng *rand.Rand, produceError bool, cases []GenerationCase, funcs template.FuncMap,
) (T, pgcode.Code, error) {
	var zero T
	name := reflect.TypeOf(zero).Name()

	tpl := template.New(name).Delims("{", "}").Funcs(funcs)

	// TODO(chrisseto): Consider caching the compiled templates.
	var compiledCases []compiledCase
	for i, gc := range cases {
		tplName := fmt.Sprintf("Case %d - %s", i, gc.Code.String())

		// If produceError is true, exclude successful cases from our selection.
		if gc.Code == pgcode.SuccessfulCompletion && produceError {
			continue
		}

		caseTpl := tpl.New(tplName)
		if _, err := caseTpl.Parse(gc.Template); err != nil {
			return zero, pgcode.Code{}, errors.NewAssertionErrorWithWrappedErrf(err, "failed to parse %q", tplName)
		}

		compiledCases = append(compiledCases, compiledCase{
			Code:         gc.Code,
			TemplateName: tplName,
		})
	}

	// Randomize which case we'll select.
	rng.Shuffle(len(compiledCases), func(i, j int) {
		compiledCases[i], compiledCases[j] = compiledCases[j], compiledCases[i]
	})

	// Prioritize cases that will succeed by pushing them to the front of our
	// slice. If no successful case is possible, we'll fallback to error cases.
	slices.SortStableFunc(compiledCases, func(a, b compiledCase) int {
		aIsSuccess := a.Code == pgcode.SuccessfulCompletion
		bIsSuccess := b.Code == pgcode.SuccessfulCompletion

		if aIsSuccess && bIsSuccess {
			// If both are valid, choose randomly.
			return cmp.Compare(rng.Float64(), 0.5)
		}
		if aIsSuccess {
			return -1
		}
		return +1
	})

	for _, c := range compiledCases {
		var raw strings.Builder
		if err := tpl.ExecuteTemplate(&raw, c.TemplateName, nil); err != nil {
			// If a case isn't possible, continue iterating until we find a valid
			// case.
			if errors.Is(err, ErrCaseNotPossible) {
				continue
			}
			return zero, pgcode.Code{}, errors.WithStack(err)
		}

		// NB: Parsing the template result as SQL ensures that we catch any
		// template errors and distinguish them from workload errors. It also
		// allows us to normalize the outputs so users don't have to worry about
		// whitespace and/or capitalization.
		stmt, err := parser.ParseOne(raw.String())
		if err != nil {
			return zero, pgcode.Code{}, errors.AssertionFailedf(
				"syntax error; could not parse %T: %q",
				zero, raw.String(),
			)
		}

		if asT, ok := stmt.AST.(T); ok {
			return asT, c.Code, nil
		}

		return zero, pgcode.Code{}, errors.AssertionFailedf(
			"expected to parse %T; got %T: %q",
			zero, stmt.AST, raw.String(),
		)
	}

	return zero, pgcode.Code{}, errors.Wrapf(ErrCaseNotPossible, "generating any case for %T", zero)
}
