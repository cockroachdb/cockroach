// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const randomTupleIterations = 1000
const randomTupleMaxLength = 10
const randomTupleStringMaxLength = 1000

func TestParseTupleRandomStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < randomTupleIterations; i++ {
		numElems := rng.Intn(randomTupleMaxLength)
		tup := make([][]byte, numElems)
		tupContents := []*types.T{}
		for tupIdx := range tup {
			len := rng.Intn(randomTupleStringMaxLength)
			str := make([]byte, len)
			for strIdx := 0; strIdx < len; strIdx++ {
				str[strIdx] = byte(rng.Intn(256))
			}
			tup[tupIdx] = str
			tupContents = append(tupContents, types.String)
		}

		var buf bytes.Buffer
		buf.WriteByte('(')
		for j, b := range tup {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteByte('"')
			// The input format for this doesn't support regular escaping, any
			// character can be preceded by a backslash to encode it directly (this
			// means that there's no printable way to encode non-printing characters,
			// users must use `e` prefixed strings).
			for _, c := range b {
				if c == '"' || c == '\\' || rng.Intn(10) == 0 {
					buf.WriteByte('\\')
				}
				buf.WriteByte(c)
			}
			buf.WriteByte('"')
		}
		buf.WriteByte(')')

		parsed, _, err := tree.ParseDTupleFromString(
			tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings()), buf.String(), types.MakeTuple(tupContents))
		if err != nil {
			t.Fatalf(`got error: "%s" for elem "%s"`, err, buf.String())
		}
		for tupIdx := range tup {
			value := tree.MustBeDString(parsed.D[tupIdx])
			if string(value) != string(tup[tupIdx]) {
				t.Fatalf(`iteration %d: tuple "%s", got %#v, expected %#v`, i, buf.String(), value, string(tup[tupIdx]))
			}
		}
	}
}

func TestParseTupleRandomDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for i := 0; i < randomTupleIterations; i++ {
		numElems := rng.Intn(randomTupleMaxLength)
		tupContents := []*types.T{}
		for i := 0; i < numElems; i++ {
			tupContents = append(tupContents, randgen.RandType(rng))
		}
		tup := randgen.RandDatum(rng, types.MakeTuple(tupContents), true /* nullOk */)
		tupString := tree.AsString(tup)

		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		parsed, _, err := tree.ParseDTupleFromString(
			evalCtx, tupString, types.MakeTuple(tupContents))
		if err != nil {
			t.Fatalf(`got error: "%s" for elem "%s"`, err, tup)
		}
		if tup.Compare(evalCtx, parsed) != 0 {
			t.Fatalf(`iteration %d: tuple "%s", got %#v, expected %#v`, i, tupString, parsed, tup)
		}
	}
}
