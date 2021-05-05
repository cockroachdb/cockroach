// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestAdjustStartKeyForInterleave(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Secondary indexes with DESC direction in the last column.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	// Index with implicit primary columns (pid1, cid2).
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_non_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE UNIQUE INDEX child_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent1")
	child := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child1")
	grandchild := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "grandchild1")

	parentDescIdx := parent.PublicNonPrimaryIndexes()[0]
	childDescIdx := child.PublicNonPrimaryIndexes()[0]
	childNonUniqueIdx := child.PublicNonPrimaryIndexes()[1]
	childUniqueIdx := child.PublicNonPrimaryIndexes()[2]
	grandchildDescIdx := grandchild.PublicNonPrimaryIndexes()[0]

	testCases := []struct {
		index catalog.Index
		// See ShortToLongKeyFmt for how to represent a key.
		input    string
		expected string
	}{
		// NOTNULLASC can appear at the end of a start key for
		// constraint IS NOT NULL on an ASC index (NULLs sorted first,
		// span starts (start key) on the first non-NULL).
		// See encodeStartConstraintAscending.

		{
			index:    parent.GetPrimaryIndex(),
			input:    "/NOTNULLASC",
			expected: "/NOTNULLASC",
		},
		{
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},
		{
			index:    grandchild.GetPrimaryIndex(),
			input:    "/1/#/2/3/#/NOTNULLASC",
			expected: "/1/#/2/3/#/NOTNULLASC",
		},

		{
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/NOTNULLASC",
			expected: "/1/#/NOTNULLASC",
		},

		{
			index:    grandchild.GetPrimaryIndex(),
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},

		// NULLDESC can appear at the end of a start key for constraint
		// IS NULL on a DESC index (NULLs sorted last, span starts
		// (start key) on the first NULLs).
		// See encodeStartConstraintDescending.

		{
			index:    parentDescIdx,
			input:    "/NULLDESC",
			expected: "/NULLDESC",
		},
		{
			index:    childDescIdx,
			input:    "/1/#/2/NULLDESC",
			expected: "/1/#/2/NULLDESC",
		},
		{
			index:    grandchildDescIdx,
			input:    "/1/#/2/3/#/NULLDESC",
			expected: "/1/#/2/3/#/NULLDESC",
		},

		{
			index:    childDescIdx,
			input:    "/1/#/NULLDESC",
			expected: "/1/#/NULLDESC",
		},

		// Keys that belong to the given index (neither parent nor
		// children keys) do not need to be tightened.
		{
			index:    parent.GetPrimaryIndex(),
			input:    "/1",
			expected: "/1",
		},
		{
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/3",
			expected: "/1/#/2/3",
		},

		// Parent keys wrt child index is not tightened.
		{
			index:    child.GetPrimaryIndex(),
			input:    "/1",
			expected: "/1",
		},

		// Children keys wrt to parent index is tightened (pushed
		// forwards) to the next parent key.
		{
			index:    parent.GetPrimaryIndex(),
			input:    "/1/#/2/3",
			expected: "/2",
		},
		{
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/3/#/4",
			expected: "/1/#/2/4",
		},

		// Key with len > 1 tokens.
		{
			index:    child.GetPrimaryIndex(),
			input:    "/12345678901234/#/1234/1234567890/#/123/1234567",
			expected: "/12345678901234/#/1234/1234567891",
		},
		{
			index:    child.GetPrimaryIndex(),
			input:    "/12345678901234/#/d1403.2594/shelloworld/#/123/1234567",
			expected: "/12345678901234/#/d1403.2594/shelloworld/PrefixEnd",
		},

		// Index key with extra columns (implicit primary key columns).
		// We should expect two extra columns (in addition to the
		// two index columns).
		{
			index:    childNonUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			index:    childNonUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			index:    childNonUniqueIdx,
			input:    "/2/3/4/5",
			expected: "/2/3/4/5",
		},
		{
			index:    childNonUniqueIdx,
			input:    "/2/3/4/5/#/10",
			expected: "/2/3/4/6",
		},

		// Unique indexes only include implicit columns if they have
		// a NULL value.
		{
			index:    childUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			index:    childUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/4",
		},
		{
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4",
			expected: "/2/NULLASC/4",
		},
		{
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4/5",
			expected: "/2/NULLASC/4/5",
		},
		{
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4/5/#/6",
			expected: "/2/NULLASC/4/6",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			codec := keys.SystemSQLCodec
			actual := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.input))
			actual, err := rowenc.AdjustStartKeyForInterleave(codec, tc.index, actual)
			if err != nil {
				t.Fatal(err)
			}

			expected := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.expected))
			if !expected.Equal(actual) {
				t.Errorf("expected tightened start key %s, got %s", expected, actual)
			}
		})
	}
}

func TestAdjustEndKeyForInterleave(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sqlutils.CreateTestInterleavedHierarchy(t, sqlDB)

	// Secondary indexes with DESC direction in the last column.
	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, fmt.Sprintf(`CREATE INDEX pid1_desc ON %s.parent1 (pid1 DESC)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_desc ON %s.child1 (pid1, cid1, cid2 DESC) INTERLEAVE IN PARENT %s.parent1 (pid1)`, sqlutils.TestDB, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE INDEX grandchild_desc ON %s.grandchild1 (pid1, cid1, cid2, gcid1 DESC) INTERLEAVE IN PARENT %s.child1(pid1, cid1, cid2)`, sqlutils.TestDB, sqlutils.TestDB))
	// Index with implicit primary columns (pid1, cid2).
	r.Exec(t, fmt.Sprintf(`CREATE INDEX child_non_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))
	r.Exec(t, fmt.Sprintf(`CREATE UNIQUE INDEX child_unique ON %s.child1 (v, cid1)`, sqlutils.TestDB))

	// The interleaved hierarchy is as follows:
	//    parent		(pid1)
	//	child		(pid1, cid1, cid2)
	//	  grandchild	(pid1, cid1, cid2, gcid1)
	parent := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "parent1")
	child := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "child1")
	grandchild := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, "grandchild1")

	parentDescIdx := parent.PublicNonPrimaryIndexes()[0]
	childDescIdx := child.PublicNonPrimaryIndexes()[0]
	childNonUniqueIdx := child.PublicNonPrimaryIndexes()[1]
	childUniqueIdx := child.PublicNonPrimaryIndexes()[2]
	grandchildDescIdx := grandchild.PublicNonPrimaryIndexes()[0]

	testCases := []struct {
		table catalog.TableDescriptor
		index catalog.Index
		// See ShortToLongKeyFmt for how to represent a key.
		input string
		// If the end key is assumed to be inclusive when passed to
		// to AdjustEndKeyForInterleave.
		inclusive bool
		expected  string
	}{
		// NOTNULLASC can appear at the end of an end key for
		// constraint IS NULL on an ASC index (NULLs sorted first,
		// span ends (end key) right before the first non-NULL).
		// See encodeEndConstraintAscending.

		{
			table:    parent,
			index:    parent.GetPrimaryIndex(),
			input:    "/NOTNULLASC",
			expected: "/NULLASC/#",
		},

		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NULLASC/#",
		},

		{
			table:    grandchild,
			index:    grandchild.GetPrimaryIndex(),
			input:    "/1/#/2/3/#/NOTNULLASC",
			expected: "/1/#/2/3/#/NULLASC/#",
		},

		// No change since interleaved rows cannot occur between
		// partial primary key columns.
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/NOTNULLASC",
			expected: "/1/#/NOTNULLASC",
		},

		// No change since key belongs to an ancestor.
		{
			table:    grandchild,
			index:    grandchild.GetPrimaryIndex(),
			input:    "/1/#/2/NOTNULLASC",
			expected: "/1/#/2/NOTNULLASC",
		},

		// NOTNULLDESC can appear at the end of a start key for
		// constraint IS NOT NULL on a DESC index (NULLs sorted last,
		// span ends (end key) right after the last non-NULL).
		// See encodeEndConstraintDescending.

		// No change since descending indexes are always secondary and
		// secondary indexes are never tightened since they cannot
		// have interleaved rows.

		{
			table:    parent,
			index:    parentDescIdx,
			input:    "/NOTNULLDESC",
			expected: "/NOTNULLDESC",
		},
		{
			table:    child,
			index:    childDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},
		{
			table:    grandchild,
			index:    grandchildDescIdx,
			input:    "/1/#/2/3/#/NOTNULLDESC",
			expected: "/1/#/2/3/#/NOTNULLDESC",
		},
		{
			table:    grandchild,
			index:    grandchildDescIdx,
			input:    "/1/#/2/NOTNULLDESC",
			expected: "/1/#/2/NOTNULLDESC",
		},

		// NULLASC with inclusive=true is possible with IS NULL for
		// ascending indexes.
		// See encodeEndConstraintAscending.

		{
			table:     parent,
			index:     parent.GetPrimaryIndex(),
			input:     "/NULLASC",
			inclusive: true,
			expected:  "/NULLASC/#",
		},

		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2/NULLASC",
			inclusive: true,
			expected:  "/1/#/2/NULLASC/#",
		},

		// Keys with all the column values of the primary key should be
		// tightened wrt to primary indexes since they can have
		// interleaved rows.

		{
			table:    parent,
			index:    parent.GetPrimaryIndex(),
			input:    "/1",
			expected: "/0/#",
		},
		{
			table:     parent,
			index:     parent.GetPrimaryIndex(),
			input:     "/1",
			inclusive: true,
			expected:  "/1/#",
		},

		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/3",
			expected: "/1/#/2/2/#",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#/2/3/#",
		},

		// Idempotency.

		{
			table:    parent,
			index:    parent.GetPrimaryIndex(),
			input:    "/1/#",
			expected: "/1/#",
		},
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#",
			expected: "/1/#",
		},
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/2/#",
			expected: "/1/#/2/2/#",
		},

		// Children end keys wrt a "parent" index should be tightened
		// to read up to the last parent key.

		{
			table:    parent,
			index:    parent.GetPrimaryIndex(),
			input:    "/1/#/2/3",
			expected: "/1/#",
		},
		{
			table:     parent,
			index:     parent.GetPrimaryIndex(),
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#",
		},

		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/3/#/4",
			expected: "/1/#/2/3/#",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2/3/#/4",
			inclusive: true,
			expected:  "/1/#/2/3/#",
		},

		// Parent keys wrt child keys need not be tightened.

		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1",
			expected: "/1",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1",
			inclusive: true,
			expected:  "/2",
		},

		// Keys with a partial prefix of the primary key columns
		// need not be tightened since no interleaving can occur after.

		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2",
			expected: "/1/#/2",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2",
			inclusive: true,
			expected:  "/1/#/3",
		},

		// Secondary indexes' end keys need not be tightened since
		// they cannot have interleaves.

		{
			table:    child,
			index:    childDescIdx,
			input:    "/1/#/2/3",
			expected: "/1/#/2/3",
		},
		{
			table:     child,
			index:     childDescIdx,
			input:     "/1/#/2/3",
			inclusive: true,
			expected:  "/1/#/2/4",
		},
		{
			table:    child,
			index:    childDescIdx,
			input:    "/1/#/2",
			expected: "/1/#/2",
		},
		{
			table:     child,
			index:     childDescIdx,
			input:     "/1/#/2",
			inclusive: true,
			expected:  "/1/#/3",
		},

		// Key with len > 1 tokens.
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/12345678901234/#/12345/12345678901234/#/123/1234567",
			expected: "/12345678901234/#/12345/12345678901234/#",
		},

		// Index key with extra columns (implicit primary key columns).
		// We should expect two extra columns (in addition to the
		// two index columns).
		{
			table:    child,
			index:    childNonUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		{
			table:    child,
			index:    childNonUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			table:    child,
			index:    childNonUniqueIdx,
			input:    "/2/3/4/5",
			expected: "/2/3/4/5",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    childNonUniqueIdx,
			input:    "/2/3/4/5/#/10",
			expected: "/2/3/4/5/#/10",
		},

		{
			table:    child,
			index:    childUniqueIdx,
			input:    "/2/3",
			expected: "/2/3",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    childUniqueIdx,
			input:    "/2/3/4",
			expected: "/2/3/4",
		},
		{
			table:    child,
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4",
			expected: "/2/NULLASC/4",
		},
		{
			table:    child,
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4/5",
			expected: "/2/NULLASC/4/5",
		},
		// End key not adjusted since secondary indexes can't have
		// interleaved rows.
		{
			table:    child,
			index:    childUniqueIdx,
			input:    "/2/NULLASC/4/5/#/6",
			expected: "/2/NULLASC/4/5/#/6",
		},

		// Keys with decimal values.
		// Not tightened since it's difficult to "go back" one logical
		// decimal value.
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/d3.4567",
			expected: "/1/#/2/d3.4567",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2/d3.4567",
			inclusive: true,
			expected:  "/1/#/2/d3.4567/#",
		},
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/d3.4567/#/8",
			expected: "/1/#/2/d3.4567/#",
		},

		// Keys with bytes values.
		// Not tightened since it's difficult to "go back" one logical
		// bytes value.
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/shelloworld",
			expected: "/1/#/2/shelloworld",
		},
		{
			table:     child,
			index:     child.GetPrimaryIndex(),
			input:     "/1/#/2/shelloworld",
			inclusive: true,
			expected:  "/1/#/2/shelloworld/#",
		},
		{
			table:    child,
			index:    child.GetPrimaryIndex(),
			input:    "/1/#/2/shelloworld/#/3",
			expected: "/1/#/2/shelloworld/#",
		},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			codec := keys.SystemSQLCodec
			actual := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.input))
			actual, err := rowenc.AdjustEndKeyForInterleave(codec, tc.table, tc.index, actual, tc.inclusive)
			if err != nil {
				t.Fatal(err)
			}

			expected := EncodeTestKey(t, kvDB, codec, ShortToLongKeyFmt(tc.expected))
			if !expected.Equal(actual) {
				t.Errorf("expected tightened end key %s, got %s", expected, actual)
			}
		})
	}
}

var tableNames = map[string]bool{
	"parent1":     true,
	"child1":      true,
	"grandchild1": true,
	"child2":      true,
	"parent2":     true,
}

// EncodeTestKey takes the short format representation of a key and transforms
// it into an actual roachpb.Key. Refer to ShortToLongKeyFmt for more info. on
// the short format.
// All tokens are interpreted as UVarint (ascending) unless they satisfy:
//    - '#' - interleaved sentinel
//    - 's' first byte - string/bytes (ascending)
//    - 'd' first byte - decimal (ascending)
//    - NULLASC, NULLDESC, NOTNULLASC, NOTNULLDESC
//    - PrefixEnd
func EncodeTestKey(tb testing.TB, kvDB *kv.DB, codec keys.SQLCodec, keyStr string) roachpb.Key {
	key := codec.TenantPrefix()
	tokens := strings.Split(keyStr, "/")
	if tokens[0] != "" {
		panic("missing '/' token at the beginning of long format")
	}

	// Omit the first empty string.
	tokens = tokens[1:]

	for _, tok := range tokens {
		if tok == "PrefixEnd" {
			key = key.PrefixEnd()
			continue
		}

		// Encode the table ID if the token is a table name.
		if tableNames[tok] {
			desc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, sqlutils.TestDB, tok)
			key = encoding.EncodeUvarintAscending(key, uint64(desc.GetID()))
			continue
		}

		switch tok[0] {
		case 's':
			key = encoding.EncodeStringAscending(key, tok[1:])
			continue
		case 'd':
			dec, cond, err := apd.NewFromString(tok[1:])
			if err != nil {
				tb.Fatal(err)
			}
			if cond.Any() {
				tb.Fatalf("encountered condition %s when parsing decimal", cond.String())
			}
			key = encoding.EncodeDecimalAscending(key, dec)
			continue
		}

		if tok == "NULLASC" {
			key = encoding.EncodeNullAscending(key)
			continue
		}

		if tok == "NOTNULLASC" {
			key = encoding.EncodeNotNullAscending(key)
			continue
		}

		if tok == "NULLDESC" {
			key = encoding.EncodeNullDescending(key)
			continue
		}

		// We make a distinction between this and the interleave
		// sentinel below.
		if tok == "NOTNULLDESC" {
			key = encoding.EncodeNotNullDescending(key)
			continue
		}

		// Interleaved sentinel.
		if tok == "#" {
			key = encoding.EncodeNotNullDescending(key)
			continue
		}

		// Assume any other value is an unsigned integer.
		tokInt, err := strconv.ParseInt(tok, 10, 64)
		if err != nil {
			tb.Fatal(err)
		}
		key = encoding.EncodeVarintAscending(key, tokInt)
	}

	return key
}
