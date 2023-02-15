// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// TestCastBuiltins sanity checks all casts for the cast map work.
// Note we don't have to check for families or anything crazy like we do
// for shouldMakeFromCastBuiltin.
func TestCastBuiltins(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer serv.Stopper().Stop(ctx)

	for fromOID, castMap := range cast.CastMap() {
		fromTyp, ok := types.OidToType[fromOID]
		if !ok {
			continue
		}
		// Cannot cast as tuple.
		if fromTyp.Family() == types.TupleFamily {
			continue
		}
		for toOID := range castMap {
			toTyp, ok := types.OidToType[toOID]
			if !ok {
				continue
			}
			toName := castFriendlyTypeName(toTyp)
			q := fmt.Sprintf("SELECT %s(NULL::%s)", toName.String(), fromTyp.String())
			t.Run(q, func(t *testing.T) {
				_, err := sqlDB.Exec(q)
				require.NoError(t, err)
			})
		}
	}
}

func castFriendlyTypeName(typ *types.T) tree.Name {
	switch typ.Oid() {
	case oid.T_char:
		// SQLString returns `"char"` for oid.T_char. This makes tree.Name
		// quote this as `"""char"""` which is not ideal.
		// Thus, we remove the double quotes when translating this to tree.Name
		// so that the .String() includes the quotes as expected.
		return "char"
	// SQLString is wrong for these types.
	case oid.T_numeric:
		return "numeric"
	case oid.T_bpchar:
		return "bpchar"
	}
	// Cast all type names to `tree.Name` so it gets quoted if it is a reserved
	// keyword.
	return tree.Name(strings.ToLower(typ.SQLString()))
}
