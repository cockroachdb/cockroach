// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
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

	cast.ForEachCast(func(
		fromOID oid.Oid, toOID oid.Oid, castCtx cast.Context, ctxOrigin cast.ContextOrigin, v volatility.V,
	) {
		fromTyp, ok := types.OidToType[fromOID]
		if !ok {
			return
		}
		// Cannot cast as tuple.
		if fromTyp.Family() == types.TupleFamily {
			return
		}
		toTyp, ok := types.OidToType[toOID]
		if !ok {
			return
		}
		toName := tree.Name(cast.CastTypeName(toTyp))
		q := fmt.Sprintf("SELECT %s(NULL::%s)", toName.String(), fromTyp.String())
		t.Run(q, func(t *testing.T) {
			_, err := sqlDB.Exec(q)
			require.NoError(t, err)
		})
	})
}
