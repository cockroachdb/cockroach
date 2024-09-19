// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fipsccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func init() {
	overload := tree.Overload{
		Types:      tree.ParamTypes{},
		ReturnType: tree.FixedReturnType(types.Bool),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			if err := utilccl.CheckEnterpriseEnabled(
				evalCtx.Settings, "fips_ready",
			); err != nil {
				return nil, err
			}
			// It's debatable whether we need a permission check here at all.
			// It's not very sensitive and is (currently) a very cheap function
			// call. However, it's something that regular users should have no
			// reason to look at so in the interest of least privilege we put it
			// behind the VIEWCLUSTERSETTING privilige.
			session := evalCtx.SessionAccessor
			hasView, err := session.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWCLUSTERSETTING)
			if err != nil {
				return nil, err
			} else if !hasView {
				return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
					"user %s does not have %s system privilege",
					evalCtx.SessionData().User(),
					privilege.VIEWCLUSTERSETTING,
				)
			}

			return tree.MakeDBool(tree.DBool(IsFIPSReady())), nil
		},
		Class:      tree.NormalClass,
		Volatility: volatility.Stable,
	}

	utilccl.RegisterCCLBuiltin("crdb_internal.fips_ready",
		`Returns true if all FIPS readiness checks pass.`,
		overload)
}
