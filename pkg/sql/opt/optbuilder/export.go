// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
)

// buildExport builds an EXPORT statement.
func (b *Builder) buildExport(export *tree.Export, inScope *scope) (outScope *scope) {
	// We don't allow the input statement to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	emptyScope := b.allocScope()
	inputScope := b.buildStmt(export.Query, nil /* desiredTypes */, emptyScope)

	texpr := emptyScope.resolveType(export.File, types.String)
	fileName := b.buildScalar(
		texpr, emptyScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
	)

	admin, err := b.catalog.HasAdminRole(b.ctx)
	if err != nil {
		panic(err)
	}
	if !admin {
		destinationURI, err := texpr.Eval(b.evalCtx)
		if err != nil {
			panic(err)
		}
		hasExplicitAuth, _, err := cloud.AccessIsWithExplicitAuth(
			string(tree.MustBeDString(destinationURI)))
		if err != nil {
			panic(err)
		}
		if !hasExplicitAuth {
			panic(pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to EXPORT to the specified URI"))
		}
	}

	options := b.buildKVOptions(export.Options, emptyScope)

	outScope = inScope.push()
	b.synthesizeResultColumns(outScope, colinfo.ExportColumns)
	outScope.expr = b.factory.ConstructExport(
		inputScope.expr.(memo.RelExpr),
		fileName,
		options,
		&memo.ExportPrivate{
			FileFormat: export.FileFormat,
			Columns:    colsToColList(outScope.cols),
			Props:      inputScope.makePhysicalProps(),
		},
	)
	return outScope
}

func (b *Builder) buildKVOptions(opts tree.KVOptions, inScope *scope) memo.KVOptionsExpr {
	res := make(memo.KVOptionsExpr, len(opts))
	for i := range opts {
		res[i].Key = string(opts[i].Key)
		if opts[i].Value != nil {
			texpr := inScope.resolveType(opts[i].Value, types.String)
			res[i].Value = b.buildScalar(
				texpr, inScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
			)
		} else {
			res[i].Value = b.factory.ConstructNull(types.String)
		}
	}
	return res
}
