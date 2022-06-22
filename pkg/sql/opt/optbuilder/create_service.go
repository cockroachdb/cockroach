// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// buildCreateService constructs a CreateService operator based on the CREATE SERVICE
// statement.
func (b *Builder) buildCreateService(cs *tree.CreateService, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true

	var input memo.RelExpr
	var inputCols physical.Presentation
	if cs.Rules != nil {
		// The execution code might need to stringify the query to run it
		// asynchronously. For that we need the data sources to be fully qualified.
		// TODO(radu): this interaction is pretty hacky, investigate moving the
		// generation of the string to the optimizer.
		b.qualifyDataSourceNamesInAST = true
		defer func() {
			b.qualifyDataSourceNamesInAST = false
		}()

		// Build the input query.
		outScope = b.buildStmtAtRoot(cs.Rules, nil /* desiredTypes */)

		input = outScope.expr
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	options := b.buildKVOptions(cs.DefaultOptions, b.allocScope())

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateService(
		input,
		options,
		&memo.CreateServicePrivate{
			InputCols: inputCols,
			Syntax:    cs,
		},
	)
	return outScope
}

// buildAlterService constructs an AlterService operator based on the ALTER SERVICE
// statement.
func (b *Builder) buildAlterService(as *tree.AlterService, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true

	var input memo.RelExpr
	var inputCols physical.Presentation
	if asr, ok := as.Cmd.(*tree.AlterServiceRules); ok && asr.Rules != nil {
		// The execution code might need to stringify the query to run it
		// asynchronously. For that we need the data sources to be fully qualified.
		// TODO(radu): this interaction is pretty hacky, investigate moving the
		// generation of the string to the optimizer.
		b.qualifyDataSourceNamesInAST = true
		defer func() {
			b.qualifyDataSourceNamesInAST = false
		}()

		// Build the input query.
		outScope = b.buildStmtAtRoot(asr.Rules, nil /* desiredTypes */)

		input = outScope.expr
		inputCols = outScope.makePhysicalProps().Presentation
	} else {
		// Create dummy empty input.
		input = b.factory.ConstructZeroValues()
	}

	var options memo.KVOptionsExpr
	if asr, ok := as.Cmd.(*tree.AlterServiceSetDefaults); ok {
		options = b.buildKVOptions(asr.Opts, b.allocScope())
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructAlterService(
		input,
		options,
		&memo.AlterServicePrivate{
			InputCols: inputCols,
			Syntax:    as,
		},
	)
	return outScope
}
