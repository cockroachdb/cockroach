// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// This file contains the implementation of common table expressions. See
// docs/RFCS/20171206_single_use_common_table_expressions.md for more details.
//
// A common table expression is essentially a binding from user-defined name to
// to the results of a statement that returns table data. In order to allow
// statements to refer to these names, we must maintain a naming environment
// that contains this mapping. Because a CTE can contain other CTEs, this naming
// environment needs to be a stack of environment frames, each of which have a
// name mapping.
//
// An environment frame is pushed onto the stack every time a new WITH clause
// is entered. Most CTE queries only have one WITH, at the top of the statement,
// so in most cases the stack will be only 1 frame deep.
//
// Resolving a CTE name works by iterating through the stack from the top down
// until the name is found.

// cteNameEnvironment is the stack of environment frames.
type cteNameEnvironment []cteNameEnvironmentFrame

// cteNameEnvironmentFrame is a map from CTE name to datasource.
type cteNameEnvironmentFrame map[tree.Name]cteSource

// cteSource is the value part of an entry in an environment frame. It holds
// the plan that will be used to retrieve the data that's named by the CTE.
type cteSource struct {
	plan planNode
	// used is set to true if this CTE has been used as a statement source. It's
	// only around to prevent multiple use of a CTE, which is currently not
	// supported.
	used bool
	// alias holds the name of the CTE and the renaming of its columns, if
	// present.
	alias tree.AliasClause
}

func (e cteNameEnvironment) push(frame cteNameEnvironmentFrame) cteNameEnvironment {
	return append(e, frame)
}

func (e cteNameEnvironment) pop() cteNameEnvironment {
	return e[:len(e)-1]
}

func popCteNameEnvironment(p *planner) {
	p.curPlan.cteNameEnvironment = p.curPlan.cteNameEnvironment.pop()
}

// initWith pushes a new environment frame onto the planner's CTE name
// environment, with all of the CTE clauses defined in the given tree.With.
// It returns a resetter function that must be called once the enclosing scope
// is finished resolving names, which pops the environment frame.
func (p *planner) initWith(ctx context.Context, with *tree.With) (func(p *planner), error) {
	if with != nil {
		frame := make(cteNameEnvironmentFrame)
		p.curPlan.cteNameEnvironment = p.curPlan.cteNameEnvironment.push(frame)
		for _, cte := range with.CTEList {
			if _, ok := frame[cte.Name.Alias]; ok {
				return nil, pgerror.NewErrorf(
					pgerror.CodeDuplicateAliasError,
					"WITH query name %s specified more than once",
					cte.Name.Alias)
			}
			ctePlan, err := p.newPlan(ctx, cte.Stmt, nil)
			if err != nil {
				return nil, err
			}
			frame[cte.Name.Alias] = cteSource{plan: ctePlan, alias: cte.Name}
		}
		return popCteNameEnvironment, nil
	}
	return nil, nil
}

// getCTEDataSource looks up the table name in the planner's CTE name
// environment, returning the planDataSource corresponding to the CTE if it was
// found. The second return parameter returns true if a CTE was found.
func (p *planner) getCTEDataSource(tn *tree.TableName) (planDataSource, bool, error) {
	if p.curPlan.cteNameEnvironment == nil {
		return planDataSource{}, false, nil
	}
	if tn.ExplicitSchema {
		// If the name was prefixed, it cannot be a CTE.
		return planDataSource{}, false, nil
	}
	// Iterate backward through the environment, most recent frame first.
	for i := range p.curPlan.cteNameEnvironment {
		frame := p.curPlan.cteNameEnvironment[len(p.curPlan.cteNameEnvironment)-1-i]
		if cteSource, ok := frame[tn.TableName]; ok {
			if cteSource.used {
				// TODO(jordan): figure out how to lift this restriction.
				// CTE expressions that are used more than once will need to be
				// pre-evaluated like subqueries, I think.
				return planDataSource{}, false, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
					"unsupported multiple use of CTE clause %q", tree.ErrString(tn))
			}
			cteSource.used = true
			frame[tn.TableName] = cteSource
			plan := cteSource.plan
			cols := planColumns(plan)
			if len(cols) == 0 {
				return planDataSource{}, false, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
					"WITH clause %q does not have a RETURNING clause", tree.ErrString(tn))
			}
			dataSource := planDataSource{
				info: sqlbase.NewSourceInfoForSingleTable(*tn, planColumns(plan)),
				plan: plan,
			}
			var err error
			dataSource, err = renameSource(dataSource, cteSource.alias, false)
			return dataSource, err == nil, err
		}
	}
	return planDataSource{}, false, nil
}
