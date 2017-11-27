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
)

type cteNameEnvironment []cteNameEnvironmentFrame

type cteNameEnvironmentFrame map[tree.Name]cteSource

type cteSource struct {
	plan planNode
	used bool
}

func (e cteNameEnvironment) push(frame cteNameEnvironmentFrame) cteNameEnvironment {
	return append(e, frame)
}

func (e cteNameEnvironment) pop() cteNameEnvironment {
	return e[:len(e)-1]
}

func (p *planner) initWith(ctx context.Context, with *tree.With) (func(), error) {
	if with != nil {
		frame := make(cteNameEnvironmentFrame)
		p.cteNameEnvironment = p.cteNameEnvironment.push(frame)
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
			frame[cte.Name.Alias] = cteSource{plan: ctePlan}
		}
		return func() { p.cteNameEnvironment = p.cteNameEnvironment.pop() }, nil
	}
	return nil, nil
}

func (p *planner) getCTEDataSource(t *tree.NormalizableTableName) (planDataSource, bool, error) {
	if p.cteNameEnvironment == nil {
		return planDataSource{}, false, nil
	}
	tn, err := t.Normalize()
	if err != nil {
		return planDataSource{}, false, err
	}
	// Iterate backward through the environment, most recent frame first.
	for i := range p.cteNameEnvironment {
		frame := p.cteNameEnvironment[len(p.cteNameEnvironment)-1-i]
		if tn.DatabaseName == "" && tn.PrefixName == "" {
			if cteSource, ok := frame[tn.TableName]; ok {
				if cteSource.used {
					// TODO(jordan): figure out how to lift this restriction.
					// CTE expressions that are used more than once will need to be
					// pre-evaluated like subqueries, I think.
					return planDataSource{}, false, pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
						"CTE clauses can currently only be used once per statement")
				}
				cteSource.used = true
				frame[tn.TableName] = cteSource
				plan := cteSource.plan
				return planDataSource{
					info: newSourceInfoForSingleTable(*tn, planColumns(plan)),
					plan: plan,
				}, true, nil
			}
		}
	}
	return planDataSource{}, false, nil
}
