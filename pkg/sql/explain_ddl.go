// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type explainDDLNode struct {
	optColumnsSlot
	options *tree.ExplainOptions
	plan    planComponents
	run     bool
	values  tree.Datums
}

func (n *explainDDLNode) Next(params runParams) (bool, error) {
	if n.run {
		return false, nil
	}
	n.run = true
	return true, nil
}

func (n *explainDDLNode) Values() tree.Datums {
	return n.values
}

func (n *explainDDLNode) Close(ctx context.Context) {
}

var _ planNode = (*explainDDLNode)(nil)

var explainNotPossibleError = pgerror.New(pgcode.FeatureNotSupported,
	"cannot explain a statement which is not supported by the declarative schema changer")

func (n *explainDDLNode) startExec(params runParams) error {
	// TODO(postamar): better error messages for each error case
	scNodes, ok := n.plan.main.planNode.(*schemaChangePlanNode)
	if !ok {
		if n.plan.main.physPlan == nil {
			return explainNotPossibleError
		} else if len(n.plan.main.physPlan.planNodesToClose) > 0 {
			scNodes, ok = n.plan.main.physPlan.planNodesToClose[0].(*schemaChangePlanNode)
			if !ok {
				return explainNotPossibleError
			}
		} else {
			return explainNotPossibleError
		}
	}
	sc, err := scplan.MakePlan(scNodes.plannedState, scplan.Params{
		ExecutionPhase:             scop.StatementPhase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return 1 },
	})
	if err != nil {
		return errors.WithAssertionFailure(err)
	}
	var vizURL string
	if n.options.Flags[tree.ExplainFlagDeps] {
		if vizURL, err = sc.DependenciesURL(); err != nil {
			return errors.WithAssertionFailure(err)
		}
	} else {
		if vizURL, err = sc.StagesURL(); err != nil {
			return errors.WithAssertionFailure(err)
		}
	}
	n.values = tree.Datums{
		tree.NewDString(vizURL),
	}
	return nil
}
