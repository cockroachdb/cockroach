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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

func (n *explainDDLNode) startExec(params runParams) error {
	scNodes, ok := n.plan.main.planNode.(*schemaChangePlanNode)
	if !ok {
		if n.plan.main.physPlan == nil {
			return pgerror.New(pgcode.FeatureNotSupported, "cannot explain a non-schema change statement\n")
		} else if len(n.plan.main.physPlan.planNodesToClose) > 0 {
			scNodes, ok = n.plan.main.physPlan.planNodesToClose[0].(*schemaChangePlanNode)
			if !ok {
				return pgerror.New(pgcode.FeatureNotSupported, "cannot explain a non-schema change statement\n")

			}
		} else {
			return pgerror.New(pgcode.FeatureNotSupported, "cannot explain a non-schema change statement\n")
		}
	}
	sc, err := scplan.MakePlan(scNodes.plannedState, scplan.Params{
		ExecutionPhase: scplan.PostCommitPhase,
		// TODO(ajwerner): Populate created descriptors.
	})
	if err != nil {
		return pgerror.Wrap(err, pgcode.FeatureNotSupported, "new schema changer failed executing this operation.")
	}
	var out string
	if n.options.Flags[tree.ExplainFlagDeps] {
		if out, err = scgraphviz.DrawDependencies(&sc); err != nil {
			return err
		}
	} else {
		if out, err = scgraphviz.DrawStages(&sc); err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := io.WriteString(w, out); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	vizURL := (&url.URL{
		Scheme:   "https",
		Host:     "cockroachdb.github.io",
		Path:     "scplan/viz.html",
		Fragment: base64.StdEncoding.EncodeToString(buf.Bytes()),
	}).String()
	n.values = tree.Datums{
		tree.NewDString(vizURL),
	}
	return nil
}
