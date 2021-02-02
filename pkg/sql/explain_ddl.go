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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraphviz"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
	b := scbuild.NewBuilder(params.p, params.p.SemaCtx(), params.p.EvalContext())
	var ts []*scpb.Node
	var err error
	switch n := params.p.stmt.AST.(*tree.Explain).Statement.(type) {
	case *tree.AlterTable:
		ts, err = b.AlterTable(params.ctx, params.extendedEvalCtx.SchemaChangerState.nodes, n)
	default:

	}
	if err != nil {
		return err
	}
	sc, err := scplan.MakePlan(ts, scplan.Params{
		ExecutionPhase: scplan.PostCommitPhase,
		// TODO(ajwerner): Populate created descriptors.
	})
	if err != nil {
		return err
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
