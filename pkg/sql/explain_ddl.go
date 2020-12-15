package sql

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/builder"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/compiler"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
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
	b := builder.NewBuilder(params.p, params.p.SemaCtx(), params.p.EvalContext())
	var ts []targets.TargetState
	var err error
	switch n := params.p.stmt.AST.(*tree.Explain).Statement.(type) {
	case *tree.AlterTable:
		ts, err = b.AlterTable(params.ctx, params.extendedEvalCtx.SchemaChangerState.targetStates, n)
	default:

	}
	if err != nil {
		return err
	}
	sc, err := compiler.Compile(ts, compiler.CompileFlags{
		ExecutionPhase: compiler.PostCommitPhase,
		// TODO(ajwerner): Populate created descriptors.
	})
	if err != nil {
		return err
	}
	var out string
	if n.options.Flags[tree.ExplainFlagDeps] {
		if out, err = sc.DrawDepGraph(); err != nil {
			return err
		}
	} else {
		if out, err = sc.DrawStageGraph(); err != nil {
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
		Host:     "ajwerner.github.io",
		Path:     "schemachangeviz/index.html",
		RawQuery: "graph=" + url.QueryEscape(base64.StdEncoding.EncodeToString(buf.Bytes())),
	}).String()

	n.values = tree.Datums{
		tree.NewDString(vizURL),
	}
	return nil
}
