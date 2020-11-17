package schemachanger_test

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
)

func init() {
	schemachanger.BuilderMaker = func(s serverutils.TestServerInterface) (_ *schemachanger.Builder, p interface{}, cleanup func()) {
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
		ip, cleanup := sql.NewInternalPlanner(
			"foo",
			kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
			security.RootUserName(),
			&sql.MemoryMetrics{},
			&execCfg,
			sessiondatapb.SessionData{},
		)
		planner := ip.(interface {
			resolver.SchemaResolver
			SemaCtx() *tree.SemaContext
			EvalContext() *tree.EvalContext
		})
		return schemachanger.NewBuilder(
			planner, planner.SemaCtx(), planner.EvalContext(),
		), planner, cleanup
	}
}
