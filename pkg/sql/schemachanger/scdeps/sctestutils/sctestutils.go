// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctestutils

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/faketreeeval"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/scviz"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/kylelemons/godebug/diff"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// WithBuilderDependenciesFromTestServer sets up and tears down an
// scbuild.Dependencies object built using the test server interface and which
// it passes to the callback.
func WithBuilderDependenciesFromTestServer(
	s serverutils.TestServerInterface, fn func(scbuild.Dependencies),
) {
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	ip, cleanup := sql.NewInternalPlanner(
		"test",
		kv.NewTxn(context.Background(), s.DB(), s.NodeID()),
		username.RootUserName(),
		&sql.MemoryMetrics{},
		&execCfg,
		// Setting the database on the session data to "defaultdb" in the obvious
		// way doesn't seem to do what we want.
		sessiondatapb.SessionData{},
	)
	defer cleanup()
	planner := ip.(interface {
		InternalSQLTxn() descs.Txn
		Descriptors() *descs.Collection
		SessionData() *sessiondata.SessionData
		resolver.SchemaResolver
		scbuild.AuthorizationAccessor
		scbuild.AstFormatter
		scbuild.FeatureChecker
	})

	refProviderFactory, refCleanup := sql.NewReferenceProviderFactoryForTest(
		"test", planner.InternalSQLTxn().KV(), username.RootUserName(), &execCfg, "defaultdb",
	)
	defer refCleanup()

	// Use "defaultdb" as current database.
	planner.SessionData().Database = "defaultdb"
	// For setting up a builder inside tests we will ensure that the new schema
	// changer will allow non-fully implemented operations.
	planner.SessionData().NewSchemaChangerMode = sessiondatapb.UseNewSchemaChangerUnsafe
	planner.SessionData().EnableUniqueWithoutIndexConstraints = true
	fn(scdeps.NewBuilderDependencies(
		execCfg.NodeInfo.LogicalClusterID(),
		execCfg.Codec,
		planner.InternalSQLTxn(),
		sql.NewSkippingCacheSchemaResolver, /* schemaResolverFactory */
		planner,                            /* authAccessor */
		planner,                            /* astFormatter */
		planner,                            /* featureChecker */
		planner.SessionData(),
		execCfg.Settings,
		nil, /* statements */
		&faketreeeval.DummyClientNoticeSender{},
		sql.NewSchemaChangerBuildEventLogger(planner.InternalSQLTxn(), &execCfg),
		refProviderFactory,
		descidgen.NewGenerator(s.ClusterSettings(), s.Codec(), s.DB()),
	))
}

// ProtoToYAML marshals a protobuf to YAML in a roundabout way. The optional
// rewrites function can be used to modify the data being displayed. The input
// are values decoded after converting the protobuf first to json and then
// decoding that into a map[string]interface{}. The function will be called
// for every object in the decoded map recursively.
func ProtoToYAML(
	m protoutil.Message, emitDefaults bool, rewrites ...func(interface{}),
) (string, error) {
	target, err := scviz.ToMap(m, emitDefaults)
	if err != nil {
		return "", err
	}
	scviz.WalkMap(target, scviz.RewriteEmbeddedIntoParent)
	for _, rewrite := range rewrites {
		scviz.WalkMap(target, rewrite)
	}
	out, err := yaml.Marshal(target)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// DiffArgs defines arguments for the Diff function.
type DiffArgs struct {
	Indent       string
	CompactLevel uint
}

// Diff returns an edit diff by calling diff.Diff and reformatting the results.
func Diff(a, b string, args DiffArgs) string {
	d := diff.Diff(a, b)
	lines := strings.Split(d, "\n")

	visible := make(map[int]struct{})
	if args.CompactLevel > 0 {
		n := int(args.CompactLevel) - 1
		for lineno, line := range lines {
			if strings.HasPrefix(line, "+") || strings.HasPrefix(line, "-") {
				for i := lineno - n; i <= lineno+n; i++ {
					visible[i] = struct{}{}
				}
			}
		}
	}

	result := make([]string, 0, len(lines))
	skipping := false
	for lineno, line := range lines {
		if _, found := visible[lineno]; found || args.CompactLevel == 0 {
			skipping = false
			result = append(result, args.Indent+line)
		} else if !skipping {
			skipping = true
			result = append(result, args.Indent+"...")
		}
	}
	return strings.Join(result, "\n")
}

// ProtoDiff generates an indented summary of the diff between two protos'
// YAML representations. See ProtoToYAML for documentation on rewrites.
func ProtoDiff(a, b protoutil.Message, args DiffArgs, rewrites ...func(interface{})) string {
	toYAML := func(m protoutil.Message) string {
		if m == nil {
			return ""
		}
		str, err := ProtoToYAML(m, false /* emitDefaults */, rewrites...)
		if err != nil {
			panic(err)
		}
		return strings.TrimSpace(str)
	}

	return Diff(toYAML(a), toYAML(b), args)
}

// MakePlan is a convenient alternative to calling scplan.MakePlan in tests.
func MakePlan(t *testing.T, state scpb.CurrentState, phase scop.Phase) scplan.Plan {
	plan, err := scplan.MakePlan(context.Background(), state, scplan.Params{
		ActiveVersion:              clusterversion.TestingClusterVersion,
		ExecutionPhase:             phase,
		SchemaChangerJobIDSupplier: func() jobspb.JobID { return 1 },
	})
	require.NoError(t, err)
	return plan
}

// TruncateJobOps truncates really long or unstable ops details which otherwise
// get in the way of testing.
func TruncateJobOps(plan *scplan.Plan) {
	for _, s := range plan.Stages {
		for _, o := range s.ExtraOps {
			switch op := o.(type) {
			case *scop.SetJobStateOnDescriptor:
				op.State = scpb.DescriptorState{
					JobID: op.State.JobID,
				}
			case *scop.UpdateSchemaChangerJob:
				op.RunningStatus = ""
			}
		}
	}
}

// TableNameFromStmt fetch table name from a statement.
func TableNameFromStmt(stmt parser.Statement) string {
	tableName := ""
	switch node := stmt.AST.(type) {
	case *tree.CreateTable:
		tableName = node.Table.String()
	case *tree.CreateSequence:
		tableName = node.Name.String()
	case *tree.CreateView:
		tableName = node.Name.String()
	}

	return tableName
}
