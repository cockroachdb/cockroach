// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scplan

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/current"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_24_3"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/release_25_1"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scstage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Params holds the arguments for planning.
type Params struct {
	// context.Context for planning.
	Ctx context.Context

	// ActiveVersion contains the version currently active in the cluster.
	ActiveVersion clusterversion.ClusterVersion

	// InRollback is used to indicate whether we've already been reverted.
	// Note that when in rollback, there is no turning back and all work is
	// non-revertible. Theory dictates that this is fine because of how we
	// had carefully crafted stages to only allow entering rollback while it
	// remains safe to do so.
	InRollback bool

	// ExecutionPhase indicates the phase that the plan should be constructed for.
	ExecutionPhase scop.Phase

	// SchemaChangerJobIDSupplier is used to return the JobID for a
	// job if one should exist.
	SchemaChangerJobIDSupplier func() jobspb.JobID

	// SkipPlannerSanityChecks, if false, strictly enforces sanity checks in the
	// declarative schema changer planner.
	SkipPlannerSanityChecks bool

	// MemAcc returns the injected bound account that
	// tracks memory allocations that long-live `scplan.MakePlan` function.
	// It is currently only used to track memory allocation for EXPLAIN(DDL)
	// output, as it's considered a continuation of the planning process.
	MemAcc *mon.BoundAccount
}

// Exported internal types
type (
	// Graph is an exported alias of scgraph.Graph.
	Graph = scgraph.Graph

	// Stage is an exported alias of scstage.Stage.
	Stage = scstage.Stage
)

// A Plan is a schema change plan, primarily containing ops to be executed that
// are partitioned into stages.
type Plan struct {
	scpb.CurrentState
	Params Params
	Graph  *scgraph.Graph
	JobID  jobspb.JobID
	Stages []Stage
}

// StagesForCurrentPhase returns the stages in the execution phase specified in
// the plan params.
func (p Plan) StagesForCurrentPhase() []scstage.Stage {
	for i, s := range p.Stages {
		if s.Phase > p.Params.ExecutionPhase {
			return p.Stages[:i]
		}
	}
	return p.Stages
}

// MakePlan generates a Plan for a particular phase of a schema change, given
// the initial state for a set of targets. Returns an error when planning fails.
func MakePlan(ctx context.Context, initial scpb.CurrentState, params Params) (p Plan, err error) {
	defer scerrors.StartEventf(
		ctx,
		0, /* level */
		"building declarative schema changer plan in %s (rollback=%v) for %s",
		redact.Safe(params.ExecutionPhase),
		redact.Safe(params.InRollback),
		redact.Safe(initial.StatementTags()),
	).HandlePanicAndLogError(ctx, &err)
	p = Plan{
		CurrentState: initial,
		Params:       params,
	}
	err = makePlan(ctx, &p)
	if err != nil && ctx.Err() == nil {
		err = p.DecorateErrorWithPlanDetails(err)
	}
	return p, err
}

func makePlan(ctx context.Context, p *Plan) (err error) {
	{
		start := timeutil.Now()
		// Generate the graph used to build the stages.
		p.Graph = buildGraph(ctx, p.Params.ActiveVersion, p.CurrentState)
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "graph generation took %v", timeutil.Since(start))
		}
	}
	{
		start := timeutil.Now()
		p.Stages = scstage.BuildStages(
			ctx,
			p.CurrentState,
			p.Params.ExecutionPhase,
			p.Graph,
			p.Params.SchemaChangerJobIDSupplier,
			!p.Params.SkipPlannerSanityChecks,
		)
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "stage generation took %v", timeutil.Since(start))
		}
	}
	if n := len(p.Stages); n > 0 && p.Stages[n-1].Phase > scop.PreCommitPhase {
		// Only get the job ID if it's actually been assigned already.
		p.JobID = p.Params.SchemaChangerJobIDSupplier()
	}
	if err := scstage.ValidateStages(p.TargetState, p.Stages, p.Graph); err != nil {
		panic(errors.Wrapf(err, "invalid execution plan"))
	}
	return nil
}

// rulesForReleaseInfo tracks the active version and registry for
// each given release.
type rulesForRelease struct {
	activeVersion clusterversion.Key
	rulesRegistry *rules.Registry
}

// rulesForRelease supported rules for each release, this is an ordered array
// with the newest supported version first.
var rulesForReleases = []rulesForRelease{
	{activeVersion: clusterversion.Latest, rulesRegistry: current.GetRegistry()},
	{activeVersion: clusterversion.V25_1, rulesRegistry: release_25_1.GetRegistry()},
	{activeVersion: clusterversion.V24_3, rulesRegistry: release_24_3.GetRegistry()},
}

// minVersionForRules the oldest version supported by the rules.
var minVersionForRules = rulesForReleases[len(rulesForReleases)-1].activeVersion

// GetRulesRegistryForRelease returns the rules registry based on the current
// active version of cockroach. In a mixed version state, it's possible for the state
// generated by a newer version of cockroach to be incompatible with old versions.
// For example dependent objects or combinations of them in a partially executed,
// plan may reach states where older versions of cockroach may not be able
// to plan further (and vice versa). To eliminate the possibility of these issues, we
// will plan with the set of rules belonging to the currently active version. One
// example of this is the dependency between index name and secondary indexes
// is more relaxed on 23.1 vs 22.2, which can lead to scenarios where the index
// name may become public before the index is public (which was disallowed on older
// versions).
func GetRulesRegistryForRelease(
	_ context.Context, activeVersion clusterversion.ClusterVersion,
) *rules.Registry {
	for _, r := range rulesForReleases {
		if activeVersion.IsActive(r.activeVersion) {
			return r.rulesRegistry
		}
	}
	return nil
}

// GetReleasesForRulesRegistries returns all the supported version
// numbers within the rule's registry.
func GetReleasesForRulesRegistries() []clusterversion.ClusterVersion {
	supportedVersions := make([]clusterversion.ClusterVersion, 0, len(rulesForReleases))
	for _, r := range rulesForReleases {
		supportedVersions = append(supportedVersions,
			clusterversion.ClusterVersion{
				Version: r.activeVersion.Version(),
			})
	}
	return supportedVersions
}

// getMinValidVersionForRules this returns either the current active version,
// or the minimum version for the rules tht we support (which ever is greater)
func getMinValidVersionForRules(
	ctx context.Context, activeVersion clusterversion.ClusterVersion,
) clusterversion.ClusterVersion {
	if !activeVersion.IsActive(minVersionForRules) {
		log.Warningf(ctx, "falling back to rules for minimum version (%v),"+
			"active version was : %v",
			minVersionForRules,
			activeVersion)
		return clusterversion.ClusterVersion{
			Version: minVersionForRules.Version(),
		}
	}
	return activeVersion
}

func applyDepRules(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, g *scgraph.Graph,
) error {
	activeVersion = getMinValidVersionForRules(ctx, activeVersion)
	registry := GetRulesRegistryForRelease(ctx, activeVersion)
	return registry.ApplyDepRules(ctx, g)
}

func buildGraph(
	ctx context.Context, activeVersion clusterversion.ClusterVersion, cs scpb.CurrentState,
) *scgraph.Graph {
	g, err := opgen.BuildGraph(ctx, activeVersion, cs)
	if err != nil {
		panic(errors.Wrapf(err, "build graph op edges"))
	}
	err = applyDepRules(ctx, activeVersion, g)
	if err != nil {
		panic(errors.Wrapf(err, "build graph dep edges"))
	}
	err = g.Validate()
	if err != nil {
		panic(errors.Wrapf(err, "validate graph"))
	}
	return g
}
