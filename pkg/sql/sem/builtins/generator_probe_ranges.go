// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

func init() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range probeRangesGenerators {
		for _, g := range v.overloads {
			if g.Class != tree.GeneratorClass {
				panic(errors.AssertionFailedf("generator functions should be marked with the tree.GeneratorClass "+
					"function class, found %v", v))
			}
		}
		registerBuiltin(k, v)
	}
}

var probeRangesGenerators = map[string]builtinDefinition{
	"crdb_internal.probe_ranges": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemInfo,
			Undocumented: true,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "timeout", Typ: types.Interval},
				{Name: "probe_type", Typ: makeEnum()},
			},
			probeRangeGeneratorType,
			makeProbeRangeGenerator,
			`Returns rows of range data based on the results received when using the prober.
			Parameters
				timeout: interval for the maximum time the user wishes the prober to probe a range.
				probe_type: enum indicating which kind of probe the prober should conduct (options are read or write).
			Example usage
				number of failed write probes: select count(1) from crdb_internal.probe_ranges(INTERVAL '1000ms', 'write') where error != '';
				50 slowest probes: select range_id, error, end_to_end_latency_ms from crdb_internal.probe_ranges(INTERVAL '1000ms', true) order by end_to_end_latency_ms desc limit 50;
			Notes
				If a probe should fail, the latency will be set to MaxInt64 in order to naturally sort above other latencies.
				Read probes are cheaper than write probes. If write probes have already ran, it's not necessary to also run a read probe.
				A write probe will effectively probe reads as well.
			`,
			volatility.Volatile,
		),
	),
}

func makeEnum() *types.T {
	enumMembers := []string{"read", "write"}
	enumType := types.MakeEnum(catid.TypeIDToOID(500), catid.TypeIDToOID(100500))
	enumType.TypeMeta = types.UserDefinedTypeMetadata{
		EnumData: &types.EnumMetadata{
			LogicalRepresentations: enumMembers,
			PhysicalRepresentations: [][]byte{
				{0x52, 0x45, 0x41, 0x44},
				{0x57, 0x52, 0x49, 0x54, 0x45},
			},
			IsMemberReadOnly: make([]bool, len(enumMembers)),
		},
	}
	return enumType
}

// probeRangeTypesGenerator supports the execution of
// crdb_internal.probe_range(timeout, type).

var probeRangeGeneratorLabels = []string{"range_id", "error", "end_to_end_latency_ms", "verbose_trace"}

var probeRangeGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.String, types.Int, types.String},
	probeRangeGeneratorLabels,
)

type probeRangeRow struct {
	rangeID      int64
	error        string
	latency      time.Duration
	verboseTrace string
}

type probeRangeGenerator struct {
	rangeProber eval.RangeProber
	timeout     time.Duration
	isWrite     bool
	tracer      *tracing.Tracer

	// The below are updated during calls to Next() throughout the lifecycle of
	// probeRangeGenerator.
	curr   probeRangeRow
	ranges []kv.KeyValue
}

func makeProbeRangeGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	// The user must be an admin to use this builtin.
	isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
	if err != nil {
		return nil, err
	}
	if !isAdmin {
		return nil, pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"only users with the admin role are allowed to use crdb_internal.probe_range",
		)
	}
	// Trace the query to meta2. Return it as part of the error string if the query fails.
	// This improves observability into a meta2 outage. We expect crdb_internal.probe_range
	// to be available, unless meta2 is down.
	var ranges []kv.KeyValue
	{
		ctx, sp := tracing.EnsureChildSpan(
			ctx, evalCtx.Tracer, "meta2scan",
			tracing.WithRecording(tracingpb.RecordingVerbose),
		)
		defer sp.Finish()
		// Handle args passed in.
		ranges, err = kvclient.ScanMetaKVs(ctx, evalCtx.Txn, roachpb.Span{
			Key:    keys.MinKey,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			return nil, errors.WithDetailf(
				errors.Wrapf(err, "error scanning meta ranges"),
				"trace:\n%s", sp.GetConfiguredRecording())
		}
	}
	timeout := time.Duration(tree.MustBeDInterval(args[0]).Duration.Nanos())
	isWrite := args[1].(*tree.DEnum).LogicalRep
	return &probeRangeGenerator{
		rangeProber: evalCtx.RangeProber,
		timeout:     timeout,
		isWrite:     isWrite == "write",
		tracer:      evalCtx.Tracer,
		ranges:      ranges,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (p *probeRangeGenerator) ResolvedType() *types.T {
	return probeRangeGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (p *probeRangeGenerator) Start(_ context.Context, _ *kv.Txn) error {
	return nil
}

// Next implements the tree.ValueGenerator interface.
func (p *probeRangeGenerator) Next(ctx context.Context) (bool, error) {
	if len(p.ranges) == 0 {
		return false, nil
	}
	rawKV := p.ranges[0]
	p.ranges = p.ranges[1:]
	p.curr = probeRangeRow{}

	var opName string
	if p.isWrite {
		opName = "write probe"
	} else {
		opName = "read probe"
	}
	ctx, sp := tracing.EnsureChildSpan(
		ctx, p.tracer, opName,
		tracing.WithRecording(tracingpb.RecordingVerbose),
	)
	defer func() {
		p.curr.verboseTrace = sp.FinishAndGetConfiguredRecording().String()
	}()

	tBegin := timeutil.Now()
	err := contextutil.RunWithTimeout(ctx, opName, p.timeout, func(ctx context.Context) error {
		var desc roachpb.RangeDescriptor
		if err := rawKV.ValueProto(&desc); err != nil {
			// NB: on error, p.curr.rangeID == 0.
			return err
		}
		p.curr.rangeID = int64(desc.RangeID)
		key := desc.StartKey.AsRawKey()
		if desc.RangeID == 1 {
			// The first range starts at KeyMin, but the replicated keyspace starts only at keys.LocalMax,
			// so there is a special case here.
			key = keys.LocalMax
		}
		return p.rangeProber.RunProbe(ctx, key, p.isWrite)
	})

	p.curr.latency = timeutil.Since(tBegin)
	if err != nil {
		p.curr.error = err.Error()
		p.curr.latency = math.MaxInt64
	}

	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (p *probeRangeGenerator) Values() (tree.Datums, error) {
	return tree.Datums{
		tree.NewDInt(tree.DInt(p.curr.rangeID)),
		tree.NewDString(p.curr.error),
		tree.NewDInt(tree.DInt(p.curr.latency.Milliseconds())),
		tree.NewDString(p.curr.verboseTrace),
	}, nil
}

// Close implements the tree.ValueGenerator interface.
func (p *probeRangeGenerator) Close(_ context.Context) {}
