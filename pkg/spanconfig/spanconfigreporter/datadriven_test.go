// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigreporter_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigreporter"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a data-driven test for spanconfig.Reporter. It offers
// the following commands:
//
//	init
//	n1: attr-key=attr-value
//	r1: [a,b)
//	----
//
//	liveness
//	n1: live|dead
//	----
//
//	allocate
//	r1: voters=[n1,n2] nonvoters=[n3]
//	----
//
//	configure
//	[a,b): num_replicas=3 constraints='...' voter_constraints='...'
//	----
//
//	report
//	[a,b)
//	[c,d)
//	----
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := clustersettings.MakeTestingClusterSettings()
	scKnobs := &spanconfig.TestingKnobs{}

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		cluster := newMockCluster(t, st, scKnobs)
		reporter := spanconfigreporter.New(cluster.liveness, cluster, cluster, cluster, st, scKnobs)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					parts := strings.Split(line, ":")
					require.Len(t, parts, 2)
					id, data := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
					switch {
					case strings.HasPrefix(id, "n"): // node
						nodeID := spanconfigtestutils.ParseNodeID(t, id)
						locality := &roachpb.Locality{}
						if data != "" {
							require.NoError(t, locality.Set(data))
						}
						cluster.addNode(roachpb.NodeDescriptor{
							NodeID:   nodeID,
							Locality: *locality,
						})
					case strings.HasPrefix(id, "r"): // range
						rangeID := spanconfigtestutils.ParseRangeID(t, id)
						span := spanconfigtestutils.ParseSpan(t, data)
						cluster.addRange(roachpb.RangeDescriptor{
							RangeID:  rangeID,
							StartKey: roachpb.RKey(span.Key),
							EndKey:   roachpb.RKey(span.EndKey),
						})
					default:
						t.Fatalf("malformed line %q, expected to find 'n' or 'r' prefix", line)
					}
				}

			case "liveness":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					parts := strings.Split(line, ":")
					require.Len(t, parts, 2)
					id, data := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
					if data != "live" && data != "dead" {
						t.Fatalf("malformed line %q, expected to find 'live' or 'dead' annotation", line)
					}
					nodeID := spanconfigtestutils.ParseNodeID(t, id)
					cluster.markLive(nodeID, data == "live")
				}

			case "allocate":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					parts := strings.Split(line, ":")
					require.Len(t, parts, 2)
					id, data := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
					rangeID := spanconfigtestutils.ParseRangeID(t, id)
					desc := cluster.getRangeDescriptor(rangeID)
					desc.SetReplicas(spanconfigtestutils.ParseReplicaSet(t, data))
					cluster.setRangeDescriptor(desc)
				}

			case "configure":
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					tag, data, found := strings.Cut(line, ":")
					require.True(t, found)
					tag, data = strings.TrimSpace(tag), strings.TrimSpace(data)
					span := spanconfigtestutils.ParseSpan(t, tag)
					conf := spanconfigtestutils.ParseZoneConfig(t, data).AsSpanConfig()
					cluster.applyConfig(ctx, span, conf)
				}

			case "report":
				var spans []roachpb.Span
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}
					spans = append(spans, spanconfigtestutils.ParseSpan(t, line))
				}
				if len(spans) == 0 {
					spans = append(spans, keys.EverythingSpan)
				}
				report, err := reporter.SpanConfigConformance(ctx, spans)
				require.NoError(t, err)
				printRangeDesc := func(r roachpb.RangeDescriptor) string {
					var buf strings.Builder
					buf.WriteString(fmt.Sprintf("r%d:", r.RangeID))
					buf.WriteString(r.RSpan().String())
					buf.WriteString(" [")
					if allReplicas := r.Replicas().Descriptors(); len(allReplicas) > 0 {
						for i, rep := range allReplicas {
							if i > 0 {
								buf.WriteString(", ")
							}
							buf.WriteString(rep.String())
						}
					} else {
						buf.WriteString("<no replicas>")
					}
					buf.WriteString("]")
					return buf.String()
				}
				printList := func(tag string, ranges []roachpb.ConformanceReportedRange) string {
					var buf strings.Builder
					for i, r := range ranges {
						if i == 0 {
							buf.WriteString(fmt.Sprintf("%s:\n", tag))
						}
						buf.WriteString(fmt.Sprintf("  %s applying %s\n", printRangeDesc(r.RangeDescriptor),
							spanconfigtestutils.PrintSpanConfigDiffedAgainstDefaults(r.Config)))
					}
					return buf.String()
				}
				var buf strings.Builder
				buf.WriteString(printList("unavailable", report.Unavailable))
				buf.WriteString(printList("under replicated", report.UnderReplicated))
				buf.WriteString(printList("over replicated", report.OverReplicated))
				buf.WriteString(printList("violating constraints", report.ViolatingConstraints))
				if buf.Len() == 0 {
					return "ok"
				}
				return buf.String()

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		})
	})
}

type mockCluster struct {
	t *testing.T

	nodes    map[roachpb.NodeID]roachpb.NodeDescriptor
	ranges   map[roachpb.RangeID]roachpb.RangeDescriptor
	liveness livenesspb.TestNodeVitality
	store    *spanconfigstore.Store
}

var _ constraint.StoreResolver = &mockCluster{}
var _ rangedesc.Scanner = &mockCluster{}
var _ spanconfig.StoreReader = &mockCluster{}

func newMockCluster(
	t *testing.T, st *clustersettings.Settings, scKnobs *spanconfig.TestingKnobs,
) *mockCluster {
	return &mockCluster{
		t:        t,
		nodes:    make(map[roachpb.NodeID]roachpb.NodeDescriptor),
		ranges:   make(map[roachpb.RangeID]roachpb.RangeDescriptor),
		liveness: livenesspb.TestCreateNodeVitality(),
		store: spanconfigstore.New(
			roachpb.TestingDefaultSpanConfig(),
			st,
			spanconfigstore.NewEmptyBoundsReader(),
			scKnobs,
		),
	}
}

// GetStoreDescriptor implements constraint.StoreResolver.
func (s *mockCluster) GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool) {
	desc, found := s.nodes[roachpb.NodeID(storeID)]
	require.True(s.t, found, "undeclared node n%d", storeID)

	return roachpb.StoreDescriptor{
		StoreID: storeID, // simulate storeIDs == nodeIDs
		Node:    desc,
	}, true
}

// Scan implements rangedesc.Scanner interface.
func (s *mockCluster) Scan(
	_ context.Context, _ int, _ func(), _ roachpb.Span, fn func(...roachpb.RangeDescriptor) error,
) error {
	var descs []roachpb.RangeDescriptor
	for _, d := range s.ranges {
		descs = append(descs, d)
	}
	sort.Slice(descs, func(i, j int) bool {
		return descs[i].StartKey.Less(descs[j].StartKey)
	})
	return fn(descs...)
}

// NeedsSplit implements spanconfig.StoreReader.
func (s *mockCluster) NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error) {
	return s.store.NeedsSplit(ctx, start, end)
}

// ComputeSplitKey implements spanconfig.StoreReader.
func (s *mockCluster) ComputeSplitKey(
	ctx context.Context, start, end roachpb.RKey,
) (roachpb.RKey, error) {
	return s.store.ComputeSplitKey(ctx, start, end)
}

// GetSpanConfigForKey implements spanconfig.StoreReader.
func (s *mockCluster) GetSpanConfigForKey(
	ctx context.Context, key roachpb.RKey,
) (roachpb.SpanConfig, roachpb.Span, error) {
	return s.store.GetSpanConfigForKey(ctx, key)
}

func (s *mockCluster) addNode(desc roachpb.NodeDescriptor) {
	_, found := s.nodes[desc.NodeID]
	require.Falsef(s.t, found, "attempting to re-add n%d", desc.NodeID)
	s.nodes[desc.NodeID] = desc
	s.liveness.AddNode(desc.NodeID)
	s.markLive(desc.NodeID, true /* live */)
}

func (s *mockCluster) markLive(id roachpb.NodeID, live bool) {
	_, found := s.nodes[id]
	require.Truef(s.t, found, "n%d not found", id)

	if live {
		s.liveness.RestartNode(id)
	} else {
		s.liveness.DownNode(id)
	}
}

func (s *mockCluster) addRange(desc roachpb.RangeDescriptor) {
	_, found := s.ranges[desc.RangeID]
	require.Falsef(s.t, found, "attempting to re-add r%d", desc.RangeID)
	s.ranges[desc.RangeID] = desc
}

func (s *mockCluster) setRangeDescriptor(desc roachpb.RangeDescriptor) {
	_, found := s.ranges[desc.RangeID]
	require.Truef(s.t, found, "r%d not found", desc.RangeID)
	s.ranges[desc.RangeID] = desc
}

func (s *mockCluster) getRangeDescriptor(id roachpb.RangeID) roachpb.RangeDescriptor {
	desc, found := s.ranges[id]
	require.Truef(s.t, found, "r%d not found", id)
	return desc
}

func (s *mockCluster) applyConfig(ctx context.Context, span roachpb.Span, conf roachpb.SpanConfig) {
	update, err := spanconfig.Addition(spanconfig.MakeTargetFromSpan(span), conf)
	require.NoError(s.t, err)
	s.store.Apply(ctx, update)
}
