// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestValidateNoRepeatKeysInZone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		constraint    string
		expectSuccess bool
	}{
		{`["+region=us-east-1"]`, true},
		{`["+region=us-east-1", "+zone=pa"]`, true},
		{`["+region=us-east-1", "-region=us-west-1"]`, true},
		{`["+region=us-east-1", "+region=us-east-2"]`, false},
		{`["+region=us-east-1", "+zone=pa", "+region=us-west-1"]`, false},
		{`["+region=us-east-1", "-region=us-east-1"]`, false},
		{`["-region=us-east-1", "+region=us-east-1"]`, false},
		{`{"+region=us-east-1":2, "+region=us-east-2":2}`, true},
		{`{"+region=us-east-1,+region=us-west-1":2, "+region=us-east-2":2}`, false},
		{`["+x1", "+x2", "+x3"]`, true},
		{`["+x1", "+x1"]`, false},
		{`["+x1", "-x1"]`, false},
		{`["-x1", "+x1"]`, false},
	}
	validate := func(constraint []byte, expectSuccess bool) {
		var zone zonepb.ZoneConfig
		err := yaml.UnmarshalStrict(constraint, &zone)
		if err != nil {
			t.Fatal(err)
		}
		err = zonepb.ValidateNoRepeatKeysInZone(&zone)
		if err != nil && expectSuccess {
			t.Errorf("expected success for %q; got %v", constraint, err)
		} else if err == nil && !expectSuccess {
			t.Errorf("expected err for %q; got success", constraint)
		}
	}
	for _, tc := range testCases {
		validate([]byte(`constraints: `+tc.constraint), tc.expectSuccess)
		validate([]byte(`voter_constraints: `+tc.constraint), tc.expectSuccess)
	}
}

func TestValidateZoneAttrsAndLocalitiesForSecondaryTenants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	codec := keys.MakeSQLCodec(serverutils.TestTenantID())
	settings := cluster.MakeTestingClusterSettings()

	getRegions := func(ctx context.Context) (*serverpb.RegionsResponse, error) {
		return &serverpb.RegionsResponse{
			Regions: map[string]*serverpb.RegionsResponse_Region{
				"us-east1": {
					Zones: []string{"us-east1-a", "us-east1-b"},
				},
				"us-east2": {
					Zones: []string{"us-east2-a", "us-east2-b"},
				},
			},
		}, nil
	}

	testCases := []struct {
		cfg   string
		errRe string
	}{
		{
			cfg:   `constraints: ["+region=us-east1"]`,
			errRe: "",
		},
		{
			cfg:   `constraints: ["+zone=us-east2-a"]`,
			errRe: "",
		},
		{
			cfg:   `lease_preferences: [["+zone=us-east2-b"]]`,
			errRe: "",
		},
		{
			cfg:   `voter_constraints: ["+region=us-east2"]`,
			errRe: "",
		},
		{
			cfg:   `voter_constraints: ["+zone=us-east1-a"]`,
			errRe: "",
		},
		{
			cfg:   `constraints: ["+zone=does-not-exist"]`,
			errRe: `zone "does-not-exist" not found`,
		},
		{
			cfg:   `voter_constraints: ["+region=does-not-exist"]`,
			errRe: `region "does-not-exist" not found`,
		},
		{
			cfg:   `constraints: ["-zone=does-not-exist"]`,
			errRe: `zone "does-not-exist" not found`,
		},
		{
			cfg:   `voter_constraints: ["-region=does-not-exist"]`,
			errRe: `region "does-not-exist" not found`,
		},
		{
			cfg:   `constraints: ["+rack=us-east1"]`,
			errRe: `operation is disabled within a virtual cluster`,
		},
		{
			cfg:   `constraints: ["-rack=us-east1"]`,
			errRe: `operation is disabled within a virtual cluster`,
		},
		{
			cfg:   `constraints: ["+ssd"]`,
			errRe: `operation is disabled within a virtual cluster`,
		},
		{
			cfg:   `constraints: ["-ssd"]`,
			errRe: `operation is disabled within a virtual cluster`,
		},
	}

	for _, anyConstraintAllowed := range []bool{false, true} {
		sqlclustersettings.SecondaryTenantsAllZoneConfigsEnabled.Override(ctx, &settings.SV, anyConstraintAllowed)
		for _, tc := range testCases {
			var zone zonepb.ZoneConfig
			err := yaml.UnmarshalStrict([]byte(tc.cfg), &zone)
			require.NoError(t, err)

			err = validateZoneLocalitiesForSecondaryTenants(ctx, getRegions, zonepb.NewZoneConfig(), &zone, codec, settings)
			if tc.errRe == "" || (anyConstraintAllowed && strings.HasPrefix(tc.errRe, "operation is disabled within a virtual cluster")) {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.True(t, testutils.IsError(err, tc.errRe), "expected %s; got %s", tc.errRe, err.Error())
			}
		}
	}
}

func TestValidateZoneAttrsAndLocalitiesForSystemTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stores := []struct {
		nodeAttrs     []string
		storeAttrs    []string
		storeLocality []roachpb.Tier
	}{
		{
			nodeAttrs:  []string{"highcpu", "highmem"},
			storeAttrs: []string{"ssd"},
			storeLocality: []roachpb.Tier{
				{Key: "geo", Value: "us"},
				{Key: "region", Value: "us-east1"},
				{Key: "zone", Value: "us-east1-b"},
			},
		},
		{
			nodeAttrs:  []string{"lowcpu", "lowmem"},
			storeAttrs: []string{"hdd"},
			storeLocality: []roachpb.Tier{
				{Key: "geo", Value: "eu"},
				{Key: "region", Value: "eu-west1"},
				{Key: "zone", Value: "eu-west1-b"},
				{Key: "rack", Value: "17"},
			},
		},
	}

	genStatusFromStore := func(nodeAttrs []string, storeAttrs []string, storeLocality []roachpb.Tier) statuspb.NodeStatus {
		return statuspb.NodeStatus{
			StoreStatuses: []statuspb.StoreStatus{
				{
					Desc: roachpb.StoreDescriptor{
						Attrs: roachpb.Attributes{
							Attrs: storeAttrs,
						},
						Node: roachpb.NodeDescriptor{
							Attrs: roachpb.Attributes{
								Attrs: nodeAttrs,
							},
							Locality: roachpb.Locality{
								Tiers: storeLocality,
							},
						},
					},
				},
			},
		}
	}

	nodes := &serverpb.NodesResponse{}
	for _, store := range stores {
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore(store.nodeAttrs, store.storeAttrs, store.storeLocality))
	}

	// Different cluster settings to test validation behavior.
	getNodes := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		return nodes, nil
	}

	// Regressions for negative constraint validation
	singleAttrNode := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		nodes := &serverpb.NodesResponse{}
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore([]string{}, []string{"ssd"}, []roachpb.Tier{}))
		return nodes, nil
	}
	singleLocalityNode := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		nodes := &serverpb.NodesResponse{}
		nodes.Nodes = append(nodes.Nodes, genStatusFromStore([]string{}, []string{}, []roachpb.Tier{{Key: "region", Value: "us-east1"}}))
		return nodes, nil
	}

	const expectSuccess = 0
	const expectParseErr = 1
	const expectValidateErr = 2
	// TODO(aayush): Clean these tests up to be more DRY.
	for i, tc := range []struct {
		cfg       string
		expectErr int
		nodes     nodeGetter
	}{
		{`nonsense`, expectParseErr, getNodes},
		{`range_max_bytes: 100`, expectSuccess, getNodes},
		{`range_max_byte: 100`, expectParseErr, getNodes},
		{`constraints: ["+region=us-east1"]`, expectSuccess, getNodes},
		{`constraints: {"+region=us-east1": 2, "+region=eu-west1": 1}`, expectSuccess, getNodes},
		{`constraints: ["+region=us-eas1"]`, expectValidateErr, getNodes},
		{`constraints: {"+region=us-eas1": 2, "+region=eu-west1": 1}`, expectValidateErr, getNodes},
		{`constraints: {"+region=us-east1": 2, "+region=eu-wes1": 1}`, expectValidateErr, getNodes},
		{`constraints: ["+regio=us-east1"]`, expectValidateErr, getNodes},
		{`constraints: ["+rack=17"]`, expectSuccess, getNodes},
		{`constraints: ["+rack=18"]`, expectValidateErr, getNodes},
		{`constraints: ["+rach=17"]`, expectValidateErr, getNodes},
		{`constraints: ["+highcpu"]`, expectSuccess, getNodes},
		{`constraints: ["+lowmem"]`, expectSuccess, getNodes},
		{`constraints: ["+ssd"]`, expectSuccess, getNodes},
		{`constraints: ["+highcp"]`, expectValidateErr, getNodes},
		{`constraints: ["+owmem"]`, expectValidateErr, getNodes},
		{`constraints: ["+sssd"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+region=us-east1"]`, expectSuccess, getNodes},
		{`voter_constraints: {"+region=us-east1": 2, "+region=eu-west1": 1}`, expectSuccess, getNodes},
		{`voter_constraints: ["+region=us-eas1"]`, expectValidateErr, getNodes},
		{`voter_constraints: {"+region=us-eas1": 2, "+region=eu-west1": 1}`, expectValidateErr, getNodes},
		{`voter_constraints: {"+region=us-east1": 2, "+region=eu-wes1": 1}`, expectValidateErr, getNodes},
		{`voter_constraints: ["+regio=us-east1"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+rack=17"]`, expectSuccess, getNodes},
		{`voter_constraints: ["+rack=18"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+rach=17"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+highcpu"]`, expectSuccess, getNodes},
		{`voter_constraints: ["+lowmem"]`, expectSuccess, getNodes},
		{`voter_constraints: ["+ssd"]`, expectSuccess, getNodes},
		{`voter_constraints: ["+highcp"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+owmem"]`, expectValidateErr, getNodes},
		{`voter_constraints: ["+sssd"]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectSuccess, getNodes},
		{`lease_preferences: [["+region=us-eat1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+foo"], ["+geo=us", "+highcpu"]]`, expectValidateErr, getNodes},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+bar"]]`, expectValidateErr, getNodes},
		{`constraints: ["-region=us-east1"]`, expectSuccess, singleLocalityNode},
		{`constraints: ["-ssd"]`, expectSuccess, singleAttrNode},
		{`constraints: ["-regio=us-eas1"]`, expectSuccess, getNodes},
		{`constraints: {"-region=us-eas1": 2, "-region=eu-wes1": 1}`, expectSuccess, getNodes},
		{`constraints: ["-foo=bar"]`, expectSuccess, getNodes},
		{`constraints: ["-highcpu"]`, expectSuccess, getNodes},
		{`constraints: ["-ssd"]`, expectSuccess, getNodes},
		{`constraints: ["-fake"]`, expectSuccess, getNodes},
		{`voter_constraints: ["-region=us-east1"]`, expectSuccess, singleLocalityNode},
		{`voter_constraints: ["-ssd"]`, expectSuccess, singleAttrNode},
		{`voter_constraints: ["-regio=us-eas1"]`, expectSuccess, getNodes},
		{`voter_constraints: {"-region=us-eas1": 2, "-region=eu-wes1": 1}`, expectSuccess, getNodes},
		{`voter_constraints: ["-foo=bar"]`, expectSuccess, getNodes},
		{`voter_constraints: ["-highcpu"]`, expectSuccess, getNodes},
		{`voter_constraints: ["-ssd"]`, expectSuccess, getNodes},
		{`voter_constraints: ["-fake"]`, expectSuccess, getNodes},
	} {
		var zone zonepb.ZoneConfig
		err := yaml.UnmarshalStrict([]byte(tc.cfg), &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Fatalf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectParseErr {
			t.Fatalf("#%d: expected parse err for %q; got success", i, tc.cfg)
		}

		err = validateZoneAttrsAndLocalitiesForSystemTenant(context.Background(), tc.nodes, zonepb.NewZoneConfig(), &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Errorf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectValidateErr {
			t.Errorf("#%d: expected err for %q; got success", i, tc.cfg)
		}
	}
}

func TestValidateVoterConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		constraints      string
		voterConstraints string
		shouldFail       bool
		errRegex         string
	}{
		{
			constraints:      ``,
			voterConstraints: ``,
			shouldFail:       false,
			errRegex:         "",
		},
		{
			constraints:      `["-foo=bar"]`,
			voterConstraints: `{"+foo=bar": 1}`,
			shouldFail:       true,
			errRegex:         "conflicts with voter_constraint",
		},
		{
			constraints:      `["-foo=bar"]`,
			voterConstraints: `{"+foo=bar": 1}`,
			shouldFail:       true,
			errRegex:         "conflicts with voter_constraint",
		},
		{
			constraints:      `["-foo=bar", "+duck=bar"]`,
			voterConstraints: `["-foo=duck", "+duck=bar"]`,
			shouldFail:       true,
			errRegex:         "voter_constraints cannot contain prohibitive",
		},
		{
			constraints:      `["-foo=bar"]`,
			voterConstraints: `["-foo=bar"]`,
			shouldFail:       true,
			errRegex:         "voter_constraints cannot contain prohibitive",
		},
		{
			constraints:      `["-foo=bar"]`,
			voterConstraints: `["-duck=bar"]`,
			shouldFail:       true,
			errRegex:         "voter_constraints cannot contain prohibitive",
		},
		{
			constraints:      `["+foo=bar"]`,
			voterConstraints: `["+foo=bar"]`,
			shouldFail:       false,
			errRegex:         "",
		},
		{
			constraints:      `{"+foo=bar": 1, "+foo=duck": 2}`,
			voterConstraints: `{"+foo=bar": 3}`,
			shouldFail:       false,
			errRegex:         "",
		},
		{
			constraints:      `{"+region=A": 1, "+region=B": 1, "+region=C": 1}`,
			voterConstraints: `["+ssd"]`,
			shouldFail:       false,
			errRegex:         "",
		},
	} {
		zone := zonepb.NewZoneConfig()
		zone.NumVoters = proto.Int32(3)
		zone.NumReplicas = proto.Int32(3)

		require.NoError(t, yaml.UnmarshalStrict([]byte(`constraints: `+tc.constraints), &zone))
		require.NoError(t, yaml.UnmarshalStrict([]byte(`voter_constraints: `+tc.voterConstraints), &zone))
		err := zone.Validate()
		if err != nil && tc.shouldFail {
			require.Regexp(t, tc.errRegex, err)
		} else if tc.shouldFail {
			t.Fatalf("validation unexpectedly succeeded for constraints: %s"+
				" and voter_constraints: %s", tc.constraints, tc.voterConstraints)
		}
	}
}
