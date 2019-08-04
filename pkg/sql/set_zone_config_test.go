// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	yaml "gopkg.in/yaml.v2"
)

func TestValidateNoRepeatKeysInZone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		constraint    string
		expectSuccess bool
	}{
		{`constraints: ["+region=us-east-1"]`, true},
		{`constraints: ["+region=us-east-1", "+zone=pa"]`, true},
		{`constraints: ["+region=us-east-1", "-region=us-west-1"]`, true},
		{`constraints: ["+region=us-east-1", "+region=us-east-2"]`, false},
		{`constraints: ["+region=us-east-1", "+zone=pa", "+region=us-west-1"]`, false},
		{`constraints: ["+region=us-east-1", "-region=us-east-1"]`, false},
		{`constraints: ["-region=us-east-1", "+region=us-east-1"]`, false},
		{`constraints: {"+region=us-east-1":2, "+region=us-east-2":2}`, true},
		{`constraints: {"+region=us-east-1,+region=us-west-1":2, "+region=us-east-2":2}`, false},
	}

	for _, tc := range testCases {
		var zone config.ZoneConfig
		err := yaml.UnmarshalStrict([]byte(tc.constraint), &zone)
		if err != nil {
			t.Fatal(err)
		}
		err = validateNoRepeatKeysInZone(&zone)
		if err != nil && tc.expectSuccess {
			t.Errorf("expected success for %q; got %v", tc.constraint, err)
		} else if err == nil && !tc.expectSuccess {
			t.Errorf("expected err for %q; got success", tc.constraint)
		}
	}
}

func TestValidateZoneAttrsAndLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	nodes := &serverpb.NodesResponse{}
	for _, store := range stores {
		nodes.Nodes = append(nodes.Nodes, statuspb.NodeStatus{
			StoreStatuses: []statuspb.StoreStatus{
				{
					Desc: roachpb.StoreDescriptor{
						Attrs: roachpb.Attributes{
							Attrs: store.storeAttrs,
						},
						Node: roachpb.NodeDescriptor{
							Attrs: roachpb.Attributes{
								Attrs: store.nodeAttrs,
							},
							Locality: roachpb.Locality{
								Tiers: store.storeLocality,
							},
						},
					},
				},
			},
		})
	}

	getNodes := func(_ context.Context, _ *serverpb.NodesRequest) (*serverpb.NodesResponse, error) {
		return nodes, nil
	}

	const expectSuccess = 0
	const expectParseErr = 1
	const expectValidateErr = 2
	for i, tc := range []struct {
		cfg       string
		expectErr int
	}{
		{`nonsense`, expectParseErr},
		{`range_max_bytes: 100`, expectSuccess},
		{`range_max_byte: 100`, expectParseErr},
		{`constraints: ["+region=us-east1"]`, expectSuccess},
		{`constraints: {"+region=us-east1": 2, "+region=eu-west1": 1}`, expectSuccess},
		{`constraints: ["+region=us-eas1"]`, expectValidateErr},
		{`constraints: {"+region=us-eas1": 2, "+region=eu-west1": 1}`, expectValidateErr},
		{`constraints: {"+region=us-east1": 2, "+region=eu-wes1": 1}`, expectValidateErr},
		{`constraints: ["+regio=us-east1"]`, expectValidateErr},
		{`constraints: ["+rack=17"]`, expectSuccess},
		{`constraints: ["+rack=18"]`, expectValidateErr},
		{`constraints: ["+rach=17"]`, expectValidateErr},
		{`constraints: ["+highcpu"]`, expectSuccess},
		{`constraints: ["+lowmem"]`, expectSuccess},
		{`constraints: ["+ssd"]`, expectSuccess},
		{`constraints: ["+highcp"]`, expectValidateErr},
		{`constraints: ["+owmem"]`, expectValidateErr},
		{`constraints: ["+sssd"]`, expectValidateErr},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectSuccess},
		{`lease_preferences: [["+region=us-eat1", "+ssd"], ["+geo=us", "+highcpu"]]`, expectValidateErr},
		{`lease_preferences: [["+region=us-east1", "+foo"], ["+geo=us", "+highcpu"]]`, expectValidateErr},
		{`lease_preferences: [["+region=us-east1", "+ssd"], ["+geo=us", "+bar"]]`, expectValidateErr},
		{`constraints: ["-region=us-east1"]`, expectSuccess},
		{`constraints: ["-regio=us-eas1"]`, expectSuccess},
		{`constraints: {"-region=us-eas1": 2, "-region=eu-wes1": 1}`, expectSuccess},
		{`constraints: ["-foo=bar"]`, expectSuccess},
		{`constraints: ["-highcpu"]`, expectSuccess},
		{`constraints: ["-ssd"]`, expectSuccess},
		{`constraints: ["-fake"]`, expectSuccess},
	} {
		var zone config.ZoneConfig
		err := yaml.UnmarshalStrict([]byte(tc.cfg), &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Fatalf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectParseErr {
			t.Fatalf("#%d: expected parse err for %q; got success", i, tc.cfg)
		}

		err = validateZoneAttrsAndLocalities(context.Background(), getNodes, &zone)
		if err != nil && tc.expectErr == expectSuccess {
			t.Errorf("#%d: expected success for %q; got %v", i, tc.cfg, err)
		} else if err == nil && tc.expectErr == expectValidateErr {
			t.Errorf("#%d: expected err for %q; got success", i, tc.cfg)
		}
	}
}
