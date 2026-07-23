// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils/regionlatency"
)

// DemoLocalityList represents a list of localities for the cockroach
// demo command.
type DemoLocalityList []roachpb.Locality

// Type implements the pflag.Value interface.
func (l *DemoLocalityList) Type() string { return "demoLocalityList" }

// String implements the pflag.Value interface.
func (l *DemoLocalityList) String() string {
	s := ""
	for _, loc := range []roachpb.Locality(*l) {
		s += loc.String()
	}
	return s
}

// Set implements the pflag.Value interface.
func (l *DemoLocalityList) Set(value string) error {
	*l = []roachpb.Locality{}
	locs := strings.Split(value, ":")
	for _, value := range locs {
		parsedLoc := &roachpb.Locality{}
		if err := parsedLoc.Set(value); err != nil {
			return err
		}
		*l = append(*l, *parsedLoc)
	}
	return nil
}

var defaultLocalities = DemoLocalityList{
	// Default localities for a 3 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east1"}, {Key: "az", Value: "d"}}},
	// Default localities for a 6 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "a"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west1"}, {Key: "az", Value: "c"}}},
	// Default localities for a 9 node cluster
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "b"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "c"}}},
	{Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}, {Key: "az", Value: "d"}}},
}

// Round-trip latencies collected from http://cloudping.co on 2019-09-11.
var localityLatencies = regionlatency.RoundTripPairs{
	{A: "us-east1", B: "us-west1"}:     66 * time.Millisecond,
	{A: "us-east1", B: "europe-west1"}: 64 * time.Millisecond,
	{A: "us-west1", B: "europe-west1"}: 146 * time.Millisecond,
}.ToLatencyMap()
