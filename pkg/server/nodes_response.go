// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// uiNodeMetrics contains all the metrics required for the db-console frontend.
// These will be the only node metrics returned in the serverpb.NodeResponse
// metrics.
var uiNodeMetrics = []string{
	"sys.cpu.user.percent",
	"sys.cpu.sys.percent",
	"sys.go.allocbytes",
	"sql.conns",
	"sys.rss",
}

// uiStoreMetrics contains all the metrics required for the db-console frontend.
// These will be the only node store metrics returned in the
// serverpb.NodeResponse store_status metrics.
var uiStoreMetrics = []string{
	"replicas",
	"replicas.leaders",
	"replicas.leaseholders",
	"ranges",
	"ranges.unavailable",
	"ranges.underreplicated",
	"livebytes",
	"keybytes",
	"valbytes",
	"rangekeybytes",
	"rangevalbytes",
	"totalbytes",
	"intentbytes",
	"livecount",
	"keycount",
	"valcount",
	"intentcount",
	"intentage",
	"gcbytesage",
	"capacity",
	"capacity.available",
	"capacity.used",
	"sysbytes",
	"syscount",
}

func nodeStatusToResp(n *statuspb.NodeStatus, hasViewClusterMetadata bool) serverpb.NodeResponse {
	tiers := make([]serverpb.Tier, len(n.Desc.Locality.Tiers))
	for j, t := range n.Desc.Locality.Tiers {
		tiers[j] = serverpb.Tier{
			Key:   t.Key,
			Value: t.Value,
		}
	}

	activity := make(map[roachpb.NodeID]serverpb.NodeResponse_NetworkActivity, len(n.Activity))
	for k, v := range n.Activity {
		activity[k] = serverpb.NodeResponse_NetworkActivity{
			Latency: v.Latency,
		}
	}

	nodeDescriptor := serverpb.NodeDescriptor{
		NodeID:  n.Desc.NodeID,
		Address: util.UnresolvedAddr{},
		Attrs:   roachpb.Attributes{},
		Locality: serverpb.Locality{
			Tiers: tiers,
		},
		ServerVersion: serverpb.Version{
			Major:    n.Desc.ServerVersion.Major,
			Minor:    n.Desc.ServerVersion.Minor,
			Patch:    n.Desc.ServerVersion.Patch,
			Internal: n.Desc.ServerVersion.Internal,
		},
		BuildTag:        n.Desc.BuildTag,
		StartedAt:       n.Desc.StartedAt,
		LocalityAddress: nil,
		ClusterName:     n.Desc.ClusterName,
		SQLAddress:      util.UnresolvedAddr{},
	}

	statuses := make([]serverpb.StoreStatus, len(n.StoreStatuses))
	for i, ss := range n.StoreStatuses {
		storeMetrics := make(map[string]float64, len(uiStoreMetrics))
		for _, m := range uiStoreMetrics {
			if d, ok := ss.Metrics[m]; ok {
				storeMetrics[m] = d
			}
		}
		statuses[i] = serverpb.StoreStatus{
			Desc: serverpb.StoreDescriptor{
				StoreID:  ss.Desc.StoreID,
				Attrs:    ss.Desc.Attrs,
				Node:     nodeDescriptor,
				Capacity: ss.Desc.Capacity,

				Properties: roachpb.StoreProperties{
					ReadOnly:  ss.Desc.Properties.ReadOnly,
					Encrypted: ss.Desc.Properties.Encrypted,
				},
			},
			Metrics: storeMetrics,
		}
		if fsprops := ss.Desc.Properties.FileStoreProperties; fsprops != nil {
			sfsprops := &roachpb.FileStoreProperties{
				FsType: fsprops.FsType,
			}
			if hasViewClusterMetadata {
				sfsprops.Path = fsprops.Path
				sfsprops.BlockDevice = fsprops.BlockDevice
				sfsprops.MountPoint = fsprops.MountPoint
				sfsprops.MountOptions = fsprops.MountOptions
			}
			statuses[i].Desc.Properties.FileStoreProperties = sfsprops
		}
	}

	metrics := make(map[string]float64, len(uiNodeMetrics))
	for _, m := range uiNodeMetrics {
		if d, ok := n.Metrics[m]; ok {
			metrics[m] = d
		}
	}

	resp := serverpb.NodeResponse{
		Desc:              nodeDescriptor,
		BuildInfo:         n.BuildInfo,
		StartedAt:         n.StartedAt,
		UpdatedAt:         n.UpdatedAt,
		Metrics:           metrics,
		StoreStatuses:     statuses,
		Args:              nil,
		Env:               nil,
		Latencies:         n.Latencies,
		Activity:          activity,
		TotalSystemMemory: n.TotalSystemMemory,
		NumCpus:           n.NumCpus,
	}

	if hasViewClusterMetadata {
		resp.Args = n.Args
		resp.Env = n.Env
		resp.Desc.Attrs = n.Desc.Attrs
		resp.Desc.Address = n.Desc.Address
		resp.Desc.LocalityAddress = n.Desc.LocalityAddress
		resp.Desc.SQLAddress = n.Desc.SQLAddress
		for _, n := range resp.StoreStatuses {
			n.Desc.Node = resp.Desc
		}
	}

	return resp
}

func regionsResponseFromNodesResponse(nr *serverpb.NodesResponse) *serverpb.RegionsResponse {
	regionsToZones := make(map[string]map[string]struct{})
	for _, node := range nr.Nodes {
		var region string
		var zone string
		for _, tier := range node.Desc.Locality.Tiers {
			switch tier.Key {
			case "region":
				region = tier.Value
			case "zone", "availability-zone", "az":
				zone = tier.Value
			}
		}
		if region == "" {
			continue
		}
		if _, ok := regionsToZones[region]; !ok {
			regionsToZones[region] = make(map[string]struct{})
		}
		if zone != "" {
			regionsToZones[region][zone] = struct{}{}
		}
	}
	ret := &serverpb.RegionsResponse{
		Regions: make(map[string]*serverpb.RegionsResponse_Region, len(regionsToZones)),
	}
	for region, zones := range regionsToZones {
		zonesArr := make([]string, 0, len(zones))
		for z := range zones {
			zonesArr = append(zonesArr, z)
		}
		sort.Strings(zonesArr)
		ret.Regions[region] = &serverpb.RegionsResponse_Region{
			Zones: zonesArr,
		}
	}
	return ret
}
