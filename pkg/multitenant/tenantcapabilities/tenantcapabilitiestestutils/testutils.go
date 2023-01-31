// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiestestutils

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// ParseBatchRequestString is a helper function to parse datadriven input that
// declares (empty) batch requests of supported types, for a particular tenant.
// Both the constructed batch request and the requesting tenant ID are returned.
// The cmds are of the following form:
//
// ten=10 cmds=(split, scan, cput)
func ParseBatchRequestString(
	t *testing.T, d *datadriven.TestData,
) (tenID roachpb.TenantID, ba roachpb.BatchRequest) {
	tenID = GetTenantID(t, d)
	for _, cmd := range d.CmdArgs {
		if cmd.Key == "cmds" {
			for _, z := range cmd.Vals {
				switch z {
				case "split":
					ba.Add(&roachpb.AdminSplitRequest{})
				case "scan":
					ba.Add(&roachpb.ScanRequest{})
				case "cput":
					ba.Add(&roachpb.ConditionalPutRequest{})
				default:
					t.Fatalf("unsupported request type: %s", z)
				}
			}
		}
	}
	return tenID, ba
}

func ParseTenantCapabilityUpsert(
	t *testing.T, d *datadriven.TestData,
) (*tenantcapabilities.Update, error) {
	tID := GetTenantID(t, d)
	cap := tenantcapabilitiespb.TenantCapabilities{}
	for _, arg := range d.CmdArgs {
		if arg.Key == "can_admin_split" {
			b, err := strconv.ParseBool(arg.Vals[0])
			if err != nil {
				return nil, err
			}
			cap.CanAdminSplit = b
		}
		if arg.Key == "can_view_node_info" {
			b, err := strconv.ParseBool(arg.Vals[0])
			if err != nil {
				return nil, err
			}
			cap.CanViewNodeInfo = b
		}
		if arg.Key == "can_view_tsdb_metrics" {
			b, err := strconv.ParseBool(arg.Vals[0])
			if err != nil {
				return nil, err
			}
			cap.CanViewTsdbMetrics = b
		}
	}
	update := tenantcapabilities.Update{
		Entry: tenantcapabilities.Entry{
			TenantID:           tID,
			TenantCapabilities: cap,
		},
	}
	return &update, nil
}

func ParseTenantCapabilityDelete(t *testing.T, d *datadriven.TestData) *tenantcapabilities.Update {
	tID := GetTenantID(t, d)
	update := tenantcapabilities.Update{
		Entry: tenantcapabilities.Entry{
			TenantID: tID,
		},
		Deleted: true,
	}
	return &update
}

func GetTenantID(t *testing.T, d *datadriven.TestData) roachpb.TenantID {
	var tenantID string
	if d.HasArg("ten") {
		d.ScanArgs(t, "ten", &tenantID)
	}
	if roachpb.IsSystemTenantName(roachpb.TenantName(tenantID)) {
		return roachpb.SystemTenantID
	}
	tID, err := roachpb.TenantIDFromString(tenantID)
	require.NoError(t, err)
	return tID
}
