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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// ParseBatchRequests is a helper function to parse datadriven input that
// declares (empty) batch requests of supported types, for a particular tenant.
// The constructed batch request is returned. The cmds are of the following
// form:
//
// cmds=(split, scan, cput)
func ParseBatchRequests(t *testing.T, d *datadriven.TestData) (ba kvpb.BatchRequest) {
	for _, cmd := range d.CmdArgs {
		if cmd.Key == "cmds" {
			for _, z := range cmd.Vals {
				method, ok := kvpb.StringToMethodMap[z]
				if !ok {
					t.Fatalf("unsupported request type: %s", z)
				}
				request := kvpb.CreateRequest(method)
				ba.Add(request)
			}
		}
	}
	return ba
}

// ParseTenantCapabilityUpsert parses all args which have a key that is a
// capability key and sets it on top of the default tenant capabilities.
func ParseTenantCapabilityUpsert(
	t *testing.T, d *datadriven.TestData,
) (roachpb.TenantID, *tenantcapabilitiespb.TenantCapabilities, error) {
	tID := GetTenantID(t, d)
	caps := tenantcapabilitiespb.TenantCapabilities{}
	for _, arg := range d.CmdArgs {
		capability, ok := tenantcapabilities.FromName(arg.Key)
		if !ok {
			continue
		}
		switch c := capability.(type) {
		case tenantcapabilities.BoolCapability:
			b, err := strconv.ParseBool(arg.Vals[0])
			if err != nil {
				return roachpb.TenantID{}, nil, err
			}
			c.Value(&caps).Set(b)
		case tenantcapabilities.SpanConfigBoundsCapability:
			jsonD, err := json.ParseJSON(arg.Vals[0])
			if err != nil {
				return roachpb.TenantID{}, nil, err
			}
			var v tenantcapabilitiespb.SpanConfigBounds
			if _, err := protoreflect.JSONBMarshalToMessage(jsonD, &v); err != nil {
				return roachpb.TenantID{}, nil, err
			}
			c.Value(&caps).Set(spanconfigbounds.New(&v))
		default:
			t.Fatalf("unknown capability type %T for capability %s", c, arg.Key)
		}
	}
	return tID, &caps, nil
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

// AlteredCapabilitiesString pretty-prints all altered capability
// values that no longer match an empty protobuf.
func AlteredCapabilitiesString(capabilities *tenantcapabilitiespb.TenantCapabilities) string {
	defaultCapabilities := &tenantcapabilitiespb.TenantCapabilities{}
	var builder strings.Builder
	builder.WriteByte('{')
	space := ""
	for _, capID := range tenantcapabilities.IDs {
		value := tenantcapabilities.MustGetValueByID(capabilities, capID)
		defaultValue := tenantcapabilities.MustGetValueByID(defaultCapabilities, capID)
		if value.String() != defaultValue.String() {
			builder.WriteString(space)
			builder.WriteString(capID.String())
			builder.WriteByte(':')
			builder.WriteString(value.String())
			space = " "
		}
	}
	// All capabilities have default values.
	if space == "" {
		builder.WriteString("default")
	}
	builder.WriteByte('}')
	return builder.String()
}
