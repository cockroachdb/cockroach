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

func ParseTenantCapabilityUpsert(
	t *testing.T, d *datadriven.TestData,
) (roachpb.TenantID, tenantcapabilitiespb.TenantCapabilities, error) {
	tID := GetTenantID(t, d)
	caps := tenantcapabilitiespb.TenantCapabilities{}
	for _, arg := range d.CmdArgs {
		if capID, ok := tenantcapabilities.CapabilityIDFromString(arg.Key); ok {
			capType := capID.CapabilityType()
			switch capType {
			case tenantcapabilities.Bool:
				b, err := strconv.ParseBool(arg.Vals[0])
				if err != nil {
					return roachpb.TenantID{}, tenantcapabilitiespb.TenantCapabilities{}, err
				}
				caps.Cap(capID).Set(b)
			default:
				t.Fatalf("unknown type %q", capType)
			}
		}
	}
	return tID, caps, nil
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

// AlteredCapabilitiesString prints all altered capability values that no
// longer match DefaultCapabilities. This is different from
// TenantCapabilities.String which only prints non-zero value fields.
func AlteredCapabilitiesString(capabilities tenantcapabilities.TenantCapabilities) string {
	defaultCapabilities := tenantcapabilities.DefaultCapabilities()
	var builder strings.Builder
	builder.WriteByte('{')
	space := ""
	for _, capID := range tenantcapabilities.CapabilityIDs {
		value := capabilities.Cap(capID).Get().String()
		defaultValue := defaultCapabilities.Cap(capID).Get().String()
		if value != defaultValue {
			builder.WriteString(space)
			builder.WriteString(capID.String())
			builder.WriteByte(':')
			builder.WriteString(value)
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
