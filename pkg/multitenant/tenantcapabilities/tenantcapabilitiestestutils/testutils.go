// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitiestestutils

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var reqWithParamRE = regexp.MustCompile(`(\w+)(?:{(\w+)})?`)

// ParseBatchRequests is a helper function to parse datadriven input that
// declares (empty) batch requests of supported types, for a particular tenant.
// The constructed batch request is returned. The cmds are of the following
// form:
//
// cmds=(AdminSplit, Scan, ConditionalPut, EndTxn{Prepare})
func ParseBatchRequests(t *testing.T, d *datadriven.TestData) (ba kvpb.BatchRequest) {
	for _, cmd := range d.CmdArgs {
		if cmd.Key == "cmds" {
			for _, z := range cmd.Vals {
				reqWithParam := reqWithParamRE.FindStringSubmatch(z)
				reqStr := reqWithParam[1]
				paramStr := reqWithParam[2]
				method, ok := kvpb.StringToMethodMap[reqStr]
				if !ok {
					t.Fatalf("unsupported request type: %s", z)
				}
				request := kvpb.CreateRequest(method)
				if paramStr != "" {
					ok = false
					switch method {
					case kvpb.EndTxn:
						switch paramStr {
						case "Prepare":
							request.(*kvpb.EndTxnRequest).Prepare = true
							ok = true
						}
					}
					if !ok {
						t.Fatalf("unsupported %s param: %s", method, paramStr)
					}
				}
				ba.Add(request)
			}
		}
	}
	return ba
}

// ParseTenantInfo collects the name, service mode and data state from the test input.
func ParseTenantInfo(
	t *testing.T, d *datadriven.TestData,
) (
	name *roachpb.TenantName,
	dataState mtinfopb.TenantDataState,
	serviceMode mtinfopb.TenantServiceMode,
	err error,
) {
	if d.HasArg("name") {
		var tenantName string
		d.ScanArgs(t, "name", &tenantName)
		tname := roachpb.TenantName(tenantName)
		name = &tname
	}
	if d.HasArg("service") {
		var mode string
		d.ScanArgs(t, "service", &mode)
		var ok bool
		serviceMode, ok = mtinfopb.TenantServiceModeValues[mode]
		if !ok {
			return nil, 0, 0, errors.Newf("unknown service mode %s", mode)
		}
	}
	if d.HasArg("data") {
		var data string
		d.ScanArgs(t, "data", &data)
		var ok bool
		dataState, ok = mtinfopb.TenantDataStateValues[data]
		if !ok {
			return nil, 0, 0, errors.Newf("unknown data state %s", data)
		}
	}
	return name, dataState, serviceMode, nil
}

// ParseTenantCapabilityUpsert parses all args which have a key that is a
// capability key and sets it on top of the default tenant capabilities.
func ParseTenantCapabilityUpsert(
	t *testing.T, d *datadriven.TestData,
) (tenantcapabilities.Entry, error) {
	entry := tenantcapabilities.Entry{
		TenantID:    GetTenantID(t, d),
		ServiceMode: GetServiceState(t, d),
	}
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
				return entry, err
			}
			c.Value(&caps).Set(b)

		case tenantcapabilities.SpanConfigBoundsCapability:
			jsonD, err := json.ParseJSON(arg.Vals[0])
			if err != nil {
				return entry, err
			}
			var v tenantcapabilitiespb.SpanConfigBounds
			if _, err := protoreflect.JSONBMarshalToMessage(jsonD, &v); err != nil {
				return entry, err
			}
			c.Value(&caps).Set(spanconfigbounds.New(&v))

		default:
			t.Fatalf("unknown capability type %T for capability %s", c, arg.Key)
		}
	}
	entry.TenantCapabilities = &caps
	return entry, nil
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

func GetServiceState(t *testing.T, d *datadriven.TestData) mtinfopb.TenantServiceMode {
	if d.HasArg("service") {
		var state string
		d.ScanArgs(t, "service", &state)
		serviceState, ok := mtinfopb.TenantServiceModeValues[state]
		if !ok {
			t.Fatalf("invalid service state: %s", state)
		}
		return serviceState
	}
	return mtinfopb.ServiceModeExternal
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
