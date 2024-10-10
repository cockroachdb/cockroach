// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package diagnostics

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	build "github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/mitchellh/reflectwalk"
	"github.com/stretchr/testify/require"
)

// TestStringRedactor_Primitive tests that fields of type `*string` will be
// correctly redacted to "_", and all other field types ( including `string`)
// will be unchanged.
func TestStringRedactor_Primitive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type Foo struct {
		A string
		B *string
		C map[string]string
	}

	string1 := "string 1"
	string2 := "string 2"
	foo := Foo{
		A: string1,
		B: &string2,
		C: map[string]string{"3": "string 3"},
	}

	require.NoError(t, reflectwalk.Walk(foo, stringRedactor{}))
	require.Equal(t, "string 1", string1)
	require.Equal(t, "string 1", foo.A)
	require.Equal(t, "_", string2)
	require.Equal(t, "_", *foo.B)
	require.Equal(t, "string 3", foo.C["3"])
}

func TestBuildReportingURL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	report := &diagnosticspb.DiagnosticReport{
		Env: diagnosticspb.Environment{
			LicenseType: "Enterprise",
			Build: build.Info{
				Tag:        "tag",
				Platform:   "platform",
				Channel:    "buildchannel",
				EnvChannel: "envchannel",
			},
		},
		Node: diagnosticspb.NodeInfo{
			NodeID: 1,
		},
		SQL: diagnosticspb.SQLInstanceInfo{
			SQLInstanceID: 2,
			Uptime:        3,
		},
	}
	licenseID, err := uuid.FromString("abc362b1-4f67-4bc0-b7dd-5628e49d2cba")
	require.NoError(t, err)
	organizationID, err := uuid.FromString("123362b1-4f67-4bc0-b7dd-5628e49d2321")
	require.NoError(t, err)
	license := &licenseccl.License{
		ValidUntilUnixSec: 4,
		Type:              licenseccl.License_Enterprise,
		Environment:       1,
		LicenseId:         licenseID.GetBytes(),
		OrganizationId:    organizationID.GetBytes(),
	}
	r := srv.DiagnosticsReporter().(*Reporter)
	url := r.buildReportingURL(report, license)
	logicalClusterUUID := r.LogicalClusterID()
	storageClusterUUID := r.StorageClusterID()
	require.Equal(t, fmt.Sprintf(`https://register.cockroachdb.com/api/clusters/report?buildchannel=buildchannel&envchannel=envchannel&environment=production&insecure=false&internal=false&license_expiry_seconds=4&license_id=abc362b1-4f67-4bc0-b7dd-5628e49d2cba&licensetype=Enterprise&logical_uuid=%s&nodeid=1&organization_id=123362b1-4f67-4bc0-b7dd-5628e49d2321&platform=platform&sqlid=2&tenantid=%s&uptime=3&uuid=%s&version=tag`, logicalClusterUUID, r.TenantID.String(), storageClusterUUID), url.String())
}

func TestBuildReportingURLNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	report := &diagnosticspb.DiagnosticReport{
		Env: diagnosticspb.Environment{
			LicenseType: "OSS",
			Build: build.Info{
				Tag:        "tag",
				Platform:   "platform",
				Channel:    "buildchannel",
				EnvChannel: "envchannel",
			},
		},
		Node: diagnosticspb.NodeInfo{
			NodeID: 1,
		},
		SQL: diagnosticspb.SQLInstanceInfo{
			SQLInstanceID: 2,
			Uptime:        3,
		},
	}
	r := srv.DiagnosticsReporter().(*Reporter)
	url := r.buildReportingURL(report, nil)
	logicalClusterUUID := r.LogicalClusterID()
	storageClusterUUID := r.StorageClusterID()
	require.Equal(t, fmt.Sprintf(`https://register.cockroachdb.com/api/clusters/report?buildchannel=buildchannel&envchannel=envchannel&environment=&insecure=false&internal=false&license_expiry_seconds=&license_id=&licensetype=OSS&logical_uuid=%s&nodeid=1&organization_id=&platform=platform&sqlid=2&tenantid=%s&uptime=3&uuid=%s&version=tag`, logicalClusterUUID, r.TenantID.String(), storageClusterUUID), url.String())
}

func TestShouldReportDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const noLicense = licenseccl.License_Type(math.MaxInt32)

	st := cluster.MakeClusterSettings()
	t0 := timeutil.Unix(0, 0)
	logcrash.SetGlobalSettings(&st.SV)
	for _, test := range []struct {
		diagnostics bool
		licenseType licenseccl.License_Type
		expected    bool
	}{
		{true, licenseccl.License_Enterprise, true},
		{false, licenseccl.License_Enterprise, false},
		{true, licenseccl.License_NonCommercial, true},
		{false, licenseccl.License_NonCommercial, false},
		{true, licenseccl.License_Evaluation, true},
		{false, licenseccl.License_Evaluation, false},
		{true, licenseccl.License_Trial, true},
		{false, licenseccl.License_Trial, true},
		{true, licenseccl.License_Free, true},
		{false, licenseccl.License_Free, true},
		{true, noLicense, true},
		{false, noLicense, false},
	} {
		t.Run(fmt.Sprintf("diagnostics enabled=%t, license=%s", test.diagnostics, test.licenseType), func(t *testing.T) {
			ctx := context.Background()
			if test.licenseType != noLicense {
				lic, err := (&licenseccl.License{
					Type:              test.licenseType,
					ValidUntilUnixSec: t0.AddDate(0, 1, 0).Unix(),
				}).Encode()
				require.NoError(t, err)
				utilccl.EnterpriseLicense.Override(ctx, &st.SV, lic)
			} else {
				utilccl.EnterpriseLicense.Override(ctx, &st.SV, "")
			}
			logcrash.DiagnosticsReportingEnabled.Override(ctx, &st.SV, test.diagnostics)
			require.Equal(t, test.expected, shouldReportDiagnostics(context.Background(), st))
		})
	}
}
