// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverutils_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

//go:generate ../../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

func TestGetExternalCaller(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c1 := serverutils.GetExternalCaller()
	t.Logf("c1: %s", c1)
	require.True(t, strings.HasSuffix(c1, "(TestGetExternalCaller)"))
	require.True(t, strings.Contains(c1, "conditional_wrap_test.go:"))

	c2 := func() string { return serverutils.GetExternalCaller() }()
	t.Logf("c2: %s", c2)
	require.True(t, strings.HasSuffix(c2, "(TestGetExternalCaller.func1)"))
	require.True(t, strings.Contains(c2, "conditional_wrap_test.go:"))

	c3 := otherFn()
	t.Logf("c3: %s", c3)
	require.True(t, strings.HasSuffix(c3, "(otherFn)"))
	require.True(t, strings.Contains(c3, "conditional_wrap_test.go:"))

	t.Logf("externalC: %s", externalC)
	require.True(t, strings.HasSuffix(externalC, "(init.func1)"))
	require.True(t, strings.Contains(externalC, "conditional_wrap_test.go:"))
}

func otherFn() string {
	return serverutils.GetExternalCaller()
}

var externalC = func() string { return serverutils.GetExternalCaller() }()

func TestInterfaceNotices(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runCalls := func(s serverutils.TestServerInterface) {
		_ = s.AppStopper()
		_ = s.NodeID()
		_ = s.StartedDefaultTestTenant()
	}

	testCases := []struct {
		name     string
		opts     base.DefaultTestTenantOptions
		scenario func(t *testing.T, exec func(func(serverutils.TestServerInterface)) string)
	}{
		{
			name: "default",
			opts: (base.DefaultTestTenantOptions{}),
			scenario: func(t *testing.T, exec func(func(serverutils.TestServerInterface)) string) {
				result := exec(runCalls)
				require.Contains(t, result, "NOTICE: .AppStopper() called via implicit interface ApplicationLayerInterface")
				require.Contains(t, result, "NOTICE: .NodeID() called via implicit interface StorageLayerInterface")
				require.Contains(t, result, "NOTICE: .StartedDefaultTestTenant() called via implicit interface TenantControlInterface")
			},
		},
		{
			name: "systemonly",
			opts: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
			scenario: func(t *testing.T, exec func(func(serverutils.TestServerInterface)) string) {
				result := exec(runCalls)
				require.NotContains(t, result, ".AppStopper()")
				require.NotContains(t, result, ".NodeID()")
				require.Contains(t, result, "NOTICE: .StartedDefaultTestTenant() called via implicit interface TenantControlInterface")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defer log.Scope(t).Close(t)

			s := serverutils.StartServerOnly(t, base.TestServerArgs{DefaultTestTenant: tc.opts})
			defer s.Stopper().Stop(context.Background())

			var buf bytes.Buffer
			serverutils.TestingSetWrapperLogger(s, func(format string, args ...interface{}) { fmt.Fprintf(&buf, format, args...) })

			exec := func(fn func(serverutils.TestServerInterface)) string {
				buf.Reset()
				fn(s)
				return buf.String()
			}
			tc.scenario(t, exec)
		})
	}
}
