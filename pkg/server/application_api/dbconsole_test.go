// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestAdminAPIUIData checks that UI customizations are properly
// persisted for both admin and non-admin users.
func TestAdminAPIUIData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	testutils.RunTrueAndFalse(t, "isAdmin", func(t *testing.T, isAdmin bool) {
		start := timeutil.Now()

		mustSetUIData := func(keyValues map[string][]byte) {
			if err := srvtestutils.PostAdminJSONProtoWithAdminOption(s, "uidata", &serverpb.SetUIDataRequest{
				KeyValues: keyValues,
			}, &serverpb.SetUIDataResponse{}, isAdmin); err != nil {
				t.Fatal(err)
			}
		}

		expectKeyValues := func(expKeyValues map[string][]byte) {
			var resp serverpb.GetUIDataResponse
			queryValues := make(url.Values)
			for key := range expKeyValues {
				queryValues.Add("keys", key)
			}
			url := "uidata?" + queryValues.Encode()
			if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, url, &resp, isAdmin); err != nil {
				t.Fatal(err)
			}
			// Do a two-way comparison. We can't use reflect.DeepEqual(), because
			// resp.KeyValues has timestamps and expKeyValues doesn't.
			for key, actualVal := range resp.KeyValues {
				if a, e := actualVal.Value, expKeyValues[key]; !bytes.Equal(a, e) {
					t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
				}
			}
			for key, expVal := range expKeyValues {
				if a, e := resp.KeyValues[key].Value, expVal; !bytes.Equal(a, e) {
					t.Fatalf("key %s: value = %v, expected = %v", key, a, e)
				}
			}

			// Sanity check LastUpdated.
			for _, val := range resp.KeyValues {
				now := timeutil.Now()
				if val.LastUpdated.Before(start) {
					t.Fatalf("val.LastUpdated %s < start %s", val.LastUpdated, start)
				}
				if val.LastUpdated.After(now) {
					t.Fatalf("val.LastUpdated %s > now %s", val.LastUpdated, now)
				}
			}
		}

		expectValueEquals := func(key string, expVal []byte) {
			expectKeyValues(map[string][]byte{key: expVal})
		}

		expectKeyNotFound := func(key string) {
			var resp serverpb.GetUIDataResponse
			url := "uidata?keys=" + key
			if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, url, &resp, isAdmin); err != nil {
				t.Fatal(err)
			}
			if len(resp.KeyValues) != 0 {
				t.Fatal("key unexpectedly found")
			}
		}

		// Basic tests.
		var badResp serverpb.GetUIDataResponse
		const errPattern = "400 Bad Request"
		if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, "uidata", &badResp, isAdmin); !testutils.IsError(err, errPattern) {
			t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
		}

		mustSetUIData(map[string][]byte{"k1": []byte("v1")})
		expectValueEquals("k1", []byte("v1"))

		expectKeyNotFound("NON_EXISTENT_KEY")

		mustSetUIData(map[string][]byte{
			"k2": []byte("v2"),
			"k3": []byte("v3"),
		})
		expectValueEquals("k2", []byte("v2"))
		expectValueEquals("k3", []byte("v3"))
		expectKeyValues(map[string][]byte{
			"k2": []byte("v2"),
			"k3": []byte("v3"),
		})

		mustSetUIData(map[string][]byte{"k2": []byte("v2-updated")})
		expectKeyValues(map[string][]byte{
			"k2": []byte("v2-updated"),
			"k3": []byte("v3"),
		})

		// Write a binary blob with all possible byte values, then verify it.
		var buf bytes.Buffer
		for i := 0; i < 997; i++ {
			buf.WriteByte(byte(i % 256))
		}
		mustSetUIData(map[string][]byte{"bin": buf.Bytes()})
		expectValueEquals("bin", buf.Bytes())
	})
}

// TestAdminAPIUISeparateData check that separate users have separate customizations.
func TestAdminAPIUISeparateData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	// Make a setting for an admin user.
	if err := srvtestutils.PostAdminJSONProtoWithAdminOption(s, "uidata",
		&serverpb.SetUIDataRequest{KeyValues: map[string][]byte{"k": []byte("v1")}},
		&serverpb.SetUIDataResponse{},
		true /*isAdmin*/); err != nil {
		t.Fatal(err)
	}

	// Make a setting for a non-admin user.
	if err := srvtestutils.PostAdminJSONProtoWithAdminOption(s, "uidata",
		&serverpb.SetUIDataRequest{KeyValues: map[string][]byte{"k": []byte("v2")}},
		&serverpb.SetUIDataResponse{},
		false /*isAdmin*/); err != nil {
		t.Fatal(err)
	}

	var resp serverpb.GetUIDataResponse
	url := "uidata?keys=k"

	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, url, &resp, true /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	if len(resp.KeyValues) != 1 || !bytes.Equal(resp.KeyValues["k"].Value, []byte("v1")) {
		t.Fatalf("unexpected admin values: %+v", resp.KeyValues)
	}
	if err := srvtestutils.GetAdminJSONProtoWithAdminOption(s, url, &resp, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}
	if len(resp.KeyValues) != 1 || !bytes.Equal(resp.KeyValues["k"].Value, []byte("v2")) {
		t.Fatalf("unexpected non-admin values: %+v", resp.KeyValues)
	}
}
