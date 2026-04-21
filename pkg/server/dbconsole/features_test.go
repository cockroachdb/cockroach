// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbconsole

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/stretchr/testify/require"
)

// Test features registered once per test binary. Use unique names to avoid
// duplicate setting registration panics.
var (
	testFeatureA = makeTestFeature("test_a")
	testFeatureB = makeTestFeature("test_b")
)

func makeTestFeature(name string) *Feature {
	return &Feature{
		Name:        name,
		Title:       "Test " + name,
		Description: "test feature " + name,
		RoutePath:   "/feature/" + name,
		Setting: settings.RegisterBoolSetting(
			settings.ApplicationLevel,
			settings.InternalKey("dbconsole.feature_flag.test_"+name),
			"test feature "+name,
			false,
		),
	}
}

// withTestFeatures replaces the global features slice for the duration of the
// test, restoring it on cleanup.
func withTestFeatures(t *testing.T, ff ...*Feature) {
	saved := features
	features = ff
	t.Cleanup(func() { features = saved })
}

func TestLookupFeature(t *testing.T) {
	withTestFeatures(t, testFeatureA, testFeatureB)

	require.Equal(t, testFeatureA, LookupFeature("test_a"))
	require.Equal(t, testFeatureB, LookupFeature("test_b"))
	require.Nil(t, LookupFeature("nonexistent"))
}

func TestGetFeatures(t *testing.T) {
	withTestFeatures(t, testFeatureA, testFeatureB)

	ff := GetFeatures()
	require.Len(t, ff, 2)
	require.Equal(t, "test_a", ff[0].Name)
	require.Equal(t, "test_b", ff[1].Name)
}

func TestRequireFeatureEnabled(t *testing.T) {
	withTestFeatures(t, testFeatureA)
	st := cluster.MakeTestingClusterSettings()

	t.Run("disabled feature returns 403", func(t *testing.T) {
		w := httptest.NewRecorder()
		ok := requireFeatureEnabled(
			context.Background(), "test_a", &st.SV, w,
		)
		require.False(t, ok)
		require.Equal(t, http.StatusForbidden, w.Code)

		var resp ErrorResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Contains(t, resp.Error, "not enabled")
	})

	t.Run("enabled feature passes", func(t *testing.T) {
		ctx := context.Background()
		testFeatureA.Setting.Override(ctx, &st.SV, true)
		t.Cleanup(func() {
			testFeatureA.Setting.Override(ctx, &st.SV, false)
		})

		w := httptest.NewRecorder()
		ok := requireFeatureEnabled(ctx, "test_a", &st.SV, w)
		require.True(t, ok)
		require.Empty(t, w.Body.Bytes())
	})

	t.Run("unknown feature returns 403", func(t *testing.T) {
		w := httptest.NewRecorder()
		ok := requireFeatureEnabled(
			context.Background(), "nonexistent", &st.SV, w,
		)
		require.False(t, ok)
		require.Equal(t, http.StatusForbidden, w.Code)
	})
}

func TestListFeatures(t *testing.T) {
	withTestFeatures(t, testFeatureA, testFeatureB)
	st := cluster.MakeTestingClusterSettings()

	ctx := context.Background()
	testFeatureA.Setting.Override(ctx, &st.SV, true)

	api := &ApiV2DBConsole{Settings: st}
	req := httptest.NewRequest("GET", "/features", nil)
	w := httptest.NewRecorder()
	api.ListFeatures(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var result FeaturesResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result.Features, 2)

	require.Equal(t, "test_a", result.Features[0].Name)
	require.Equal(t, "Test test_a", result.Features[0].Title)
	require.Equal(t, "/feature/test_a", result.Features[0].RoutePath)
	require.True(t, result.Features[0].Enabled)

	require.Equal(t, "test_b", result.Features[1].Name)
	require.False(t, result.Features[1].Enabled)
}
