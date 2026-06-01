// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file is a compiling demonstration of the feature-gate API shape for the
// prototype. It exercises both kinds of gate that featureflag.Register can
// build: a license-only gate (modeled on multi-region) and a license+setting
// gate (modeled on changefeed), and shows how Enabled evaluates each. It is
// deliberately self-contained and does not touch any real call sites; the
// setting it registers is a throwaway used solely to drive the operator gate.
package featureflag_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/featureflag"
	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// changefeedEnabled is a throwaway operator setting that stands in for one of
// the real feature.*.enabled bools. It is registered at package scope because
// settings registration must happen at init and panics on duplicate names.
var changefeedEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"test.featuregate.changefeed.enabled",
	"test-only setting for the feature-gate example",
	true,
)

// TestFeatureGateExample demonstrates the feature-gate API shape by building
// and evaluating both kinds of gate. Because pkg/server/license is not imported
// here, GetLicenseHook stays nil and the license gate is permissive, which lets
// the test focus on the operator-setting path.
func TestFeatureGateExample(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	// A license-only gate (like multi-region) has no operator setting. With no
	// license installed the license gate is permissive, so Enabled allows it.
	mrGate := featureflag.Register(licensepb.Feature_MULTIREGION)
	require.NoError(t, mrGate.Enabled(ctx, st))

	// A license+setting gate (like changefeed) additionally consults an operator
	// setting. With the setting defaulting true and no license installed, both
	// gates allow the feature.
	cfGate := featureflag.Register(
		licensepb.Feature_CHANGEFEED,
		featureflag.WithSetting(changefeedEnabled),
	)
	require.NoError(t, cfGate.Enabled(ctx, st))

	// Disabling the operator setting denies the feature even though the license
	// gate remains permissive.
	changefeedEnabled.Override(ctx, &st.SV, false)
	require.Error(t, cfGate.Enabled(ctx, st))
}
