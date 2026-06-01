// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package featureflag

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/server/license/licensepb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// FeatureGate combines a license-entitlement check with an optional operator
// cluster setting (and stubs for experimental/cloud-only gating) into a single
// enforcement point for a feature.
//
// The license-entitlement check reads the installed license proto directly (via
// GetLicenseHook) and asks whether the gate's feature is among the entitlements
// the license grants. This deliberately avoids relying on denormalized cluster
// settings so that entitlements remain a single source of truth.
//
// The operator setting, when supplied via WithSetting, is one of the existing
// feature.*.enabled cluster-setting bools. The gate does not register a new
// setting; it reuses the one the caller already owns, so that operators retain
// the ability to turn a feature off even when the license permits it.
//
// Lifecycle: a FeatureGate is constructed once, typically in a package-level
// var via Register, and is immutable thereafter. It is evaluated per-use by
// calling Enabled, which performs the ordered license-then-operator checks.
type FeatureGate struct {
	// feature is the license entitlement this gate guards.
	feature licensepb.Feature

	// setting is the operator cluster setting that can additionally disable
	// this feature, or nil if the gate has no operator setting.
	setting *settings.BoolSetting

	// experimental marks the gate as experimental. This is currently a stub:
	// Enabled does not yet enforce any experimental semantics.
	experimental bool

	// cloudOnly marks the gate as available only in cloud deployments. This is
	// currently a stub: Enabled does not yet enforce any cloud-only semantics.
	cloudOnly bool

	// name is the human-readable name used in error messages. It defaults to
	// the feature's enum String() when WithName is not supplied.
	name string
}

// Option configures a FeatureGate during construction. Options follow the
// functional-options pattern and are applied in order by Register.
type Option func(*FeatureGate)

// Register constructs a FeatureGate for the given license feature, applies the
// supplied options, and returns it. It is typically called once to initialize a
// package-level var. If no WithName option is supplied, the gate's display name
// defaults to the feature's enum String().
func Register(feature licensepb.Feature, opts ...Option) *FeatureGate {
	g := &FeatureGate{
		feature: feature,
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.name == "" {
		g.name = feature.String()
	}
	return g
}

// WithSetting attaches an existing operator cluster setting to the gate. The
// gate does not register a new setting; the caller supplies one of the existing
// feature.*.enabled bools. When the setting is false, Enabled denies the
// feature even if the license permits it.
func WithSetting(s *settings.BoolSetting) Option {
	return func(g *FeatureGate) {
		g.setting = s
	}
}

// WithName overrides the display name used in error messages. Without it, the
// gate's name defaults to the feature's enum String().
func WithName(name string) Option {
	return func(g *FeatureGate) {
		g.name = name
	}
}

// WithExperimental marks the gate as experimental.
//
// TODO: the evaluation semantics for experimental gates (build tag? version
// gate? injected setting?) are an open design question for David. Enabled does
// not yet enforce experimental gating.
func WithExperimental() Option {
	return func(g *FeatureGate) {
		g.experimental = true
	}
}

// WithCloudOnly marks the gate as available only in cloud deployments.
//
// TODO: the evaluation semantics for cloud-only gates are an open design
// question for David. Enabled does not yet enforce cloud-only gating.
func WithCloudOnly() Option {
	return func(g *FeatureGate) {
		g.cloudOnly = true
	}
}

// GetLicenseHook returns the currently installed license, or nil if none is
// installed. It is populated by an init() in pkg/server/license to avoid an
// import cycle (server/license imports featureflag, not the reverse). When
// nil, the license gate is permissive.
var GetLicenseHook func(st *cluster.Settings) (*licensepb.License, error)

// Enabled evaluates the gate against the current cluster state and returns nil
// if the feature is permitted, or an error describing why it is denied.
//
// The checks are evaluated in order:
//
//  1. License entitlement. If no license is installed (or no hook is wired up),
//     the gate is permissive. If a license is present, the feature must appear
//     in the license's entitlement list; otherwise the call is denied with an
//     InsufficientPrivilege error.
//  2. Operator setting. If the gate has an operator setting and it is disabled,
//     the call is denied with an OperatorIntervention error and a denial
//     telemetry counter is incremented.
//
// Experimental and cloud-only gating are not yet enforced.
func (g *FeatureGate) Enabled(ctx context.Context, st *cluster.Settings) error {
	// License gate first.
	//
	// During this prototype, the absence of a license is permissive: with no
	// hook wired up or no license installed we allow the feature and fall
	// through to the operator-setting gate.
	if GetLicenseHook != nil {
		lic, err := GetLicenseHook(st)
		if err != nil {
			return errors.Wrap(err, "reading license")
		}
		if lic != nil {
			// TODO: an installed license whose Features list is empty predates
			// the entitlements field. This prototype treats that as permissive
			// (allow). Whether empty-features should be strict (deny) or
			// permissive (allow) is an open decision for David.
			if len(lic.Features) > 0 && !slices.Contains(lic.Features, g.feature) {
				err := pgerror.Newf(
					pgcode.InsufficientPrivilege,
					"feature %s is not included in your license",
					g.name,
				)
				return errors.WithHint(err, "upgrade your license to enable this feature")
			}
		}
	}

	// Operator setting gate second.
	if g.setting != nil {
		if !g.setting.Get(&st.SV) {
			telemetry.Inc(sqltelemetry.FeatureDeniedByFeatureFlagCounter)
			return pgerror.Newf(
				pgcode.OperatorIntervention,
				"feature %s was disabled by the database administrator",
				g.name,
			)
		}
	}

	return nil
}
