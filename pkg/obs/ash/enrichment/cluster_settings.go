// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package enrichment hosts the ASH sample enrichment subsystem: a
// per-tenant cache of per-execution attributes (database, user,
// query, plan_gist, canary_stats, txn_id, session_id) that the ASH
// sampler resolves alongside each sample. Each gateway writes its
// own executions into the cache at exec time; the sampler resolves
// attributes for samples produced on remote nodes via the
// Status.GetASHEnrichmentData RPC.
//
// Design doc:
// https://docs.google.com/document/d/1o9iMUCiIHsf5OuCOg-ci-dkzjrZZRwaOAK3_esIyEoE
package enrichment

import "github.com/cockroachdb/cockroach/pkg/settings"

// Enabled is the master switch for the ASH enrichment subsystem.
// When false, the gateway cache is a no-op for both Put and Get,
// the sampler skips the resolution phase, and the
// GetASHEnrichmentData RPC handler returns empty responses.
// App-name resolution continues to work via the legacy
// AppNameMappings path (preserved for mixed-version compatibility,
// removed in 26.4).
// Defaults to false during the 26.3 rollout.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"obs.ash.enrichment.enabled",
	"if true, the ASH sampler enriches each sample with per-execution attributes "+
		"(database, user, query, plan_gist, canary_stats, txn_id, session_id) cached "+
		"on the gateway node",
	false,
	settings.WithPublic,
)
