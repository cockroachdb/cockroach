// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
)

var envExperimentalDRPCEnabled = envutil.EnvOrDefaultBool("COCKROACH_EXPERIMENTAL_DRPC_ENABLED", false)

// ExperimentalDRPCEnabled determines whether a drpc server accepting BatchRequest
// is enabled. This server is experimental and completely unsuitable to production
// usage (for example, does not implement authorization checks).
var ExperimentalDRPCEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"rpc.experimental_drpc.enabled",
	"if true, use drpc to execute Batch RPCs (instead of gRPC)",
	envExperimentalDRPCEnabled,
	settings.WithValidateBool(func(values *settings.Values, b bool) error {
		// drpc support is highly experimental and should not be enabled in production.
		// Since authorization is not implemented, we only even host the server if the
		// env var is set or it's a CRDB test build. Consequently, these are prereqs
		// for setting the cluster setting.
		if b && !(envExperimentalDRPCEnabled || buildutil.CrdbTestBuild) {
			return errors.New("experimental drpc is not allowed in this environment")
		}
		return nil
	}))
