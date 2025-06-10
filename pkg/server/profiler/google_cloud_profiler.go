// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"

	gcprofiler "cloud.google.com/go/profiler"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// InitGoogleProfiler initializes the Google Cloud Profiler if enabled via
// environment variable.
//
// The profiler initialization should be done as early as possible in the
// server startup process for best results.
func InitGoogleProfiler(ctx context.Context) {
	// Detect cloud provider to determine smart default
	provider, _ := cloudinfo.GetInstanceRegion(ctx)

	// should be false, but can set to true for easy testing
	enabled := envutil.EnvOrDefaultBool("COCKROACH_GOOGLE_CONTINUOUS_PROFILER_ENABLED", false)
	if provider == "gcp" {
		if enabled {
			log.Infof(ctx, "Google Cloud Profiler enabled via environment variable")
			cfg := gcprofiler.Config{
				Service:        "cockroachdb",
				ServiceVersion: build.BinaryVersion(),
				// numProfiles per minute
			}
			if err := gcprofiler.Start(cfg); err != nil {
				log.Warningf(ctx, "failed to start google profiler: %v", err)
			} else {
				log.Infof(ctx, "Google Cloud Profiler started successfully on %s", provider)
			}
		} else {
			log.Infof(ctx, "Google Cloud Profiler disabled via environment variable")
			return
		}
	} else {
		log.Infof(ctx, "Google Cloud Profiler disabled (detected cloud: %s)", provider)
		return
	}

}
