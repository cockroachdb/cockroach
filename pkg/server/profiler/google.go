// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"

	gcprofiler "cloud.google.com/go/profiler"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var googleCloudProfilerEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.google_cloud_profiler.enabled",
	"set to true to enable google cloud profiler, false to disable; default is false",
	true) // default should be false, setting to true for testing, CHANGE BACK.

// InitGoogleProfiler initializes the Google Cloud Profiler if enabled via
// cluster setting. The profiler is automatically enabled on GCP clusters
// and disabled elsewhere, but can be manually overridden via the cluster setting.
//
// The profiler initialization should be done as early as possible in the
// server startup process for best results.
func InitGoogleProfiler(ctx context.Context, st *cluster.Settings) {
	// Detect cloud provider to determine smart default
	provider, _ := cloudinfo.GetInstanceClass(ctx)

	// On GCP, enable by default unless explicitly disabled
	// On other clouds, disable by default unless explicitly enabled
	var enabled bool
	if provider == "gcp" {
		enabled = googleCloudProfilerEnabled.Get(&st.SV)
		if enabled {
			cfg := gcprofiler.Config{
				Service: "cockroachdb",
				// ServiceVersion: "1.0.0",
				// ProjectID must be set if not running on GCP.
				ProjectID: "cockroach-ephemeral",
			}

			if err := gcprofiler.Start(cfg); err != nil {
				log.Warningf(ctx, "failed to start google profiler: %v", err)
			} else {
				log.Infof(ctx, "Google Cloud Profiler started successfully on %s", provider)
			}
		} else {
			log.Infof(ctx, "Google Cloud Profiler disabled via cluster setting on GCP")
		}
	} else {
		log.Infof(ctx, "Google Cloud Profiler disabled (detected cloud: %s).", provider)
	}
}
