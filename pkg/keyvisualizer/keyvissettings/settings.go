package keyvissettings

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"time"
)

var Enabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"keyvisualizer.job.enabled",
	"enable the use of the key visualizer", false)


var SampleInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"keyvisualizer.job.sample_interval",
	"the frequency at which the key visualizer job runs",
	10*time.Second,
	settings.NonNegativeDuration,
)

