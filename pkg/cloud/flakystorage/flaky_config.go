package flakystorage

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloud.flaky_storage.enabled",
	"if true, enables the flaky storage wrapper which randomly fail cloud operations",
	false,
)

var errorProbability = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"cloud.flaky_storage.error_probability",
	"the probability of any operation failing due to an injected error",
	0.05,
)

var minErrorInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"cloud.flaky_storage.min_error_interval",
	"the minimum amount of time between injected errors",
	time.Minute,
)

func MaybeWrapStorage(
	ctx context.Context, settings *cluster.Settings, storage cloud.ExternalStorage,
) cloud.ExternalStorage {
	if !enabled.Get(&settings.SV) {
		return storage
	}
	return &flakyStorage{
		wrappedStorage: storage,
		shouldInject: func() bool {
			return defaultThrottle.shouldInject(settings)
		},
	}
}

func ConfigureEarlyBoot(settings *cluster.Settings) {
	earlyBootSettings.Store(settings)
}

var earlyBootSettings atomic.Pointer[cluster.Settings]

func WrapStorage(probability float64, storage cloud.ExternalStorage) cloud.ExternalStorage {
	// TODO(jeffswenson): this should be an environment variable or
	// controlled by a flag.
	return &flakyStorage{
		wrappedStorage: storage,
		shouldInject: func() bool {
			return defaultThrottle.shouldInject(earlyBootSettings.Load())
		},
	}
}

type errorThrottle struct {
	sync.Mutex
	lastErr time.Time
}

var defaultThrottle = &errorThrottle{}

func (t *errorThrottle) shouldInject(settings *cluster.Settings) bool {
	t.Lock()
	defer t.Unlock()

	if !enabled.Get(&settings.SV) {
		return false
	}

	if timeutil.Since(t.lastErr) < minErrorInterval.Get(&settings.SV) {
		return false
	}

	if rand.Float64() < errorProbability.Get(&settings.SV) {
		t.lastErr = timeutil.Now()
		return true
	}

	return false
}
