package flakystorage

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

func MaybeWrapStorage(ctx context.Context, settings *cluster.Settings, storage cloud.ExternalStorage) cloud.ExternalStorage {
	if !enabled.Get(&settings.SV) {
		return storage
	}
	return &flakyStorage{
		wrappedStorage: storage,
		throttle:       defaultThrottle,
	}
}

type errorThrottle struct {
	sync.Mutex
	lastErr time.Time
}

var defaultThrottle = &errorThrottle{}

func (t *errorThrottle) injectErr(ctx context.Context, settings *cluster.Settings, opName string, objectName string) error {
	t.Lock()
	defer t.Unlock()

	if !enabled.Get(&settings.SV) {
		return nil
	}

	if timeutil.Since(t.lastErr) < minErrorInterval.Get(&settings.SV) {
		return nil
	}

	if rand.Float64() < errorProbability.Get(&settings.SV) {
		log.Infof(ctx, "injected error for %s %s", opName, objectName)
		t.lastErr = timeutil.Now()
		// Construct an error each time so that the error contains the stack trace.
		return errors.Newf("%s failed: injected error for '%s'", opName, objectName)
	}

	return nil
}
