// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
)

const maxConcurrentRestoreWorkers = 32

// The maximum is not enforced since if the maximum is reduced in the future that
// may cause the cluster setting to fail.
var numRestoreWorkers = settings.RegisterIntSetting(
	"kv.bulk_io_write.restore_node_concurrency",
	fmt.Sprintf("the number of workers processing a restore per job per node; maximum %d",
		maxConcurrentRestoreWorkers),
	1, /* default */
	settings.PositiveInt,
)

// bulkThrottler is a bulk IO throttler.
type bulkThrottler struct {
	name          string
	workerLimiter limit.ConcurrentRequestLimiter
}

func (t *bulkThrottler) acquireRestoreWorker(ctx context.Context) (limit.Reservation, error) {
	return t.workerLimiter.Begin(ctx)
}

func (t *bulkThrottler) updateConfig(config bulkThrottlerConfig) {
	t.workerLimiter.SetLimit(config.restoreWorkerLimit)
}

type bulkThrottlerConfig struct {
	restoreWorkerLimit int
}

// newThrottler creates a new throttler with the specified configuration.
func newThrottler(name string, config bulkThrottlerConfig) *bulkThrottler {
	t := &bulkThrottler{
		name:          name,
		workerLimiter: limit.MakeConcurrentRequestLimiter("restore-workeer", config.restoreWorkerLimit),
	}
	t.updateConfig(config)
	return t
}

// nodeBulkThrottler is the singleton throttler of bulk jobs.
var nodeBulkThrottler = struct {
	sync.Once
	*bulkThrottler
}{}

// nodeLevelThrottler returns node level bulkThrottler for bulk jobs.
func nodeLevelThrottler(sv *settings.Values) *bulkThrottler {
	// Initialize node level throttler once.
	nodeBulkThrottler.Do(func() {
		if nodeBulkThrottler.bulkThrottler != nil {
			panic("bulk throttler already initialized")
		}
		getConfig := func() bulkThrottlerConfig {
			return bulkThrottlerConfig{
				restoreWorkerLimit: int(numRestoreWorkers.Get(sv)),
			}
		}
		nodeBulkThrottler.bulkThrottler = newThrottler("bulk.node.throttle", getConfig())

		// Update node throttler configs when settings change.
		numRestoreWorkers.SetOnChange(sv, func(ctx context.Context) {
			nodeBulkThrottler.bulkThrottler.updateConfig(getConfig())
		})
	})

	return nodeBulkThrottler.bulkThrottler
}
