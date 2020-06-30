// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/time/rate"
)

// bulkIOWriteBurst is the burst for the BulkIOWriteLimiter. It is also used as
// the default value for the kv.bulk_sst.sync_size and kv.snapshot_sst.sync_size
// cluster settings.
const bulkIOWriteBurst = 512 << 10 // 512 KB

const bulkIOWriteLimiterLongWait = 500 * time.Millisecond

func limitBulkIOWrite(ctx context.Context, limiter *rate.Limiter, cost int) {
	// The limiter disallows anything greater than its burst (set to
	// BulkIOWriteLimiterBurst), so cap the batch size if it would overflow.
	//
	// TODO(dan): This obviously means the limiter is no longer accounting for the
	// full cost. I've tried calling WaitN in a loop to fully cover the cost, but
	// that didn't seem to be as smooth in practice (NB [dt]: that was when this
	// limit was done before writing the whole file, rather than on individual
	// chunks).
	if cost > bulkIOWriteBurst {
		cost = bulkIOWriteBurst
	}

	begin := timeutil.Now()
	if err := limiter.WaitN(ctx, cost); err != nil {
		log.Errorf(ctx, "error rate limiting bulk io write: %+v", err)
	}

	if d := timeutil.Since(begin); d > bulkIOWriteLimiterLongWait {
		log.Warningf(ctx, "bulk io write limiter took %s (>%s):\n%s",
			d, bulkIOWriteLimiterLongWait, debug.Stack())
	}
}

// sstWriteSyncRate wraps "kv.bulk_sst.sync_size". 0 disables syncing.
var sstWriteSyncRate = settings.RegisterByteSizeSetting(
	"kv.bulk_sst.sync_size",
	"threshold after which non-Rocks SST writes must fsync (0 disables)",
	bulkIOWriteBurst,
)

// writeFileSyncing is essentially ioutil.WriteFile -- writes data to a file
// named by filename -- but with rate limiting and periodic fsyncing controlled
// by settings and the passed limiter (should be the store's limiter). Periodic
// fsync provides smooths out disk IO, as mentioned in #20352 and #20279, and
// provides back-pressure, along with the explicit rate limiting. If the file
// does not exist, WriteFile creates it with permissions perm; otherwise
// WriteFile truncates it before writing.
func writeFileSyncing(
	ctx context.Context,
	filename string,
	data []byte,
	eng storage.Engine,
	perm os.FileMode,
	settings *cluster.Settings,
	limiter *rate.Limiter,
) error {
	chunkSize := sstWriteSyncRate.Get(&settings.SV)
	sync := true
	if chunkSize == 0 {
		chunkSize = bulkIOWriteBurst
		sync = false
	}

	f, err := eng.Create(filename)
	if err != nil {
		if strings.Contains(err.Error(), "No such file or directory") {
			return os.ErrNotExist
		}
		return err
	}

	for i := int64(0); i < int64(len(data)); i += chunkSize {
		end := i + chunkSize
		if l := int64(len(data)); end > l {
			end = l
		}
		chunk := data[i:end]

		// rate limit
		limitBulkIOWrite(ctx, limiter, len(chunk))
		_, err = f.Write(chunk)
		if err == nil && sync {
			err = f.Sync()
		}
		if err != nil {
			break
		}
	}

	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
