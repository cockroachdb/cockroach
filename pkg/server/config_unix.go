// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !windows

package server

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// rlimit is a replacement struct for `unix.Rlimit` which abstracts
// from the possible differences in type definitions between platforms
// (e.g. GNU/Linux uses uint64, FreeBSD uses signed int64).
type rlimit struct {
	Cur, Max uint64
}

func setOpenFileLimitInner(physicalStoreCount int) (uint64, error) {
	minimumOpenFileLimit := uint64(physicalStoreCount*storage.MinimumMaxOpenFiles + minimumNetworkFileDescriptors)
	networkConstrainedFileLimit := uint64(physicalStoreCount*storage.RecommendedMaxOpenFiles + minimumNetworkFileDescriptors)
	recommendedOpenFileLimit := uint64(physicalStoreCount*storage.RecommendedMaxOpenFiles + recommendedNetworkFileDescriptors)
	var rLimit rlimit
	if err := getRlimitNoFile(&rLimit); err != nil {
		if log.V(1) {
			log.Infof(context.TODO(), "could not get rlimit; setting maxOpenFiles to the recommended value %d - %s", storage.RecommendedMaxOpenFiles, err)
		}
		return storage.RecommendedMaxOpenFiles, nil
	}

	// The max open file descriptor limit is too low.
	if rLimit.Max < minimumOpenFileLimit {
		return 0, fmt.Errorf("hard open file descriptor limit of %d is under the minimum required %d\n%s",
			rLimit.Max,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	// If the current limit is less than the recommended limit, set the current
	// limit to the minimum of the max limit or the recommendedOpenFileLimit.
	var newCurrent uint64
	if rLimit.Max > recommendedOpenFileLimit {
		newCurrent = recommendedOpenFileLimit
	} else {
		newCurrent = rLimit.Max
	}
	if rLimit.Cur < newCurrent {
		if log.V(1) {
			log.Infof(context.TODO(), "setting the soft limit for open file descriptors from %d to %d",
				rLimit.Cur, newCurrent)
		}
		oldCurrent := rLimit.Cur
		rLimit.Cur = newCurrent
		if err := setRlimitNoFile(&rLimit); err != nil {
			// It is surprising if setrlimit fails, because we were careful to check
			// getrlimit first to construct a valid limit. However, the validation
			// rules for setrlimit have been known to change between Go versions (for
			// an example, see https://github.com/golang/go/issues/30401), so we don't
			// want to fail hard if setrlimit fails. Instead we log a warning and
			// carry on. If the rlimit is really too low, we'll bail out later in this
			// function.
			log.Warningf(context.TODO(), "adjusting the limit for open file descriptors to %d failed: %s",
				rLimit.Cur, err)

			// Setting the limit to our "recommended" level failed. This may
			// be because getRlimitNoFile gave us the wrong answer (on some
			// platforms there are limits that are not reflected by
			// getrlimit()). If the previous limit is below our minimum, try
			// one more time to increase it to the minimum.
			if oldCurrent < minimumOpenFileLimit {
				rLimit.Cur = minimumOpenFileLimit
				if err := setRlimitNoFile(&rLimit); err != nil {
					log.Warningf(context.TODO(), "adjusting the limit for open file descriptors to %d failed: %s",
						rLimit.Cur, err)
				}
			}
		}
		// Sadly, even when setrlimit returns successfully, the new limit is not
		// always set as expected (e.g. on macOS), so fetch the limit again to see
		// the actual current limit.
		if err := getRlimitNoFile(&rLimit); err != nil {
			return 0, errors.Wrap(err, "getting updated soft limit for open file descriptors")
		}
		if log.V(1) {
			log.Infof(context.TODO(), "soft open file descriptor limit is now %d", rLimit.Cur)
		}
	}

	// The current open file descriptor limit is still too low.
	if rLimit.Cur < minimumOpenFileLimit {
		return 0, fmt.Errorf("soft open file descriptor limit of %d is under the minimum required %d and cannot be increased\n%s",
			rLimit.Cur,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	if rLimit.Cur < recommendedOpenFileLimit {
		// We're still below the recommended amount, we should always show a
		// warning.
		log.Warningf(context.TODO(), "soft open file descriptor limit %d is under the recommended limit %d; this may decrease performance\n%s",
			rLimit.Cur,
			recommendedOpenFileLimit,
			productionSettingsWebpage)
	}

	// If we have no physical stores, return 0.
	if physicalStoreCount == 0 {
		return 0, nil
	}

	// If the current open file descriptor limit meets or exceeds the recommended
	// value, we can divide up the current limit, less what we need for
	// networking, between the stores.
	if rLimit.Cur >= recommendedOpenFileLimit {
		return (rLimit.Cur - recommendedNetworkFileDescriptors) / uint64(physicalStoreCount), nil
	}

	// If we have more than enough file descriptors to hit the recommended number
	// for each store, than only constrain the network ones by giving the stores
	// their full recommended number.
	if rLimit.Cur >= networkConstrainedFileLimit {
		return storage.RecommendedMaxOpenFiles, nil
	}

	// Always sacrifice all but the minimum needed network descriptors to be
	// used by the stores.
	return (rLimit.Cur - minimumNetworkFileDescriptors) / uint64(physicalStoreCount), nil
}
