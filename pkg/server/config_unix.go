// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build !windows

package server

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type rlimit struct {
	cur, max uint64
}

func setOpenFileLimitInner(physicalStoreCount int) (int, error) {
	minimumOpenFileLimit := uint64(physicalStoreCount*engine.MinimumMaxOpenFiles + minimumNetworkFileDescriptors)
	networkConstrainedFileLimit := uint64(physicalStoreCount*engine.RecommendedMaxOpenFiles + minimumNetworkFileDescriptors)
	recommendedOpenFileLimit := uint64(physicalStoreCount*engine.RecommendedMaxOpenFiles + recommendedNetworkFileDescriptors)
	var rLimit rlimit
	if err := getrlimit_nofile(&rLimit); err != nil {
		if log.V(1) {
			log.Infof(context.TODO(), "could not get rlimit; setting maxOpenFiles to the default value %d - %s", engine.DefaultMaxOpenFiles, err)
		}
		return engine.DefaultMaxOpenFiles, nil
	}

	// The max open file descriptor limit is too low.
	if rLimit.max < minimumOpenFileLimit {
		return 0, fmt.Errorf("hard open file descriptor limit of %d is under the minimum required %d\n%s",
			rLimit.max,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	// If current open file descriptor limit is higher than the recommended
	// value, we can just use the default value.
	if rLimit.cur > recommendedOpenFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// If the current limit is less than the recommended limit, set the current
	// limit to the minimum of the max limit or the recommendedOpenFileLimit.
	var newCurrent uint64
	if rLimit.max > recommendedOpenFileLimit {
		newCurrent = recommendedOpenFileLimit
	} else {
		newCurrent = rLimit.max
	}
	if rLimit.cur < newCurrent {
		if log.V(1) {
			log.Infof(context.TODO(), "setting the soft limit for open file descriptors from %d to %d",
				rLimit.cur, newCurrent)
		}
		rLimit.cur = newCurrent
		if err := setrlimit_nofile(&rLimit); err != nil {
			return 0, err
		}
		// Sadly, the current limit is not always set as expected, (e.g. OSX)
		// so fetch the limit again to see the new current limit.
		if err := getrlimit_nofile(&rLimit); err != nil {
			return 0, err
		}
		if log.V(1) {
			log.Infof(context.TODO(), "soft open file descriptor limit is now %d", rLimit.cur)
		}
	}

	// The current open file descriptor limit is still too low.
	if uint64(rLimit.cur) < minimumOpenFileLimit {
		return 0, fmt.Errorf("soft open file descriptor limit of %d is under the minimum required %d and cannot be increased\n%s",
			rLimit.cur,
			minimumOpenFileLimit,
			productionSettingsWebpage)
	}

	// If we have the desired number, just use the default values.
	if rLimit.cur >= recommendedOpenFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// We're still below the recommended amount, we should always show a
	// warning.
	log.Warningf(context.TODO(), "soft open file descriptor limit %d is under the recommended limit %d; this may decrease performance\n%s",
		rLimit.cur,
		recommendedOpenFileLimit,
		productionSettingsWebpage)

	// if we have no physical stores, return 0.
	if physicalStoreCount == 0 {
		return 0, nil
	}

	// If we have more than enough file descriptors to hit the recommend number
	// for each store, than only constrain the network ones by giving the stores
	// their full recommended number.
	if rLimit.cur >= networkConstrainedFileLimit {
		return engine.DefaultMaxOpenFiles, nil
	}

	// Always sacrifice all but the minimum needed network descriptors to be
	// used by the stores.
	return int(rLimit.cur-minimumNetworkFileDescriptors) / physicalStoreCount, nil
}
