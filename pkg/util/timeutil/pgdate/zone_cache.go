// Copyright 2018 The Cockroach Authors.
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

package pgdate

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// zoneCacheInstance saves quite a bit of time resolving time.Location
// instances.
var zoneCacheInstance = zoneCache{}

func init() {
	zoneCacheInstance.mu.named = make(map[string]*zoneCacheEntry)
	zoneCacheInstance.mu.fixed = make(map[int]*time.Location)
}

type zoneCache struct {
	mu struct {
		syncutil.Mutex
		named map[string]*zoneCacheEntry
		fixed map[int]*time.Location
	}
}

type zoneCacheEntry struct {
	loc *time.Location
	err error
}

// LoadLocation wraps time.LoadLocation which does not perform
// caching internally and which requires many disk accesses to
// locate the named zoneinfo file.
func (z *zoneCache) LoadLocation(zone string) (*time.Location, error) {
	z.mu.Lock()
	entry, ok := z.mu.named[zone]
	z.mu.Unlock()

	if !ok {
		loc, err := time.LoadLocation(zone)

		entry = &zoneCacheEntry{loc: loc, err: err}
		z.mu.Lock()
		z.mu.named[zone] = entry
		z.mu.Unlock()
	}
	return entry.loc, entry.err
}

// FixedZone wraps time.FixedZone.
func (z *zoneCache) FixedZone(hours, minutes, seconds int) *time.Location {
	offset := (hours*60+minutes)*60 + seconds
	z.mu.Lock()
	ret, ok := z.mu.fixed[offset]
	z.mu.Unlock()

	if !ok {
		if minutes < 0 {
			minutes *= -1
		}
		if seconds < 0 {
			seconds *= -1
		}
		ret = time.FixedZone(fmt.Sprintf("%+03d%02d%02d", hours, minutes, seconds), offset)
		z.mu.Lock()
		z.mu.fixed[offset] = ret
		z.mu.Unlock()
	}

	return ret
}
