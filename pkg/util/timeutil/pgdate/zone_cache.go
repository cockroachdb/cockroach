package pgdate

import (
	"fmt"
	"sync"
	"time"
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
		sync.Mutex
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
func (z *zoneCache) FixedZone(hours, minutes int) *time.Location {
	offset := hours*60 + minutes
	z.mu.Lock()
	ret, ok := z.mu.fixed[offset]
	z.mu.Unlock()

	if !ok {
		if minutes < 0 {
			minutes *= -1
		}
		ret = time.FixedZone(fmt.Sprintf("%+03d%02d", hours, minutes), offset*60)
		z.mu.Lock()
		z.mu.fixed[offset] = ret
		z.mu.Unlock()
	}

	return ret
}
