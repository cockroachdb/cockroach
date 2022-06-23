// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/rate"
)

var nilPacer = &noopPacer{}

type limiter interface {
	DelayN(now time.Time, n int) time.Duration
	AllowN(now time.Time, n int) bool
	Burst() int
}

// pacer is the interface for flush and compaction rate limiters. The rate limiter
// is possible applied on each iteration step of a flush or compaction. This is to
// limit background IO usage so that it does not contend with foreground traffic.
type pacer interface {
	maybeThrottle(bytesIterated uint64) error
}

// deletionPacerInfo contains any info from the db necessary to make deletion
// pacing decisions.
type deletionPacerInfo struct {
	freeBytes     uint64
	obsoleteBytes uint64
	liveBytes     uint64
}

// deletionPacer rate limits deletions of obsolete files. This is necessary to
// prevent overloading the disk with too many deletions too quickly after a
// large compaction, or an iterator close. On some SSDs, disk performance can be
// negatively impacted if too many blocks are deleted very quickly, so this
// mechanism helps mitigate that.
type deletionPacer struct {
	limiter               limiter
	freeSpaceThreshold    uint64
	obsoleteBytesMaxRatio float64

	getInfo func() deletionPacerInfo
}

// newDeletionPacer instantiates a new deletionPacer for use when deleting
// obsolete files. The limiter passed in must be a singleton shared across this
// pebble instance.
func newDeletionPacer(limiter limiter, getInfo func() deletionPacerInfo) *deletionPacer {
	return &deletionPacer{
		limiter: limiter,
		// If there are less than freeSpaceThreshold bytes of free space on
		// disk, do not pace deletions at all.
		freeSpaceThreshold: 16 << 30, // 16 GB
		// If the ratio of obsolete bytes to live bytes is greater than
		// obsoleteBytesMaxRatio, do not pace deletions at all.
		obsoleteBytesMaxRatio: 0.20,

		getInfo: getInfo,
	}
}

// limit applies rate limiting if the current free disk space is more than
// freeSpaceThreshold, and the ratio of obsolete to live bytes is less than
// obsoleteBytesMaxRatio.
func (p *deletionPacer) limit(amount uint64, info deletionPacerInfo) error {
	obsoleteBytesRatio := float64(1.0)
	if info.liveBytes > 0 {
		obsoleteBytesRatio = float64(info.obsoleteBytes) / float64(info.liveBytes)
	}
	paceDeletions := info.freeBytes > p.freeSpaceThreshold &&
		obsoleteBytesRatio < p.obsoleteBytesMaxRatio
	if paceDeletions {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			d := p.limiter.DelayN(time.Now(), burst)
			if d == rate.InfDuration {
				return errors.Errorf("pacing failed")
			}
			time.Sleep(d)
			amount -= uint64(burst)
		}
		d := p.limiter.DelayN(time.Now(), int(amount))
		if d == rate.InfDuration {
			return errors.Errorf("pacing failed")
		}
		time.Sleep(d)
	} else {
		burst := p.limiter.Burst()
		for amount > uint64(burst) {
			// AllowN will subtract burst if there are enough tokens available,
			// else leave the tokens untouched. That is, we are making a
			// best-effort to account for this activity in the limiter, but by
			// ignoring the return value, we do the activity instantaneously
			// anyway.
			p.limiter.AllowN(time.Now(), burst)
			amount -= uint64(burst)
		}
		p.limiter.AllowN(time.Now(), int(amount))
	}
	return nil
}

// maybeThrottle slows down a deletion of this file if it's faster than
// opts.Experimental.MinDeletionRate.
func (p *deletionPacer) maybeThrottle(bytesToDelete uint64) error {
	return p.limit(bytesToDelete, p.getInfo())
}

type noopPacer struct{}

func (p *noopPacer) maybeThrottle(_ uint64) error {
	return nil
}
