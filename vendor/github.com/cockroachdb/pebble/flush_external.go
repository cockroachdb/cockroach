// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"time"

	"github.com/cockroachdb/pebble/internal/private"
)

// flushExternalTable is installed as the private.FlushExternalTable hook.
// It's used by the internal/replay package to flush external tables into L0
// for supporting the pebble bench compact commands. Clients should use the
// replay package rather than calling this private hook directly.
func flushExternalTable(untypedDB interface{}, path string, originalMeta *fileMetadata) error {
	d := untypedDB.(*DB)
	if err := d.closed.Load(); err != nil {
		panic(err)
	}
	if d.opts.ReadOnly {
		return ErrReadOnly
	}

	d.mu.Lock()
	fileNum := d.mu.versions.getNextFileNum()
	jobID := d.mu.nextJobID
	d.mu.nextJobID++
	d.mu.Unlock()

	m := &fileMetadata{
		FileNum:        fileNum,
		Size:           originalMeta.Size,
		CreationTime:   time.Now().Unix(),
		SmallestSeqNum: originalMeta.SmallestSeqNum,
		LargestSeqNum:  originalMeta.LargestSeqNum,
	}
	if originalMeta.HasPointKeys {
		m.ExtendPointKeyBounds(d.cmp, originalMeta.SmallestPointKey, originalMeta.LargestPointKey)
	}
	if originalMeta.HasRangeKeys {
		m.ExtendRangeKeyBounds(d.cmp, originalMeta.SmallestRangeKey, originalMeta.LargestRangeKey)
	}

	// Hard link the sstable into the DB directory.
	if err := ingestLink(jobID, d.opts, d.dirname, []string{path}, []*fileMetadata{m}); err != nil {
		return err
	}
	if err := d.dataDir.Sync(); err != nil {
		return err
	}

	// Assign sequence numbers from its current value through to the
	// external file's largest sequence number. It's possible that the current
	// sequence number has already exceeded m.LargestSeqNum if there were many
	// nonoverlapping ingestions, so skip if this doesn't bump the sequence
	// number.
	d.commit.ratchetSeqNum(m.LargestSeqNum + 1)
	d.commit.mu.Lock()
	defer d.commit.mu.Unlock()

	// Apply the version edit.
	d.mu.Lock()
	d.mu.versions.logLock()
	ve := &versionEdit{
		NewFiles: []newFileEntry{{Level: 0, Meta: m}},
	}
	metrics := map[int]*LevelMetrics{
		0: {
			NumFiles:       1,
			Size:           int64(m.Size),
			BytesIngested:  m.Size,
			TablesIngested: 1,
		},
	}
	err := d.mu.versions.logAndApply(jobID, ve, metrics, false /* forceRotation */, func() []compactionInfo {
		return d.getInProgressCompactionInfoLocked(nil)
	})
	if err != nil {
		// NB: logAndApply will release d.mu.versions.logLock  unconditionally.
		d.mu.Unlock()
		if err2 := ingestCleanup(d.opts.FS, d.dirname, []*fileMetadata{m}); err2 != nil {
			d.opts.Logger.Infof("flush external cleanup failed: %v", err2)
		}
		return err
	}
	d.updateReadStateLocked(d.opts.DebugCheck)
	d.updateTableStatsLocked(ve.NewFiles)
	d.deleteObsoleteFiles(jobID, true /* waitForOngoing */)
	d.maybeScheduleCompaction()
	d.mu.Unlock()
	return nil
}

// ratchetSeqNum is a hook for allocating and publishing sequence numbers up
// to a specific absolute value. Its first parameter is a *pebble.DB and its
// second is the new next sequence number. RatchetSeqNum does nothing if the
// next sequence is already greater than or equal to nextSeqNum.
//
// This function is used by the internal/replay package to ensure replayed
// operations receive the same absolute sequence number.
func ratchetSeqNum(untypedDB interface{}, nextSeqNum uint64) {
	d := untypedDB.(*DB)
	d.commit.ratchetSeqNum(nextSeqNum)
}

func init() {
	private.FlushExternalTable = flushExternalTable
	private.RatchetSeqNum = ratchetSeqNum
}
