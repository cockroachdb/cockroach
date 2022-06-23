// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/record"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/atomicfs"
)

const numLevels = manifest.NumLevels

const manifestMarkerName = `manifest`

// Provide type aliases for the various manifest structs.
type bulkVersionEdit = manifest.BulkVersionEdit
type deletedFileEntry = manifest.DeletedFileEntry
type fileMetadata = manifest.FileMetadata
type newFileEntry = manifest.NewFileEntry
type version = manifest.Version
type versionEdit = manifest.VersionEdit
type versionList = manifest.VersionList

// versionSet manages a collection of immutable versions, and manages the
// creation of a new version from the most recent version. A new version is
// created from an existing version by applying a version edit which is just
// like it sounds: a delta from the previous version. Version edits are logged
// to the MANIFEST file, which is replayed at startup.
type versionSet struct {
	// WARNING: The following struct `atomic` contains fields are accessed atomically.
	//
	// Go allocations are guaranteed to be 64-bit aligned which we take advantage
	// of by placing the 64-bit fields which we access atomically at the beginning
	// of the versionSet struct.
	// For more information, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG.
	atomic struct {
		logSeqNum uint64 // next seqNum to use for WAL writes

		// The upper bound on sequence numbers that have been assigned so far.
		// A suffix of these sequence numbers may not have been written to a
		// WAL. Both logSeqNum and visibleSeqNum are atomically updated by the
		// commitPipeline.
		visibleSeqNum uint64 // visible seqNum (<= logSeqNum)

		// Number of bytes present in sstables being written by in-progress
		// compactions. This value will be zero if there are no in-progress
		// compactions. Updated and read atomically.
		atomicInProgressBytes int64
	}

	// Immutable fields.
	dirname string
	// Set to DB.mu.
	mu             *sync.Mutex
	opts           *Options
	fs             vfs.FS
	cmp            Compare
	cmpName        string
	diskAvailBytes func() uint64
	// Dynamic base level allows the dynamic base level computation to be
	// disabled. Used by tests which want to create specific LSM structures.
	dynamicBaseLevel bool

	// Mutable fields.
	versions versionList
	picker   compactionPicker

	metrics Metrics

	// A pointer to versionSet.addObsoleteLocked. Avoids allocating a new closure
	// on the creation of every version.
	obsoleteFn        func(obsolete []*manifest.FileMetadata)
	obsoleteTables    []*manifest.FileMetadata
	obsoleteManifests []fileInfo
	obsoleteOptions   []fileInfo

	// Zombie tables which have been removed from the current version but are
	// still referenced by an inuse iterator.
	zombieTables map[FileNum]uint64 // filenum -> size

	// minUnflushedLogNum is the smallest WAL log file number corresponding to
	// mutations that have not been flushed to an sstable.
	minUnflushedLogNum FileNum

	// The next file number. A single counter is used to assign file numbers
	// for the WAL, MANIFEST, sstable, and OPTIONS files.
	nextFileNum FileNum

	// The current manifest file number.
	manifestFileNum FileNum
	manifestMarker  *atomicfs.Marker

	manifestFile vfs.File
	manifest     *record.Writer
	setCurrent   func(FileNum) error

	writing    bool
	writerCond sync.Cond
}

func (vs *versionSet) init(
	dirname string,
	opts *Options,
	marker *atomicfs.Marker,
	setCurrent func(FileNum) error,
	mu *sync.Mutex,
) {
	vs.dirname = dirname
	vs.mu = mu
	vs.writerCond.L = mu
	vs.opts = opts
	vs.fs = opts.FS
	vs.cmp = opts.Comparer.Compare
	vs.cmpName = opts.Comparer.Name
	vs.dynamicBaseLevel = true
	vs.versions.Init(mu)
	vs.obsoleteFn = vs.addObsoleteLocked
	vs.zombieTables = make(map[FileNum]uint64)
	vs.nextFileNum = 1
	vs.manifestMarker = marker
	vs.setCurrent = setCurrent
	if vs.diskAvailBytes == nil {
		vs.diskAvailBytes = func() uint64 { return math.MaxUint64 }
	}
}

// create creates a version set for a fresh DB.
func (vs *versionSet) create(
	jobID int,
	dirname string,
	opts *Options,
	marker *atomicfs.Marker,
	setCurrent func(FileNum) error,
	mu *sync.Mutex,
) error {
	vs.init(dirname, opts, marker, setCurrent, mu)
	newVersion := &version{}
	vs.append(newVersion)
	var err error

	vs.picker = newCompactionPicker(newVersion, vs.opts, nil, vs.metrics.levelSizes(), vs.diskAvailBytes)

	// Note that a "snapshot" version edit is written to the manifest when it is
	// created.
	vs.manifestFileNum = vs.getNextFileNum()
	err = vs.createManifest(vs.dirname, vs.manifestFileNum, vs.minUnflushedLogNum, vs.nextFileNum)
	if err == nil {
		if err = vs.manifest.Flush(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST flush failed: %v", err)
		}
	}
	if err == nil {
		if err = vs.manifestFile.Sync(); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST sync failed: %v", err)
		}
	}
	if err == nil {
		// NB: setCurrent is responsible for syncing the data directory.
		if err = vs.setCurrent(vs.manifestFileNum); err != nil {
			vs.opts.Logger.Fatalf("MANIFEST set current failed: %v", err)
		}
	}

	vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
		JobID:   jobID,
		Path:    base.MakeFilepath(vs.fs, vs.dirname, fileTypeManifest, vs.manifestFileNum),
		FileNum: vs.manifestFileNum,
		Err:     err,
	})
	if err != nil {
		return err
	}
	return nil
}

// load loads the version set from the manifest file.
func (vs *versionSet) load(
	dirname string,
	opts *Options,
	manifestFileNum FileNum,
	marker *atomicfs.Marker,
	setCurrent func(FileNum) error,
	mu *sync.Mutex,
) error {
	vs.init(dirname, opts, marker, setCurrent, mu)

	vs.manifestFileNum = manifestFileNum
	manifestPath := base.MakeFilepath(opts.FS, dirname, fileTypeManifest, vs.manifestFileNum)
	manifestFilename := opts.FS.PathBase(manifestPath)

	// Read the versionEdits in the manifest file.
	var bve bulkVersionEdit
	bve.AddedByFileNum = make(map[base.FileNum]*fileMetadata)
	manifest, err := vs.fs.Open(manifestPath)
	if err != nil {
		return errors.Wrapf(err, "pebble: could not open manifest file %q for DB %q",
			errors.Safe(manifestFilename), dirname)
	}
	defer manifest.Close()
	rr := record.NewReader(manifest, 0 /* logNum */)
	for {
		r, err := rr.Next()
		if err == io.EOF || record.IsInvalidRecord(err) {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "pebble: error when loading manifest file %q",
				errors.Safe(manifestFilename))
		}
		var ve versionEdit
		err = ve.Decode(r)
		if err != nil {
			// Break instead of returning an error if the record is corrupted
			// or invalid.
			if err == io.EOF || record.IsInvalidRecord(err) {
				break
			}
			return err
		}
		if ve.ComparerName != "" {
			if ve.ComparerName != vs.cmpName {
				return errors.Errorf("pebble: manifest file %q for DB %q: "+
					"comparer name from file %q != comparer name from Options %q",
					errors.Safe(manifestFilename), dirname, errors.Safe(ve.ComparerName), errors.Safe(vs.cmpName))
			}
		}
		if err := bve.Accumulate(&ve); err != nil {
			return err
		}
		if ve.MinUnflushedLogNum != 0 {
			vs.minUnflushedLogNum = ve.MinUnflushedLogNum
		}
		if ve.NextFileNum != 0 {
			vs.nextFileNum = ve.NextFileNum
		}
		if ve.LastSeqNum != 0 {
			// logSeqNum is the _next_ sequence number that will be assigned,
			// while LastSeqNum is the last assigned sequence number. Note that
			// this behaviour mimics that in RocksDB; the first sequence number
			// assigned is one greater than the one present in the manifest
			// (assuming no WALs contain higher sequence numbers than the
			// manifest's LastSeqNum). Increment LastSeqNum by 1 to get the
			// next sequence number that will be assigned.
			vs.atomic.logSeqNum = ve.LastSeqNum + 1
		}
	}
	// We have already set vs.nextFileNum = 2 at the beginning of the
	// function and could have only updated it to some other non-zero value,
	// so it cannot be 0 here.
	if vs.minUnflushedLogNum == 0 {
		if vs.nextFileNum >= 2 {
			// We either have a freshly created DB, or a DB created by RocksDB
			// that has not had a single flushed SSTable yet. This is because
			// RocksDB bumps up nextFileNum in this case without bumping up
			// minUnflushedLogNum, even if WALs with non-zero file numbers are
			// present in the directory.
		} else {
			return base.CorruptionErrorf("pebble: malformed manifest file %q for DB %q",
				errors.Safe(manifestFilename), dirname)
		}
	}
	vs.markFileNumUsed(vs.minUnflushedLogNum)

	newVersion, _, err := bve.Apply(nil, vs.cmp, opts.Comparer.FormatKey, opts.FlushSplitBytes, opts.Experimental.ReadCompactionRate)
	if err != nil {
		return err
	}
	newVersion.L0Sublevels.InitCompactingFileInfo(nil /* in-progress compactions */)
	vs.append(newVersion)

	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.NumFiles = int64(newVersion.Levels[i].Len())
		files := newVersion.Levels[i].Slice()
		l.Size = int64(files.SizeSum())
	}

	vs.picker = newCompactionPicker(newVersion, vs.opts, nil, vs.metrics.levelSizes(), vs.diskAvailBytes)
	return nil
}

func (vs *versionSet) close() error {
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
	}
	if vs.manifestMarker != nil {
		if err := vs.manifestMarker.Close(); err != nil {
			return err
		}
	}
	return nil
}

// logLock locks the manifest for writing. The lock must be released by either
// a call to logUnlock or logAndApply.
//
// DB.mu must be held when calling this method, but the mutex may be dropped and
// re-acquired during the course of this method.
func (vs *versionSet) logLock() {
	// Wait for any existing writing to the manifest to complete, then mark the
	// manifest as busy.
	for vs.writing {
		vs.writerCond.Wait()
	}
	vs.writing = true
}

// logUnlock releases the lock for manifest writing.
//
// DB.mu must be held when calling this method.
func (vs *versionSet) logUnlock() {
	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}
	vs.writing = false
	vs.writerCond.Signal()
}

// logAndApply logs the version edit to the manifest, applies the version edit
// to the current version, and installs the new version.
//
// DB.mu must be held when calling this method and will be released temporarily
// while performing file I/O. Requires that the manifest is locked for writing
// (see logLock). Will unconditionally release the manifest lock (via
// logUnlock) even if an error occurs.
//
// inProgressCompactions is called while DB.mu is held, to get the list of
// in-progress compactions.
func (vs *versionSet) logAndApply(
	jobID int,
	ve *versionEdit,
	metrics map[int]*LevelMetrics,
	forceRotation bool,
	inProgressCompactions func() []compactionInfo,
) error {
	if !vs.writing {
		vs.opts.Logger.Fatalf("MANIFEST not locked for writing")
	}
	defer vs.logUnlock()

	if ve.MinUnflushedLogNum != 0 {
		if ve.MinUnflushedLogNum < vs.minUnflushedLogNum ||
			vs.nextFileNum <= ve.MinUnflushedLogNum {
			panic(fmt.Sprintf("pebble: inconsistent versionEdit minUnflushedLogNum %d",
				ve.MinUnflushedLogNum))
		}
	}

	// This is the next manifest filenum, but if the current file is too big we
	// will write this ve to the next file which means what ve encodes is the
	// current filenum and not the next one.
	//
	// TODO(sbhola): figure out why this is correct and update comment.
	ve.NextFileNum = vs.nextFileNum

	// LastSeqNum is set to the current upper bound on the assigned sequence
	// numbers. Note that this is exactly the behavior of RocksDB. LastSeqNum is
	// used to initialize versionSet.logSeqNum and versionSet.visibleSeqNum on
	// replay. It must be higher than or equal to any than any sequence number
	// written to an sstable, including sequence numbers in ingested files.
	// Note that LastSeqNum is not (and cannot be) the minimum unflushed sequence
	// number. This is fallout from ingestion which allows a sequence number X to
	// be assigned to an ingested sstable even though sequence number X-1 resides
	// in an unflushed memtable. logSeqNum is the _next_ sequence number that
	// will be assigned, so subtract that by 1 to get the upper bound on the
	// last assigned sequence number.
	logSeqNum := atomic.LoadUint64(&vs.atomic.logSeqNum)
	ve.LastSeqNum = logSeqNum - 1
	if logSeqNum == 0 {
		// logSeqNum is initialized to 1 in Open() if there are no previous WAL
		// or manifest records, so this case should never happen.
		vs.opts.Logger.Fatalf("logSeqNum must be a positive integer: %d", logSeqNum)
	}

	currentVersion := vs.currentVersion()
	var newVersion *version

	// Generate a new manifest if we don't currently have one, or the current one
	// is too large.
	var newManifestFileNum FileNum
	var prevManifestFileSize uint64
	if forceRotation || vs.manifest == nil || vs.manifest.Size() >= vs.opts.MaxManifestFileSize {
		newManifestFileNum = vs.getNextFileNum()
		prevManifestFileSize = uint64(vs.manifest.Size())
	}

	// Grab certain values before releasing vs.mu, in case createManifest() needs
	// to be called.
	minUnflushedLogNum := vs.minUnflushedLogNum
	nextFileNum := vs.nextFileNum

	var zombies map[FileNum]uint64
	if err := func() error {
		vs.mu.Unlock()
		defer vs.mu.Lock()

		var bve bulkVersionEdit
		if err := bve.Accumulate(ve); err != nil {
			return err
		}

		var err error
		newVersion, zombies, err = bve.Apply(currentVersion, vs.cmp, vs.opts.Comparer.FormatKey, vs.opts.FlushSplitBytes, vs.opts.Experimental.ReadCompactionRate)
		if err != nil {
			return errors.Wrap(err, "MANIFEST apply failed")
		}

		if newManifestFileNum != 0 {
			if err := vs.createManifest(vs.dirname, newManifestFileNum, minUnflushedLogNum, nextFileNum); err != nil {
				vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
					JobID:   jobID,
					Path:    base.MakeFilepath(vs.fs, vs.dirname, fileTypeManifest, newManifestFileNum),
					FileNum: newManifestFileNum,
					Err:     err,
				})
				return errors.Wrap(err, "MANIFEST create failed")
			}
		}

		w, err := vs.manifest.Next()
		if err != nil {
			return errors.Wrap(err, "MANIFEST next record write failed")
		}
		// NB: Any error from this point on is considered fatal as we don't now if
		// the MANIFEST write occurred or not. Trying to determine that is
		// fraught. Instead we rely on the standard recovery mechanism run when a
		// database is open. In particular, that mechanism generates a new MANIFEST
		// and ensures it is synced.
		if err := ve.Encode(w); err != nil {
			return errors.Wrap(err, "MANIFEST write failed")
		}
		if err := vs.manifest.Flush(); err != nil {
			return errors.Wrap(err, "MANIFEST flush failed")
		}
		if err := vs.manifestFile.Sync(); err != nil {
			return errors.Wrap(err, "MANIFEST sync failed")
		}
		if newManifestFileNum != 0 {
			// NB: setCurrent is responsible for syncing the data directory.
			if err := vs.setCurrent(newManifestFileNum); err != nil {
				return errors.Wrap(err, "MANIFEST set current failed")
			}
			vs.opts.EventListener.ManifestCreated(ManifestCreateInfo{
				JobID:   jobID,
				Path:    base.MakeFilepath(vs.fs, vs.dirname, fileTypeManifest, newManifestFileNum),
				FileNum: newManifestFileNum,
			})
		}
		return nil
	}(); err != nil {
		// Any error encountered during any of the operations in the previous
		// closure are considered fatal. Treating such errors as fatal is preferred
		// to attempting to unwind various file and b-tree reference counts, and
		// re-generating L0 sublevel metadata. This may change in the future, if
		// certain manifest / WAL operations become retryable. For more context, see
		// #1159 and #1792.
		vs.opts.Logger.Fatalf("%s", err)
		return err
	}

	// Now that DB.mu is held again, initialize compacting file info in
	// L0Sublevels.
	inProgress := inProgressCompactions()

	newVersion.L0Sublevels.InitCompactingFileInfo(inProgressL0Compactions(inProgress))

	// Update the zombie tables set first, as installation of the new version
	// will unref the previous version which could result in addObsoleteLocked
	// being called.
	for fileNum, size := range zombies {
		vs.zombieTables[fileNum] = size
	}

	// Install the new version.
	vs.append(newVersion)
	if ve.MinUnflushedLogNum != 0 {
		vs.minUnflushedLogNum = ve.MinUnflushedLogNum
	}
	if newManifestFileNum != 0 {
		if vs.manifestFileNum != 0 {
			vs.obsoleteManifests = append(vs.obsoleteManifests, fileInfo{
				fileNum:  vs.manifestFileNum,
				fileSize: prevManifestFileSize,
			})
		}
		vs.manifestFileNum = newManifestFileNum
	}

	for level, update := range metrics {
		vs.metrics.Levels[level].Add(update)
	}
	for i := range vs.metrics.Levels {
		l := &vs.metrics.Levels[i]
		l.Sublevels = 0
		if l.NumFiles > 0 {
			l.Sublevels = 1
		}
		if invariants.Enabled {
			if count := int64(newVersion.Levels[i].Len()); l.NumFiles != count {
				vs.opts.Logger.Fatalf("versionSet metrics L%d NumFiles = %d, actual count = %d", i, l.NumFiles, count)
			}
			levelFiles := newVersion.Levels[i].Slice()
			if size := int64(levelFiles.SizeSum()); l.Size != size {
				vs.opts.Logger.Fatalf("versionSet metrics L%d Size = %d, actual size = %d", i, l.Size, size)
			}
		}
	}
	vs.metrics.Levels[0].Sublevels = int32(len(newVersion.L0SublevelFiles))

	vs.picker = newCompactionPicker(newVersion, vs.opts, inProgress, vs.metrics.levelSizes(), vs.diskAvailBytes)
	if !vs.dynamicBaseLevel {
		vs.picker.forceBaseLevel1()
	}
	return nil
}

func (vs *versionSet) incrementCompactions(kind compactionKind, extraLevels []*compactionLevel) {
	switch kind {
	case compactionKindDefault:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.DefaultCount++

	case compactionKindFlush:
		vs.metrics.Flush.Count++

	case compactionKindMove:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.MoveCount++

	case compactionKindDeleteOnly:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.DeleteOnlyCount++

	case compactionKindElisionOnly:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.ElisionOnlyCount++

	case compactionKindRead:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.ReadCount++

	case compactionKindRewrite:
		vs.metrics.Compact.Count++
		vs.metrics.Compact.RewriteCount++
	}
	if len(extraLevels) > 0 {
		vs.metrics.Compact.MultiLevelCount++
	}
}

func (vs *versionSet) incrementCompactionBytes(numBytes int64) {
	atomic.AddInt64(&vs.atomic.atomicInProgressBytes, numBytes)
}

// createManifest creates a manifest file that contains a snapshot of vs.
func (vs *versionSet) createManifest(
	dirname string, fileNum, minUnflushedLogNum, nextFileNum FileNum,
) (err error) {
	var (
		filename     = base.MakeFilepath(vs.fs, dirname, fileTypeManifest, fileNum)
		manifestFile vfs.File
		manifest     *record.Writer
	)
	defer func() {
		if manifest != nil {
			manifest.Close()
		}
		if manifestFile != nil {
			manifestFile.Close()
		}
		if err != nil {
			vs.fs.Remove(filename)
		}
	}()
	manifestFile, err = vs.fs.Create(filename)
	if err != nil {
		return err
	}
	manifest = record.NewWriter(manifestFile)

	snapshot := versionEdit{
		ComparerName: vs.cmpName,
	}
	for level, levelMetadata := range vs.currentVersion().Levels {
		iter := levelMetadata.Iter()
		for meta := iter.First(); meta != nil; meta = iter.Next() {
			snapshot.NewFiles = append(snapshot.NewFiles, newFileEntry{
				Level: level,
				Meta:  meta,
			})
		}
	}

	// When creating a version snapshot for an existing DB, this snapshot VersionEdit will be
	// immediately followed by another VersionEdit (being written in logAndApply()). That
	// VersionEdit always contains a LastSeqNum, so we don't need to include that in the snapshot.
	// But it does not necessarily include MinUnflushedLogNum, NextFileNum, so we initialize those
	// using the corresponding fields in the versionSet (which came from the latest preceding
	// VersionEdit that had those fields).
	snapshot.MinUnflushedLogNum = minUnflushedLogNum
	snapshot.NextFileNum = nextFileNum

	w, err1 := manifest.Next()
	if err1 != nil {
		return err1
	}
	if err := snapshot.Encode(w); err != nil {
		return err
	}

	if vs.manifest != nil {
		vs.manifest.Close()
		vs.manifest = nil
	}
	if vs.manifestFile != nil {
		if err := vs.manifestFile.Close(); err != nil {
			return err
		}
		vs.manifestFile = nil
	}

	vs.manifest, manifest = manifest, nil
	vs.manifestFile, manifestFile = manifestFile, nil
	return nil
}

func (vs *versionSet) markFileNumUsed(fileNum FileNum) {
	if vs.nextFileNum <= fileNum {
		vs.nextFileNum = fileNum + 1
	}
}

func (vs *versionSet) getNextFileNum() FileNum {
	x := vs.nextFileNum
	vs.nextFileNum++
	return x
}

func (vs *versionSet) append(v *version) {
	if v.Refs() != 0 {
		panic("pebble: version should be unreferenced")
	}
	if !vs.versions.Empty() {
		vs.versions.Back().UnrefLocked()
	}
	v.Deleted = vs.obsoleteFn
	v.Ref()
	vs.versions.PushBack(v)
}

func (vs *versionSet) currentVersion() *version {
	return vs.versions.Back()
}

func (vs *versionSet) addLiveFileNums(m map[FileNum]struct{}) {
	current := vs.currentVersion()
	for v := vs.versions.Front(); true; v = v.Next() {
		for _, lm := range v.Levels {
			iter := lm.Iter()
			for f := iter.First(); f != nil; f = iter.Next() {
				m[f.FileNum] = struct{}{}
			}
		}
		if v == current {
			break
		}
	}
}

func (vs *versionSet) addObsoleteLocked(obsolete []*manifest.FileMetadata) {
	for _, fileMeta := range obsolete {
		// Note that the obsolete tables are no longer zombie by the definition of
		// zombie, but we leave them in the zombie tables map until they are
		// deleted from disk.
		if _, ok := vs.zombieTables[fileMeta.FileNum]; !ok {
			vs.opts.Logger.Fatalf("MANIFEST obsolete table %s not marked as zombie", fileMeta.FileNum)
		}
	}
	vs.obsoleteTables = append(vs.obsoleteTables, obsolete...)
	vs.incrementObsoleteTablesLocked(obsolete)
}

func (vs *versionSet) incrementObsoleteTablesLocked(obsolete []*manifest.FileMetadata) {
	for _, fileMeta := range obsolete {
		vs.metrics.Table.ObsoleteCount++
		vs.metrics.Table.ObsoleteSize += fileMeta.Size
	}
}

func setCurrentFunc(
	vers FormatMajorVersion, marker *atomicfs.Marker, fs vfs.FS, dirname string, dir vfs.File,
) func(FileNum) error {
	if vers < formatVersionedManifestMarker {
		// Pebble versions before `formatVersionedManifestMarker` used
		// the CURRENT file to signal which MANIFEST is current. Ignore
		// the filename read during LocateMarker.
		return func(manifestFileNum FileNum) error {
			if err := setCurrentFile(dirname, fs, manifestFileNum); err != nil {
				return err
			}
			if err := dir.Sync(); err != nil {
				// This is a  panic here, rather than higher in the call
				// stack, for parity with the atomicfs.Marker behavior.
				// A panic is always necessary because failed Syncs are
				// unrecoverable.
				panic(errors.Wrap(err, "fatal: MANIFEST dirsync failed"))
			}
			return nil
		}
	}
	return setCurrentFuncMarker(marker, fs, dirname)
}

func setCurrentFuncMarker(marker *atomicfs.Marker, fs vfs.FS, dirname string) func(FileNum) error {
	return func(manifestFileNum FileNum) error {
		return marker.Move(base.MakeFilename(fileTypeManifest, manifestFileNum))
	}
}

func findCurrentManifest(
	vers FormatMajorVersion, fs vfs.FS, dirname string,
) (marker *atomicfs.Marker, manifestNum FileNum, exists bool, err error) {
	// NB: We always locate the manifest marker, even if we might not
	// actually use it (because we're opening the database at an earlier
	// format major version that uses the CURRENT file).  Locating a
	// marker should succeed even if the marker has never been placed.
	var filename string
	marker, filename, err = atomicfs.LocateMarker(fs, dirname, manifestMarkerName)
	if err != nil {
		return nil, 0, false, err
	}

	if vers < formatVersionedManifestMarker {
		// Pebble versions before `formatVersionedManifestMarker` used
		// the CURRENT file to signal which MANIFEST is current. Ignore
		// the filename read during LocateMarker.

		manifestNum, err = readCurrentFile(fs, dirname)
		if oserror.IsNotExist(err) {
			return marker, 0, false, nil
		} else if err != nil {
			return marker, 0, false, err
		}
		return marker, manifestNum, true, nil
	}

	// The current format major version is >=
	// formatVersionedManifestMarker indicating that the
	// atomicfs.Marker is the source of truth on the current manifest.

	if filename == "" {
		// The marker hasn't been set yet. This database doesn't exist.
		return marker, 0, false, nil
	}

	var ok bool
	_, manifestNum, ok = base.ParseFilename(fs, filename)
	if !ok {
		return marker, 0, false, base.CorruptionErrorf("pebble: MANIFEST name %q is malformed", errors.Safe(filename))
	}
	return marker, manifestNum, true, nil
}

func readCurrentFile(fs vfs.FS, dirname string) (FileNum, error) {
	// Read the CURRENT file to find the current manifest file.
	current, err := fs.Open(base.MakeFilepath(fs, dirname, fileTypeCurrent, 0))
	if err != nil {
		return 0, errors.Wrapf(err, "pebble: could not open CURRENT file for DB %q", dirname)
	}
	defer current.Close()
	stat, err := current.Stat()
	if err != nil {
		return 0, err
	}
	n := stat.Size()
	if n == 0 {
		return 0, errors.Errorf("pebble: CURRENT file for DB %q is empty", dirname)
	}
	if n > 4096 {
		return 0, errors.Errorf("pebble: CURRENT file for DB %q is too large", dirname)
	}
	b := make([]byte, n)
	_, err = current.ReadAt(b, 0)
	if err != nil {
		return 0, err
	}
	if b[n-1] != '\n' {
		return 0, base.CorruptionErrorf("pebble: CURRENT file for DB %q is malformed", dirname)
	}
	b = bytes.TrimSpace(b)

	_, manifestFileNum, ok := base.ParseFilename(fs, string(b))
	if !ok {
		return 0, base.CorruptionErrorf("pebble: MANIFEST name %q is malformed", errors.Safe(b))
	}
	return manifestFileNum, nil
}

func newFileMetrics(newFiles []manifest.NewFileEntry) map[int]*LevelMetrics {
	m := map[int]*LevelMetrics{}
	for _, nf := range newFiles {
		lm := m[nf.Level]
		if lm == nil {
			lm = &LevelMetrics{}
			m[nf.Level] = lm
		}
		lm.NumFiles++
		lm.Size += int64(nf.Meta.Size)
	}
	return m
}
