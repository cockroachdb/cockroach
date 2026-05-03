// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// TestingRSEngine implements innerRSEngine for testing without a real Pebble
// engine. It writes human-readable manifest files to the filesystem. SSTables
// are tracked by name only (contents are opaque).
type TestingRSEngine struct {
	opts        InnerRSEngineOptions
	manifestNum DiskFileNum
	sstables    []string // sorted list of sstable basenames
	// mu protects internal state. mu is never held when calling
	// ManifestChangeCommitter.
	mu struct {
		syncutil.Mutex
		// refs tracks callers that have called Ref but not yet Unref. closeInner
		// waits for this to reach 0.
		refs int
		// Snapshot tracking.
		nextSnapshotID uint64
		// closeInner waits for this to be empty.
		openSnapshots map[uint64]*snapshotInfo
		// cond is signalled when refs is decremented or a snapshot is closed.
		cond *sync.Cond
		// ongoingManifestChange is used for serializing manifest changes. An
		// ongoing manifest change holds either a ref or a snapshot.
		ongoingManifestChange bool
		// Transitions to true in quiece or innerClose, whichever happens first.
		closing bool
		// Transitions to true in innerClose.
		closed bool
		// opLog logs a subset of method calls for testing verification.
		opLog strings.Builder
	}
	// closeWaitingCh is an optional test hook. If non-nil, innerClose() closes
	// this channel before it starts waiting, allowing tests to deterministically
	// verify blocking behavior.
	closeWaitingCh chan struct{}
}

// snapshotInfo tracks an open snapshot for debugging.
type snapshotInfo struct {
	id         uint64
	stack      string
	createTime time.Time
}

var _ OpenInnerRSEngineFunc = OpenTestingRSEngine

// OpenTestingRSEngine creates a new TestingRSEngine. If manifestNum !=
// NoManifestNum, loads existing manifest from BasaltDir.
func OpenTestingRSEngine(
	manifestNum DiskFileNum, opts InnerRSEngineOptions,
) (innerRSEngine, error) {
	if opts.manifestChangeCommitter == nil {
		return nil, errors.New("ManifestChangeCommitter is required")
	}
	if opts.basaltFS == nil {
		return nil, errors.New("BasaltFS is required")
	}
	if opts.basaltDir == "" {
		return nil, errors.New("BasaltDir is required")
	}
	if opts.basaltScratchPathDir == "" {
		return nil, errors.New("BasaltScratchPathDir is required")
	}
	// Create directories if they don't exist.
	if err := opts.basaltFS.MkdirAll(opts.basaltDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating BasaltDir")
	}
	if err := opts.basaltFS.MkdirAll(opts.basaltScratchPathDir, 0755); err != nil {
		return nil, errors.Wrap(err, "creating BasaltScratchPathDir")
	}
	var sstables []string
	// Load existing manifest if specified.
	if manifestNum != NoManifestNum {
		var err error
		sstables, err = readManifestFile(opts.basaltFS, opts.basaltDir, manifestNum)
		if err != nil {
			return nil, err
		}
	}
	// Else, fresh engine with no manifest yet.

	e := &TestingRSEngine{
		opts:        opts,
		manifestNum: manifestNum,
		sstables:    sstables,
	}
	e.mu.openSnapshots = make(map[uint64]*snapshotInfo)
	e.mu.cond = sync.NewCond(&e.mu.Mutex)
	return e, nil
}

// formatManifestName returns the manifest filename for a DiskFileNum.
// Format: MANIFEST-NNNNNN (6-digit zero-padded).
func formatManifestName(num DiskFileNum) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

// formatSSTName returns the SST filename for a DiskFileNum.
// Format: NNNNNN.sst (6-digit zero-padded).
func formatSSTName(num DiskFileNum) string {
	return fmt.Sprintf("%06d.sst", num)
}

// readManifestFile reads a human-readable manifest file and returns the list of
// sstable basenames. Validates that the manifest number matches the expected
// value.
//
// Manifest format:
//
//	manifest:<num>
//	000040.sst
//	000041.sst
func readManifestFile(fs vfs.FS, basaltDir string, manifestNum DiskFileNum) ([]string, error) {
	manifestName := formatManifestName(manifestNum)
	path := fs.PathJoin(basaltDir, manifestName)
	f, err := fs.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "opening manifest %s", path)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, errors.Wrapf(err, "reading manifest %s", path)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return nil, errors.Newf("empty manifest file %s", path)
	}
	// First line should be "manifest:<num>".
	if !strings.HasPrefix(lines[0], "manifest:") {
		return nil, errors.Newf("invalid manifest header in %s: %s", path, lines[0])
	}
	var parsedNum DiskFileNum
	if _, err := fmt.Sscanf(lines[0], "manifest:%d", &parsedNum); err != nil {
		return nil, errors.Wrapf(err, "parsing manifest number from %s", lines[0])
	}
	if parsedNum != manifestNum {
		return nil, errors.Newf("manifest number mismatch in %s: expected %d, got %d",
			path, manifestNum, parsedNum)
	}
	// Remaining lines are sstable names.
	sstables := make([]string, 0, len(lines)-1)
	for _, line := range lines[1:] {
		line = strings.TrimSpace(line)
		if line != "" {
			sstables = append(sstables, line)
		}
	}
	if !slices.IsSorted(sstables) {
		return nil, errors.Newf("sstables not sorted in %s: %v", path, sstables)
	}
	return sstables, nil
}

// writeManifestFile writes a human-readable manifest file.
func writeManifestFile(
	fs vfs.FS, basaltDir string, manifestNum DiskFileNum, sstables []string,
) error {
	var buf strings.Builder
	fmt.Fprintf(&buf, "manifest:%d\n", manifestNum)
	for _, sst := range sstables {
		fmt.Fprintf(&buf, "%s\n", sst)
	}
	manifestName := formatManifestName(manifestNum)
	path := fs.PathJoin(basaltDir, manifestName)
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return errors.Wrapf(err, "creating manifest %s", path)
	}
	if _, err := f.Write([]byte(buf.String())); err != nil {
		_ = f.Close()
		return errors.Wrapf(err, "writing manifest %s", path)
	}
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "closing manifest %s", path)
	}
	return nil
}

// compactionToggle implements innerRSEngine. It is a no-op for the testing
// engine.
func (e *TestingRSEngine) compactionToggle(enable bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(&e.mu.opLog, "CompactionToggle(%v)\n", enable)
}

// enableUnreferencedFileDeletion implements innerRSEngine. It is a no-op for
// the testing engine.
func (e *TestingRSEngine) enableUnreferencedFileDeletion() {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(&e.mu.opLog, "EnableUnreferencedFileDeletion\n")
}

// waitForOngoingManifestChanges implements innerRSEngine.
func (e *TestingRSEngine) waitForOngoingManifestChanges() {
	e.mu.Lock()
	defer e.mu.Unlock()
	fmt.Fprintf(&e.mu.opLog, "started WaitForOngoingManifestChanges()\n")
	for e.mu.ongoingManifestChange && !e.mu.closing {
		e.mu.cond.Wait()
	}
	fmt.Fprintf(&e.mu.opLog, "finished WaitForOngoingManifestChanges()\n")
}

// currentManifestNum implements innerRSEngine.
func (e *TestingRSEngine) currentManifestNum() DiskFileNum {
	return e.manifestNum
}

func (e *TestingRSEngine) getAndClearOpLog() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.mu.opLog.String()
	e.mu.opLog.Reset()
	return s
}

// beginOp waits until no manifest change operation is ongoing, then marks an
// operation as started. Returns error if the engine is closing.
func (e *TestingRSEngine) beginOp() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for e.mu.ongoingManifestChange && !e.mu.closing {
		e.mu.cond.Wait()
	}
	if e.mu.closing {
		return ErrInnerRSEngineClosed
	}
	e.mu.ongoingManifestChange = true
	return nil
}

// endOp marks the current manifest change operation as complete.
func (e *TestingRSEngine) endOp() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.ongoingManifestChange = false
	e.mu.cond.Broadcast()
}

const testingMinGetFileNumCount = 60

// flushSSTables implements innerRSEngine.
func (e *TestingRSEngine) flushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error {
	if len(scratchNames) != 1 {
		return errors.AssertionFailedf(
			"TestingRSEngine.FlushSSTables expects exactly 1 file, got %d", len(scratchNames))
	}
	if err := e.beginOp(); err != nil {
		return err
	}
	defer e.endOp()
	// Verify scratch file exists.
	scratchPath := e.opts.basaltFS.PathJoin(e.opts.basaltScratchPathDir, scratchNames[0])
	if _, err := e.opts.basaltFS.Stat(scratchPath); err != nil {
		return errors.Wrapf(err, "scratch file %s not found", scratchPath)
	}
	// Get file numbers: one for the new manifest, one for the new sstable. Get extra,
	// to exercise raft and any stashing of nums.
	count := max(2, testingMinGetFileNumCount)
	fileNums, err := e.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return err
	}
	// Use the highest for manifest, next highest for sstable.
	newManifestNum := fileNums[len(fileNums)-1]
	newSSTNum := fileNums[len(fileNums)-2]
	newSSTName := formatSSTName(newSSTNum)
	// Hardlink SST from scratch to BasaltDir.
	dstSSTPath := e.opts.basaltFS.PathJoin(e.opts.basaltDir, newSSTName)
	if err := e.opts.basaltFS.Link(scratchPath, dstSSTPath); err != nil {
		return errors.Wrapf(err, "linking SST %s to %s", scratchPath, dstSSTPath)
	}
	// Build new sstable list: current + new.
	sstables := append([]string(nil), e.sstables...)
	sstables = append(sstables, newSSTName)
	if !slices.IsSorted(sstables) {
		return errors.AssertionFailedf("sstables not sorted after append: %v", sstables)
	}
	// Write manifest file.
	if err := writeManifestFile(e.opts.basaltFS, e.opts.basaltDir, newManifestNum, sstables); err != nil {
		return err
	}
	// Install new manifest via Raft. Do NOT update internal state — the engine
	// will be closed and reopened with the new manifest number after this
	// returns.
	manifestInfo := ManifestInfo{
		Manifest: FileNameAndNum{Name: formatManifestName(newManifestNum), Num: newManifestNum},
		Files:    []FileNameAndNum{{Name: newSSTName, Num: newSSTNum}},
	}
	if err := e.opts.manifestChangeCommitter.InstallNewManifest(
		e.manifestNum, manifestInfo, flushCommit); err != nil {
		return err
	}
	// Match Pebble's contract: on a successful ingest, the scratch source file
	// is owned by the engine and removed.
	if err := e.opts.basaltFS.Remove(scratchPath); err != nil {
		return errors.Wrapf(err, "removing scratch file %s after flush", scratchPath)
	}
	return nil
}

// addSSTables implements innerRSEngine. It is not implemented for testing
// engine.
func (e *TestingRSEngine) addSSTables(scratchNames []string) error {
	return errors.Errorf("AddSSTables not implemented")
}

// ref implements innerRSEngine.
func (e *TestingRSEngine) ref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.refs++
}

// unref implements innerRSEngine.
func (e *TestingRSEngine) unref() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.refs--
	if e.mu.refs < 0 {
		panic("externalRefs went negative")
	}
	e.mu.cond.Broadcast()
}

// newSnapshot implements innerRSEngine.
func (e *TestingRSEngine) newSnapshot() RSEngineSnapshot {
	// Capture stack trace for debugging snapshot leaks.
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	stack := string(buf[:n])
	e.mu.Lock()
	defer e.mu.Unlock()
	// Track snapshot for debugging.
	e.mu.nextSnapshotID++
	id := e.mu.nextSnapshotID
	e.mu.openSnapshots[id] = &snapshotInfo{
		id:         id,
		stack:      stack,
		createTime: timeutil.Now(),
	}
	return &TestingRSEngineSnapshot{
		engine:     e,
		snapshotID: id,
	}
}

// prepareExternalManifest implements innerRSEngine.
func (e *TestingRSEngine) prepareExternalManifest(manifestNum DiskFileNum) error {
	return errors.Errorf("TestingRSEngine does not support in-place manifest install")
}

// installPreparedManifest implements innerRSEngine.
func (e *TestingRSEngine) installPreparedManifest(manifestNum DiskFileNum) {
	panic("TestingRSEngine does not support in-place manifest install")
}

// quiesce implements innerRSEngine.
func (e *TestingRSEngine) quiesce() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.closing = true
	e.mu.cond.Broadcast()
}

// closeInner implements innerRSEngine.
func (e *TestingRSEngine) closeInner() {
	// Mark as closed and wait for ongoing operation to complete.
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.mu.closing = true
		e.mu.closed = true
	}()
	// Signal test hook before waiting.
	if e.closeWaitingCh != nil {
		close(e.closeWaitingCh)
	}
	startTime := timeutil.Now()
	for !e.closeWaitDone(&startTime) {
		time.Sleep(100 * time.Millisecond)
	}
}

// closeWaitDone checks whether all refs have been released and all snapshots
// closed. If blocked for too long, it logs diagnostic info about open
// snapshots. Returns true when cleanup is complete.
func (e *TestingRSEngine) closeWaitDone(startTime *time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.mu.openSnapshots) == 0 && e.mu.refs == 0 {
		return true
	}
	if elapsed := timeutil.Since(*startTime); elapsed > 5*time.Second {
		var buf strings.Builder
		fmt.Fprintf(&buf, "TestingRSEngine.closeInner() blocked for %v waiting for "+
			"%d open snapshot(s), %d ref(s):\n",
			elapsed, len(e.mu.openSnapshots), e.mu.refs)
		for id, info := range e.mu.openSnapshots {
			fmt.Fprintf(&buf, "  snapshot %d (created %v ago):\n%s\n",
				id, timeutil.Since(info.createTime), info.stack)
		}
		fmt.Print(buf.String())
		*startTime = timeutil.Now()
	}
	return false
}

// TestFlushSSTables is a test helper that creates a scratch file and calls
// FlushSSTables.
func (e *TestingRSEngine) TestFlushSSTables(scratchFileName string) error {
	// Write simple content to scratch file.
	content := fmt.Sprintf("previous-manifest:%d", e.manifestNum)
	scratchPath := e.opts.basaltFS.PathJoin(e.opts.basaltScratchPathDir, scratchFileName)
	f, err := e.opts.basaltFS.Create(scratchPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(content)); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return e.flushSSTables([]string{scratchFileName}, nil)
}

// TestingRSEngineSnapshot implements RSEngineSnapshot for testing.
type TestingRSEngineSnapshot struct {
	engine     *TestingRSEngine
	snapshotID uint64
	mu         struct {
		syncutil.Mutex
		closed bool
	}
}

var _ RSEngineSnapshot = (*TestingRSEngineSnapshot)(nil)

// ManifestInfo implements RSEngineSnapshot.
func (s *TestingRSEngineSnapshot) ManifestInfo() ManifestInfo {
	if s.mu.closed {
		panic("snapshot is closed")
	}
	files := make([]FileNameAndNum, len(s.engine.sstables))
	for i, name := range s.engine.sstables {
		// Parse DiskFileNum from name (e.g., "000042.sst" -> 42).
		var num DiskFileNum
		_, err := fmt.Sscanf(name, "%d.sst", &num)
		if err != nil {
			panic(err)
		}
		files[i] = FileNameAndNum{Name: name, Num: num}
	}
	return ManifestInfo{
		Manifest: FileNameAndNum{
			Name: formatManifestName(s.engine.manifestNum),
			Num:  s.engine.manifestNum,
		},
		Files: files,
	}
}

// ManifestNum implements RSEngineSnapshot.
func (s *TestingRSEngineSnapshot) ManifestNum() DiskFileNum {
	return s.engine.manifestNum
}

// Clone implements RSEngineSnapshot.
func (s *TestingRSEngineSnapshot) Clone() RSEngineSnapshot {
	return s.engine.newSnapshot()
}

// Split implements RSEngineSnapshot.
func (s *TestingRSEngineSnapshot) Split(
	ctx context.Context, splitKey roachpb.Key, rhsDir string,
) (lhsManifest FileNameAndNum, rhs ManifestInfo, err error) {
	if s.mu.closed {
		panic("snapshot is closed")
	}
	if err := s.engine.beginOp(); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, err
	}
	defer s.engine.endOp()
	// Get file numbers for new manifests.
	count := max(testingMinGetFileNumCount, 1+len(s.engine.sstables))
	fileNums, err := s.engine.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return FileNameAndNum{}, ManifestInfo{}, err
	}
	// Use same manifest number for both LHS and RHS (there is no collision since
	// they are in separate dirs).
	newManifestNum := fileNums[len(fileNums)-1]
	manifestName := formatManifestName(newManifestNum)
	// Create LHS manifest in BasaltDir.
	if err := writeManifestFile(s.engine.opts.basaltFS, s.engine.opts.basaltDir,
		newManifestNum, s.engine.sstables); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, err
	}
	// Create RHS directory and manifest.
	if err := s.engine.opts.basaltFS.MkdirAll(rhsDir, 0755); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, errors.Wrap(err, "creating RHS directory")
	}
	if err := writeManifestFile(s.engine.opts.basaltFS, rhsDir, newManifestNum, s.engine.sstables); err != nil {
		return FileNameAndNum{}, ManifestInfo{}, err
	}
	// Hardlink sstables from LHS to RHS.
	rhsFiles := make([]FileNameAndNum, len(s.engine.sstables))
	for i, sstName := range s.engine.sstables {
		srcPath := s.engine.opts.basaltFS.PathJoin(s.engine.opts.basaltDir, sstName)
		dstPath := s.engine.opts.basaltFS.PathJoin(rhsDir, sstName)
		if err := s.engine.opts.basaltFS.Link(srcPath, dstPath); err != nil {
			return FileNameAndNum{}, ManifestInfo{}, errors.Wrapf(err, "linking SST %s to %s", srcPath, dstPath)
		}
		var num DiskFileNum
		_, err = fmt.Sscanf(sstName, "%d.sst", &num)
		if err != nil {
			panic(err)
		}
		rhsFiles[i] = FileNameAndNum{Name: sstName, Num: num}
	}
	lhsManifest = FileNameAndNum{Name: manifestName, Num: newManifestNum}
	rhs = ManifestInfo{
		Manifest: FileNameAndNum{Name: manifestName, Num: newManifestNum},
		Files:    rhsFiles,
	}
	return lhsManifest, rhs, nil
}

// Merge implements RSEngineSnapshot.
func (s *TestingRSEngineSnapshot) Merge(
	ctx context.Context, rhs RSEngineSnapshot,
) (merged ManifestInfo, err error) {
	rhsSnap, ok := rhs.(*TestingRSEngineSnapshot)
	if !ok {
		return ManifestInfo{}, errors.AssertionFailedf("expected *TestingRSEngineSnapshot, got %T", rhs)
	}
	// Acquire operation lock on both engines.
	if err := s.engine.beginOp(); err != nil {
		return ManifestInfo{}, err
	}
	defer s.engine.endOp()
	if err := rhsSnap.engine.beginOp(); err != nil {
		return ManifestInfo{}, err
	}
	defer rhsSnap.engine.endOp()
	rhsInfo := rhs.ManifestInfo()
	// Get file numbers: one for manifest, one for each RHS sstable.
	count := max(testingMinGetFileNumCount, 1+len(rhsInfo.Files))
	fileNums, err := s.engine.opts.manifestChangeCommitter.GetFileNums(count)
	if err != nil {
		return ManifestInfo{}, err
	}
	newManifestNum := fileNums[len(fileNums)-1]
	// Renumber RHS sstables and hardlink to LHS directory.
	newFiles := make([]FileNameAndNum, len(rhsInfo.Files))
	allSSTables := append([]string{}, s.engine.sstables...)
	for i, rhsFile := range rhsInfo.Files {
		newNum := fileNums[i]
		newName := formatSSTName(newNum)
		// Hardlink from RHS directory to LHS directory.
		srcPath := rhsSnap.engine.opts.basaltFS.PathJoin(rhsSnap.engine.opts.basaltDir, rhsFile.Name)
		dstPath := s.engine.opts.basaltFS.PathJoin(s.engine.opts.basaltDir, newName)
		if err := s.engine.opts.basaltFS.Link(srcPath, dstPath); err != nil {
			return ManifestInfo{}, errors.Wrapf(err, "linking SST %s to %s", srcPath, dstPath)
		}
		newFiles[i] = FileNameAndNum{Name: newName, Num: newNum}
		allSSTables = append(allSSTables, newName)
	}
	if !slices.IsSorted(allSSTables) {
		return ManifestInfo{}, errors.AssertionFailedf("sstables not sorted after merge: %v", allSSTables)
	}
	// Write merged manifest.
	if err := writeManifestFile(s.engine.opts.basaltFS, s.engine.opts.basaltDir,
		newManifestNum, allSSTables); err != nil {
		return ManifestInfo{}, err
	}
	return ManifestInfo{
		Manifest: FileNameAndNum{Name: formatManifestName(newManifestNum), Num: newManifestNum},
		Files:    newFiles,
	}, nil
}

// Close releases the manifest ref held by this snapshot.
func (s *TestingRSEngineSnapshot) Close() {
	var alreadyClosed bool
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.mu.closed {
			alreadyClosed = true
			return
		}
		s.mu.closed = true
	}()
	if alreadyClosed {
		return
	}
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()
	// Remove from snapshot tracking.
	delete(s.engine.mu.openSnapshots, s.snapshotID)
	s.engine.mu.cond.Broadcast()
}

// printFilesystem pretty-prints directory contents recursively. For
// directories, it prints a trailing slash and recurses. For files, it prints
// just the filename (no contents, to keep output concise).
//
// If rangeIDMap is non-nil, directory names matching the pattern r<num>:<num>
// will have their range ID (first number) replaced with the mapped synthetic
// value. This makes test output deterministic when range IDs vary between runs.
func printFilesystem(fs vfs.FS, dir string, rangeIDMap map[roachpb.RangeID]int) string {
	var buf strings.Builder
	printFilesystemRecursive(fs, dir, "", &buf, rangeIDMap)
	return buf.String()
}

func printFilesystemRecursive(
	fs vfs.FS, dir, prefix string, buf *strings.Builder, rangeIDMap map[roachpb.RangeID]int,
) {
	entries, err := fs.List(dir)
	if err != nil {
		fmt.Fprintf(buf, "%serror: %v\n", prefix, err)
		return
	}
	slices.Sort(entries)
	for _, entry := range entries {
		path := fs.PathJoin(dir, entry)
		stat, err := fs.Stat(path)
		if err != nil {
			fmt.Fprintf(buf, "%s%s: error\n", prefix, entry)
			continue
		}
		if stat.IsDir() {
			// Check if this is a range directory pattern r<rangeID>:<replicaID>.
			if rangeID, ok := parseRangeDir(entry); ok {
				// If map is non-nil and rangeID not in map, skip entirely.
				if rangeIDMap != nil {
					if _, inMap := rangeIDMap[rangeID]; !inMap {
						continue
					}
				}
			}
			displayEntry := remapRangeID(entry, rangeIDMap)
			fmt.Fprintf(buf, "%s%s/\n", prefix, displayEntry)
			printFilesystemRecursive(fs, path, prefix+displayEntry+"/", buf, rangeIDMap)
		} else {
			displayEntry := remapRangeID(entry, rangeIDMap)
			fmt.Fprintf(buf, "%s%s:\n", prefix, displayEntry)
			// Print file contents with indentation.
			f, err := fs.Open(path)
			if err != nil {
				fmt.Fprintf(buf, "%s  error: %v\n", prefix, err)
				continue
			}
			data, err := io.ReadAll(f)
			err = errors.CombineErrors(err, f.Close())
			if err != nil {
				fmt.Fprintf(buf, "%s  error: %v\n", prefix, err)
				continue
			}
			for _, line := range strings.Split(string(data), "\n") {
				if line != "" {
					fmt.Fprintf(buf, "%s  %s\n", prefix, line)
				}
			}
		}
	}
}

// parseRangeDir parses a directory name matching pattern
// r<rangeID>:<replicaID>. Returns the rangeID and true if the pattern matches,
// otherwise returns 0, false.
func parseRangeDir(name string) (roachpb.RangeID, bool) {
	var rangeID, replicaID int
	if n, _ := fmt.Sscanf(name, "r%d:%d", &rangeID, &replicaID); n == 2 {
		return roachpb.RangeID(rangeID), true
	}
	return 0, false
}

// remapRangeID replaces range IDs in directory names like "r82:1" with their
// synthetic mapped values. If the map is nil or the range ID is not found, the
// original string is returned unchanged.
func remapRangeID(name string, rangeIDMap map[roachpb.RangeID]int) string {
	if rangeIDMap == nil {
		return name
	}
	// Parse pattern: r<rangeID>:<replicaID>
	var rangeID, replicaID int
	if n, _ := fmt.Sscanf(name, "r%d:%d", &rangeID, &replicaID); n == 2 {
		if synthetic, ok := rangeIDMap[roachpb.RangeID(rangeID)]; ok {
			return fmt.Sprintf("r%d:%d", synthetic, replicaID)
		}
	}
	return name
}

// PrintTestingRSEngineState prints the internal manifest state of a
// TestingRSEngine. It shows the current manifest number and all tracked
// manifests with their reference counts and SSTable lists.
func PrintTestingRSEngineState(engine *TestingRSEngine) string {
	var buf strings.Builder
	engine.mu.Lock()
	defer engine.mu.Unlock()
	fmt.Fprintf(&buf, "current-manifest: %d\n", engine.manifestNum)
	fmt.Fprintf(&buf, "manifest %d: refs=%d snaps=%d sstables=%v\n",
		engine.manifestNum, engine.mu.refs, len(engine.mu.openSnapshots), engine.sstables)
	return buf.String()
}
