// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package corpus

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
)

// Collector is used to collect declarative schema changer into a corpus file. This
// will structure will help gather schema changer states in memory for tests,
// by providing a BeforeStage callback (see scexec.TestingKnobs) that can be
// injected into a cluster. Additionally, the callback will take into account
// the root test name and subtest name when labelling detected statements, which
// allows us to separate configuration for logic test (for example:
// TestLogic/local/system-3-node-tenant_TestLogic/local/system-3-node-tenant_DROP DATABASE test CASCADE_0).
// Once  all the declarative states from a given set of tests are gathered, the
// UpdateCorpus method will then update the on disk corpus file to merge in the
// new entries replacing existing ones. This structure is designed for concurrent
// use both in memory and on disk, meaning that it can parallel threads adding new
// entries. The  UpdateCorpus method uses a lock file to ensure that only a single
// process at a time is merging entries.
type Collector struct {
	Reader
	mu             syncutil.Mutex
	corpusEntries  []*scpb.CorpusState
	corpusPrefixes map[string]struct{}
}

// Reader used to read the declarative schema changer state from a corpus.
type Reader struct {
	corpusFilePath string
	corpusLockPath string
	diskState      scpb.CorpusDisk
}

// getCorpusPath based on the type of corpus determines the storage path.
func getCorpusPath(basePath string) string {
	return filepath.Join(basePath, "corpus")
}

// getCorpusLockFile gets the name of the lock file for the given corpus.
func getCorpusLockFile(corpusFile string) string {
	return corpusFile + ".lock"
}

// NewCorpusReader creates a reader object for a declarative schema changer
// corpus, which can be used to read back states stored on disk.
func NewCorpusReader(basePath string) (*Reader, error) {
	corpusFilePath := getCorpusPath(basePath)
	return NewCorpusReaderWithPath(corpusFilePath)
}

// NewCorpusReaderWithPath creates a reader object for a declarative schema changer
// corpus, which can be used to read back states stored on disk.
func NewCorpusReaderWithPath(corpusFilePath string) (*Reader, error) {
	corpusLockPath := getCorpusLockFile(corpusFilePath)
	return &Reader{
		corpusFilePath: corpusFilePath,
		corpusLockPath: corpusLockPath,
	}, nil
}

// NewCorpusCollector creates a collector object for a declarative schema changer
// corpus, which can be used to collect states during execution.s
func NewCorpusCollector(basePath string) (*Collector, error) {
	reader, err := NewCorpusReader(basePath)
	if err != nil {
		return nil, err
	}
	return &Collector{
		Reader:         *reader,
		corpusPrefixes: make(map[string]struct{}),
	}, nil
}

// statementsToName creates an identifier for a given set of statements.
func statementsToName(statements []scpb.Statement) string {
	sb := strings.Builder{}
	for _, stmt := range statements {
		sb.Write([]byte(stmt.Statement))
	}
	return sb.String()
}

// makeCorpusName creates a name entry for a given set of statements.
func makeCorpusName(
	parentTestName string, testName string, statements []scpb.Statement, stageIdx int,
) (corpusName string, corpusPrefix string) {
	return fmt.Sprintf("%s_%s_%s_%d",
			parentTestName,
			testName,
			statementsToName(statements),
			stageIdx),
		fmt.Sprintf("%s_%s",
			parentTestName, testName)
}

// getCorpusTestPrefix extracts the test name / prefix for a given corpus.
func getCorpusTestPrefix(corpusName string) string {
	prefixName := strings.SplitN(corpusName, "_", 3)
	return prefixName[0] + "_" + prefixName[1]
}

// GetBeforeStage gets a BeforeStage callback for a scrun.TestingKnob, which will
// gather any declarative schema changer stage. This function will add labels on
// top of any corpus to indicate which test/config and other detail the target
// statement was collected from to more uniquely identify it.
func (cc *Collector) GetBeforeStage(
	parentName string, t *testing.T,
) func(p scplan.Plan, stageIdx int) error {
	return func(p scplan.Plan, stageIdx int) error {
		s := p.Stages[stageIdx]
		if s.Phase == scop.PostCommitPhase ||
			s.Phase == scop.PostCommitNonRevertiblePhase {
			corpusName, corpusPrefix := makeCorpusName(parentName, t.Name(), p.Statements, stageIdx)
			entry := &scpb.CorpusState{
				Name:        corpusName,
				Status:      p.Initial,
				TargetState: &p.TargetState,
				InRollback:  p.InRollback,
				Revertible:  p.Revertible,
			}
			cc.mu.Lock()
			cc.corpusEntries = append(cc.corpusEntries, entry)
			cc.corpusPrefixes[corpusPrefix] = struct{}{}
			cc.mu.Unlock()
		}
		return nil
	}
}

// UpdateCorpus updates the corpus file on disk, which will lock the file
// and merge any new entries with the existing ones on disk.
func (cc *Collector) UpdateCorpus() error {
	corpusFile, unlockFn, err := cc.openCorpus(true)
	if err != nil {
		return err
	}
	defer unlockFn()
	defer corpusFile.Close()
	err = cc.readCorpusWithFile(corpusFile)
	if err != nil {
		return err
	}
	_, err = corpusFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	// Combine all the entries.
	entryMap := make(map[string]*scpb.CorpusState)
	for _, entry := range cc.diskState.CorpusArray {
		// If a prefix has been replaced, then skip it
		if _, ok := cc.corpusPrefixes[getCorpusTestPrefix(entry.Name)]; ok {
			continue
		}
		entryMap[entry.Name] = entry
	}
	for _, entry := range cc.corpusEntries {
		entryMap[entry.Name] = entry
	}
	mergedEntries := scpb.CorpusDisk{
		CorpusArray: make([]*scpb.CorpusState, 0, len(entryMap)),
	}
	for _, entry := range entryMap {
		mergedEntries.CorpusArray = append(mergedEntries.CorpusArray, entry)
	}
	// Sort them out.
	sort.SliceStable(mergedEntries.CorpusArray, func(i, j int) bool {
		return mergedEntries.CorpusArray[i].Name < mergedEntries.CorpusArray[j].Name
	})
	// Write the merged entries back
	err = corpusFile.Truncate(0)
	if err != nil {
		return err
	}
	bytes, err := protoutil.Marshal(&mergedEntries)
	if err != nil {
		return err
	}
	_, err = corpusFile.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// lockCorpus locks the corpus file on disk for reading/writing.
func (cr *Reader) lockCorpus() (unlockFn func(), err error) {
	var f *os.File
	r := retry.Start(retry.Options{})
	// File creation/deletion is atomic so use it to lock the corpus on disk.
	// Note: On Linux/Unix we can do better and use unix.Flock, but that won't
	// be platform independent.
	for r.Next() {
		f, err = os.OpenFile(cr.corpusLockPath, os.O_EXCL|os.O_CREATE, 0666)
		if oserror.IsExist(err) {
			continue
		}
		if err != nil {
			return nil, errors.Wrapf(err, "creating lock file %q", cr.corpusLockPath)
		}
		break
	}
	return func() {
		_ = f.Close()
		_ = os.Remove(cr.corpusLockPath)
	}, nil
}

// openCorpus opens the corpus file, the caller is responsible
// for locking.
func (cr *Reader) openCorpus(write bool) (corpusFile *os.File, unlockFn func(), err error) {
	readFlags := os.O_RDONLY
	if write {
		readFlags = os.O_RDWR | os.O_CREATE
		unlockFn, err = cr.lockCorpus()
	}
	if err != nil {
		return nil, nil, err
	}
	corpusFile, err = os.OpenFile(cr.corpusFilePath, readFlags, 0666)
	if err != nil {
		if unlockFn != nil {
			unlockFn()
		}
		return nil, nil, err
	}
	return corpusFile, unlockFn, nil
}

func (cr *Reader) readCorpusWithFile(corpusFile *os.File) error {
	offset, err := corpusFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	_, err = corpusFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if offset != 0 {
		bytes := make([]byte, offset)
		_, err := corpusFile.Read(bytes)
		if err != nil {
			return err
		}
		err = protoutil.Unmarshal(bytes, &cr.diskState)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadCorpus reads all the entries from the corpus from disk into memory.
func (cr *Reader) ReadCorpus() error {
	corpusFile, _, err := cr.openCorpus(false)
	if err != nil {
		return err
	}
	defer corpusFile.Close()
	err = cr.readCorpusWithFile(corpusFile)
	if err != nil {
		return err
	}
	return nil
}

// GetNumEntries gets the number of entries in the corpus file.
func (cr *Reader) GetNumEntries() int {
	return len(cr.diskState.CorpusArray)
}

// GetCorpus gets a corpus by index, and converts it into the schema changer
// state.
func (cr *Reader) GetCorpus(idx int) (name string, state *scpb.CurrentState) {
	corpus := cr.diskState.CorpusArray[idx]
	return corpus.Name, &scpb.CurrentState{
		TargetState: *corpus.TargetState,
		Initial:     corpus.Status,
		Current:     corpus.Status,
		InRollback:  corpus.InRollback,
		Revertible:  corpus.Revertible,
	}
}
