// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
)

// fileMetadata captures the relevant stats associated with a given file
// on-disk. This is used for caching.
type fileMetadata struct {
	size    int64
	modTime time.Time
}

// Phase is an enumeration of the three kinds of phases the watcher goes
// through (as in a state machine).
type Phase int

const (
	// The first phase wherein we perform an initial caching of all artifact
	// metadata.
	initialCachingPhase Phase = iota
	// The second phase wherein we may update any files that have been newly
	// updated since the first round of caching.
	incrementalUpdatePhase
	// The final phase wherein we stage ALL artifacts that have not been
	// staged yet.
	finalizePhase
)

var archivedCdepRegex = regexp.MustCompile("^external/archived_cdep_libgeos_[[:alpha:]]*/")

// A watcher watches the status of a build and regularly copies over relevant
// artifacts.
type watcher struct {
	// Channel to watch for completion of the build.
	completion <-chan error
	info       buildInfo
	// Map of file path -> file metadata. Cache used to determine whether
	// any given file is up-to-date. If the fileMetadata doesn't match the
	// latest file `stat`, we assume the file's been updated and read it
	// again.
	fileToMeta map[string]fileMetadata
	// Map of file path -> unit struct for all files that have already been
	// staged. Useful because we don't ever WANT to stage the same file more
	// than twice, and because we need to make sure we've staged everything
	// in the finalize phase.
	fileToStaged map[string]struct{}
}

// makeWatcher returns an appropriate watcher.
func makeWatcher(completion <-chan error, info buildInfo) *watcher {
	return &watcher{
		completion:   completion,
		info:         info,
		fileToMeta:   make(map[string]fileMetadata),
		fileToStaged: make(map[string]struct{}),
	}
}

// Watch performs most of the heavy lifting for bazci by monitoring the
// progress of the ongoing Bazel build (represented by the `completion`
// channel in the watcher), staging any test artifacts that are produced.
func (w watcher) Watch() error {
	// First, cache the file metadata for all test artifacts.
	err := w.stageTestArtifacts(initialCachingPhase)
	if err != nil {
		return err
	}
	// The main watch loop.
	for {
		select {
		case buildErr := <-w.completion:
			// Note that even if the build failed, we still want to
			// try to stage test artifacts.
			testErr := w.stageTestArtifacts(finalizePhase)
			// Also stage binary artifacts this time.
			binErr := w.stageBinaryArtifacts()
			// Only check errors after we're done staging all
			// artifacts -- we don't want to miss anything because
			// the build failed.
			if buildErr != nil {
				return buildErr
			}
			if testErr != nil {
				return testErr
			}
			if binErr != nil {
				return binErr
			}
			return nil
		case <-time.After(10 * time.Second):
			// Otherwise, every 10 seconds, stage the latest test
			// artifacts.
			err := w.stageTestArtifacts(incrementalUpdatePhase)
			if err != nil {
				return err
			}
		}
	}
}

// stageTestArtifacts copies the latest test artifacts into the artifactsDir.
// The "test artifacts" are the test.log (which is copied verbatim) and test.xml
// (which we need to munge a little bit before copying).
func (w watcher) stageTestArtifacts(phase Phase) error {
	targetToRelDir := func(target string) string {
		return strings.ReplaceAll(strings.TrimPrefix(target, "//"), ":", "/")
	}
	for _, test := range w.info.tests {
		// relDir is the directory under bazel-testlogs where the test
		// output files can be found.
		relDir := targetToRelDir(test)
		for _, tup := range []struct {
			relPath string
			stagefn func(srcContent []byte, outFile io.Writer) error
		}{
			{path.Join(relDir, "test.log"), copyContentTo},
			{path.Join(relDir, "*", "test.log"), copyContentTo},
			{path.Join(relDir, "test.xml"), bazelutil.MungeTestXML},
			{path.Join(relDir, "*", "test.xml"), bazelutil.MungeTestXML},
		} {
			err := w.maybeStageArtifact(w.info.testlogsDir, tup.relPath, 0644, phase,
				tup.stagefn)
			if err != nil {
				return err
			}
		}
	}
	// go_transition_tests have their own special log directory.
	for test, transitionTestLogsDir := range w.info.transitionTests {
		relDir := targetToRelDir(test)
		for _, tup := range []struct {
			relPath string
			stagefn func(srcContent []byte, outFile io.Writer) error
		}{
			{path.Join(relDir, "test.log"), copyContentTo},
			{path.Join(relDir, "test.xml"), bazelutil.MungeTestXML},
		} {
			err := w.maybeStageArtifact(transitionTestLogsDir, tup.relPath, 0644, phase, tup.stagefn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// stageBinaryArtifacts stages the latest binary artifacts from the build.
// (We only ever stage binary artifacts during the finalize phase -- it isn't
// important for them to pop up before the build is complete, as with test
// results.)
func (w watcher) stageBinaryArtifacts() error {
	crossConfig := getCrossConfig()
	crossConfig = strings.TrimPrefix(crossConfig, "cross")
	for _, bin := range w.info.goBinaries {
		relBinPath := bazelutil.OutputOfBinaryRule(bin, crossConfig == "windows")
		err := w.maybeStageArtifact(w.info.binDir, relBinPath, 0755, finalizePhase,
			copyContentTo)
		if err != nil {
			return err
		}
	}
	for _, bin := range w.info.genruleTargets {
		// Ask Bazel to list all the outputs of the genrule.
		query, err := runBazelReturningStdout("query", "--output=xml", bin)
		if err != nil {
			return err
		}
		outs, err := bazelutil.OutputsOfGenrule(bin, query)
		if err != nil {
			return err
		}
		for _, relBinPath := range outs {
			err := w.maybeStageArtifact(w.info.binDir, relBinPath, 0644, finalizePhase, copyContentTo)
			if err != nil {
				return err
			}
		}
	}
	if w.info.geos {
		// geos doesn't have a stable, predictable location, so we have
		// to hard-code this.
		var ext string
		rootDir := "archived_cdep_libgeos_" + crossConfig
		libDir := "lib"
		if crossConfig == "windows" {
			ext = "dll"
			// NB: the libs end up in the "bin" subdir of libgeos
			// on Windows.
			libDir = "bin"
		} else if strings.HasPrefix(crossConfig, "macos") {
			ext = "dylib"
		} else {
			ext = "so"
		}
		for _, relBinPath := range []string{
			fmt.Sprintf("external/%s/%s/libgeos_c.%s", rootDir, libDir, ext),
			fmt.Sprintf("external/%s/%s/libgeos.%s", rootDir, libDir, ext),
		} {
			err := w.maybeStageArtifact(w.info.executionRootDir, relBinPath, 0644, finalizePhase, copyContentTo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// copyContentTo writes the given srcContent to the output file. Helper function
// meant to be used with maybeStageArtifact.
func copyContentTo(srcContent []byte, outFile io.Writer) error {
	_, err := outFile.Write(srcContent)
	return err
}

// cancelableWriter is an io.WriteCloser with the following properties:
// 1. The entire contents of the file is accumulated in memory in the `buf`.
// 2. The file is written to disk when Close() is called...
// 3. ...UNLESS Canceled is set.
// These properties allow us to make sure we only stage artifacts once --
// if an artifact isn't ready to be staged, just set "Canceled = true" and we
// can trivially skip it for now.
type cancelableWriter struct {
	filename string
	perm     os.FileMode
	buf      bytes.Buffer
	Canceled bool
}

// Assertion to ensure we implement the WriteCloser interface.
var _ io.WriteCloser = (*cancelableWriter)(nil)

func (w *cancelableWriter) Write(p []byte) (n int, err error) {
	n, err = w.buf.Write(p)
	if err != nil {
		w.Canceled = true
	}
	return
}

func (w *cancelableWriter) Close() error {
	if !w.Canceled {
		err := os.MkdirAll(path.Dir(w.filename), 0755)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, w.perm)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.Write(w.buf.Bytes())
		return err
	}
	return nil
}

// maybeStageArtifact stages one or more build or test artifacts in the
// artifactsDir unless the cache indicates the file to be staged is already
// up-to-date. maybeStageArtifact stages all files in the given `root` (e.g.
// either `bazel-testlogs` or `bazel-bin`) matching the given `pattern`.
// `filepath.Glob()` is used to determine which files match the given pattern.
// If maybeStageArtifact determines that a file needs to be staged, it is
// created in the artifactsDir at the same `root` and relative path with the
// given `perm`, and the `stagefn` is called to populate the contents of the
// newly staged file. If the source artifact does not exist, or has not been
// updated since the last time it was staged, maybeStageArtifact does nothing.
// If the `stagefn` returns a non-nil error, then the artifact is not staged.
// (Errors will not be propagated back up to the caller of maybeStageArtifact
// unless we're in the "finalize" phase -- errors can happen sporadically if
// we're not finalizing, especially if we read an artifact while it's in the
// process of being written.)
//
// In the initialCachingPhase, NO artifacts will be staged, but
// maybeStageArtifact will still stat the source file and cache its metadata.
// This is important because Bazel aggressively caches build and test artifacts,
// so just because a file exists, doesn't necessarily mean that it should be
// staged. For example -- if we're running //pkg/settings:settings_test, and
// bazel-testlogs/pkg/settings/settings_test/test.xml exists, it may just be
// because that file was created by a PREVIOUS run of :settings_test. Staging
// it right away makes no sense in this case -- the XML will contain old data.
// Instead, we note the file metadata so that we know to re-stage it when the
// file is updated. Meanwhile, we force-stage everything that hasn't yet been
// staged once in the finalizePhase, to cover cases where Bazel reused the old
// cached artifact.
//
// For example, one might stage a set of log files with a call like:
// w.maybeStageArtifact(testlogsSourceDir, "pkg/server/server_test/*/test.log",
//                      0644, incrementalUpdatePhase, copycontentTo)
func (w watcher) maybeStageArtifact(
	rootPath string,
	pattern string,
	perm os.FileMode,
	phase Phase,
	stagefn func(srcContent []byte, outFile io.Writer) error,
) error {
	stage := func(srcPath, destPath string) error {
		contents, err := os.ReadFile(srcPath)
		if err != nil {
			return err
		}
		writer := &cancelableWriter{filename: destPath, perm: perm}
		err = stagefn(contents, writer)
		if err != nil {
			// If the stagefn fails, make sure to cancel the write
			// so we don't just write garbage to the file.
			writer.Canceled = true
			// Sporadic failures are OK if we're not finalizing.
			if phase == finalizePhase {
				return err
			}
			log.Printf("WARNING: got error %v trying to stage artifact %s", err,
				destPath)
			return nil
		}
		err = writer.Close()
		if err != nil {
			return err
		}
		var toInsert struct{}
		w.fileToStaged[srcPath] = toInsert
		return nil
	}

	matches, err := filepath.Glob(path.Join(rootPath, pattern))
	if err != nil {
		return err
	}
	for _, srcPath := range matches {
		relPath, err := filepath.Rel(rootPath, srcPath)
		if err != nil {
			return err
		}
		var destPath string
		if strings.HasPrefix(relPath, "external/archived_cdep_libgeos_") {
			destPath = path.Join(artifactsDir, archivedCdepRegex.ReplaceAllString(relPath, "bazel-bin/c-deps/libgeos/"))
		} else {
			artifactsSubdir := filepath.Base(rootPath)
			if !strings.HasPrefix(artifactsSubdir, "bazel") {
				artifactsSubdir = "bazel-" + artifactsSubdir
			}
			destPath = path.Join(artifactsDir, artifactsSubdir, relPath)
		}

		stat, err := os.Stat(srcPath)
		// stat errors can simply be ignored -- if the file doesn't
		// exist, we skip it. (Note that this is unlikely, but due to a
		// race could occur.)
		if err != nil {
			continue
		}
		meta := fileMetadata{size: stat.Size(), modTime: stat.ModTime()}
		oldMeta, oldMetaOk := w.fileToMeta[srcPath]
		// If we don't have metadata for this file yet, or if the
		// metadata has been updated, stage the file.
		if !oldMetaOk || meta != oldMeta {
			switch phase {
			case initialCachingPhase:
				// Don't stage, but do make a note of the
				// metadata.
			case incrementalUpdatePhase, finalizePhase:
				_, staged := w.fileToStaged[srcPath]
				// This is not a great situation: we've been asked to stage a file
				// that was already staged. Do it again, but print a warning. Log
				// files are OK, since TC doesn't parse them.
				if staged && !strings.HasSuffix(destPath, ".log") {
					log.Printf("WARNING: re-staging already-staged file at %s",
						destPath)
				}
				err := stage(srcPath, destPath)
				if err != nil {
					return err
				}
			}
			w.fileToMeta[srcPath] = meta
		}
		_, staged := w.fileToStaged[srcPath]
		// During the finalize phase, stage EVERYTHING that hasn't been
		// staged yet. This is our last chance!
		if !staged && phase == finalizePhase {
			err := stage(srcPath, destPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
