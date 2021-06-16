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
	"encoding/xml"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// SourceDir is an enumeration of possible output locations.
type SourceDir int

const (
	// Represents `bazel-testlogs`.
	testlogsSourceDir SourceDir = iota
	// Represents `bazel-bin`.
	binSourceDir
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
	for _, test := range w.info.tests {
		// relDir is the directory under bazel-testlogs where the test
		// output files can be found.
		relDir := strings.ReplaceAll(strings.TrimPrefix(test, "//"), ":", "/")
		for _, tup := range []struct {
			relPath string
			stagefn func(srcContent []byte, outFile io.Writer) error
		}{
			{path.Join(relDir, "test.log"), copyContentTo},
			{path.Join(relDir, "*", "test.log"), copyContentTo},
			{path.Join(relDir, "test.xml"), mungeTestXML},
			{path.Join(relDir, "*", "test.xml"), mungeTestXML},
		} {
			err := w.maybeStageArtifact(testlogsSourceDir, tup.relPath, 0666, phase,
				tup.stagefn)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Below are data structures representing the `test.xml` schema.
// Ref: https://github.com/bazelbuild/rules_go/blob/master/go/tools/bzltestutil/xml.go
type testSuites struct {
	XMLName xml.Name    `xml:"testsuites"`
	Suites  []testSuite `xml:"testsuite"`
}

type testSuite struct {
	XMLName   xml.Name   `xml:"testsuite"`
	TestCases []testCase `xml:"testcase"`
	Attrs     []xml.Attr `xml:",any,attr"`
}

type testCase struct {
	XMLName xml.Name `xml:"testcase"`
	// Note that we deliberately exclude the `classname` attribute. It never
	// contains useful information (always just the name of the package --
	// this isn't Java so there isn't a classname) and excluding it causes
	// the TeamCity UI to display the same data in a slightly more coherent
	// and usable way.
	Name    string      `xml:"name,attr"`
	Time    string      `xml:"time,attr"`
	Failure *xmlMessage `xml:"failure,omitempty"`
	Error   *xmlMessage `xml:"error,omitempty"`
	Skipped *xmlMessage `xml:"skipped,omitempty"`
}

type xmlMessage struct {
	Message  string     `xml:"message,attr"`
	Attrs    []xml.Attr `xml:",any,attr"`
	Contents string     `xml:",chardata"`
}

// stageBinaryArtifacts stages the latest binary artifacts from the build.
// (We only ever stage binary artifacts during the finalize phase -- it isn't
// important for them to pop up before the build is complete, as with test
// results.)
func (w watcher) stageBinaryArtifacts() error {
	for _, bin := range w.info.goBinaries {
		// Convert a target like `//pkg/cmd/cockroach-short` to the
		// relative path atop bazel-bin where that file can be found --
		// in this example, `pkg/cmd/cockroach-short/cockroach-short_/cockroach-short.
		head := strings.ReplaceAll(strings.TrimPrefix(bin, "//"), ":", "/")
		components := strings.Split(bin, ":")
		relBinPath := path.Join(head+"_", components[len(components)-1])
		if usingCrossWindowsConfig() {
			relBinPath = relBinPath + ".exe"
		}
		err := w.maybeStageArtifact(binSourceDir, relBinPath, 0777, finalizePhase,
			copyContentTo)
		if err != nil {
			return err
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

// mungeTestXML parses and slightly munges the XML in the source file and writes
// it to the output file. Helper function meant to be used with
// maybeStageArtifact.
func mungeTestXML(srcContent []byte, outFile io.Writer) error {
	// Parse the XML into a testSuites struct.
	suites := testSuites{}
	err := xml.Unmarshal(srcContent, &suites)
	// Note that we return an error if parsing fails. This isn't
	// unexpected -- if we read the XML file before it's been
	// completely written to disk, that will happen. Returning the
	// error will cancel the write to disk, which is exactly what we
	// want.
	if err != nil {
		return err
	}
	// We only want the first test suite in the list of suites.
	munged, err := xml.MarshalIndent(&suites.Suites[0], "", "\t")
	if err != nil {
		return err
	}
	_, err = outFile.Write(munged)
	if err != nil {
		return err
	}
	// Insert a newline just to make our lives a little easier.
	_, err = outFile.Write([]byte("\n"))
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
		err := os.MkdirAll(path.Dir(w.filename), 0777)
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
// In the intialCachingPhase, NO artifacts will be staged, but
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
//                      0666, incrementalUpdatePhase, copycontentTo)
func (w watcher) maybeStageArtifact(
	root SourceDir,
	pattern string,
	perm os.FileMode,
	phase Phase,
	stagefn func(srcContent []byte, outFile io.Writer) error,
) error {
	var rootPath string
	var artifactsSubdir string
	switch root {
	case testlogsSourceDir:
		rootPath = w.info.testlogsDir
		artifactsSubdir = "bazel-testlogs"
	case binSourceDir:
		rootPath = w.info.binDir
		artifactsSubdir = "bazel-bin"
	}
	stage := func(srcPath, destPath string) error {
		contents, err := ioutil.ReadFile(srcPath)
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
		destPath := path.Join(artifactsDir, artifactsSubdir, relPath)

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
