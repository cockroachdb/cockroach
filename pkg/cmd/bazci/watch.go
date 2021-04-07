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
	"encoding/xml"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
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
	files map[string]fileMetadata
}

// makeWatcher returns an appropriate watcher.
func makeWatcher(completion <-chan error, info buildInfo) *watcher {
	return &watcher{
		completion: completion,
		info:       info,
		files:      make(map[string]fileMetadata),
	}
}

// Watch performs most of the heavy lifting for bazci by monitoring the
// progress of the ongoing Bazel build (represented by the `completion`
// channel in the watcher), staging any test artifacts that are produced.
func (w watcher) Watch() error {
	for {
		select {
		case buildErr := <-w.completion:
			// Note that even if the build failed, we still want to
			// try to stage test artifacts.
			testErr := w.stageTestArtifacts()
			// Also stage binary artifacts this time.
			binErr := w.stageBinaryArtifacts()
			// Only check errors after we're done staging all
			// artifacts.
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
			err := w.stageTestArtifacts()
			if err != nil {
				return err
			}
		}
	}
}

// stageTestArtifacts copies the latest test artifacts into the artifactsDir.
// The "test artifacts" are the test.log (which is copied verbatim) and test.xml
// (which we need to munge a little bit before copying).
func (w watcher) stageTestArtifacts() error {
	for _, test := range w.info.tests {
		// relDir is the directory under bazel-testlogs where the test
		// output files can be found.
		relDir := strings.ReplaceAll(strings.TrimPrefix(test, "//"), ":", "/")
		// First, copy the log file verbatim.
		relLogFilePath := path.Join(relDir, "test.log")
		err := w.maybeStageArtifact(testlogsSourceDir, relLogFilePath, 0666, copyContentTo)
		if err != nil {
			return err
		}
		// Munge and copy the xml file.
		relXMLFilePath := path.Join(relDir, "test.xml")
		err = w.maybeStageArtifact(testlogsSourceDir, relXMLFilePath, 0666,
			func(srcContent []byte, outFile io.Writer) error {
				// Parse the XML into a testSuites struct.
				suites := testSuites{}
				err := xml.Unmarshal(srcContent, &suites)
				if err != nil {
					switch {
					// Syntax errors we can just ignore. (Maybe we read the file
					// while it was in the process of being written?) Better to
					// have something than nothing, so copy the raw content.
					case errors.HasType(err, (*xml.SyntaxError)(nil)):
						_, err = outFile.Write(srcContent)
						return err
					default:
						return err
					}
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
			})
		if err != nil {
			return err
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
func (w watcher) stageBinaryArtifacts() error {
	for _, bin := range w.info.goBinaries {
		// Convert a target like `//pkg/cmd/cockroach-short` to the
		// relative path atop bazel-bin where that file can be found --
		// in this example, `pkg/cmd/cockroach-short/cockroach-short_/cockroach-short.
		head := strings.ReplaceAll(strings.TrimPrefix(bin, "//"), ":", "/")
		components := strings.Split(bin, ":")
		relBinPath := path.Join(head+"_", components[len(components)-1])
		err := w.maybeStageArtifact(binSourceDir, relBinPath, 0777, copyContentTo)
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

// maybeStageArtifact stages a build or test artifact in the artifactsDir unless
// the cache indicates the file is already up-to-date. maybeStageArtifact stages
// the file from the given `root` (e.g. either `bazel-testlogs` or `bazel-bin`)
// and at the given relative path. If maybeStageArtifact determines that a file
// needs to be staged, it is created in the artifactsDir at the same `root` and
// `relPath` with the given `perm`, and the `stagefn` is called to populate the
// contents of the newly staged file. With the `stagefn`, callers can choose
// whether to copy the file verbatim or edit it. If the source artifact does not
// exist, then maybeStageArtifact does nothing.
//
// For example, one might stage a log file with a call like:
// w.maybeStageArtifact(testlogsSourceDir, "pkg/server/server_test/test.log", 0666, copycontentTo)
func (w watcher) maybeStageArtifact(
	root SourceDir,
	relPath string,
	perm os.FileMode,
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
	// Fully qualified source and destination paths.
	srcPath := path.Join(rootPath, relPath)
	destPath := path.Join(artifactsDir, artifactsSubdir, relPath)

	stat, err := os.Stat(srcPath)
	if err == nil {
		meta := fileMetadata{size: stat.Size(), modTime: stat.ModTime()}
		oldMeta, ok := w.files[srcPath]
		// Stage if we haven't staged this file yet, or if the size or modTime has been
		// updated since the last time we staged it.
		if !ok || meta != oldMeta {
			err := os.MkdirAll(path.Dir(destPath), 0777)
			if err != nil {
				return err
			}
			f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
			if err != nil {
				return err
			}
			defer f.Close()
			contents, err := ioutil.ReadFile(srcPath)
			if err != nil {
				return err
			}
			err = stagefn(contents, f)
			if err != nil {
				return err
			}
			w.files[srcPath] = meta
		}
	}
	// stat errors can simply be ignored -- if the file doesn't exist, we
	// skip it.
	return nil
}
