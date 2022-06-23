// Copyright 2018 The Bazel Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bazel

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	RUNFILES_MANIFEST_FILE = "RUNFILES_MANIFEST_FILE"
	RUNFILES_DIR           = "RUNFILES_DIR"
)

// Runfile returns an absolute path to the file named by "path", which
// should be a relative path from the workspace root to the file within
// the bazel workspace.
//
// Runfile may be called from tests invoked with 'bazel test' and
// binaries invoked with 'bazel run'. On Windows,
// only tests invoked with 'bazel test' are supported.
func Runfile(path string) (string, error) {
	// Search in working directory
	if _, err := os.Stat(path); err == nil {
		return filepath.Abs(path)
	}

	if err := ensureRunfiles(); err != nil {
		return "", err
	}

	// Search manifest if we have one.
	if entry, ok := runfiles.index[path]; ok {
		return entry.Path, nil
	}

	// Search the main workspace.
	if runfiles.workspace != "" {
		mainPath := filepath.Join(runfiles.dir, runfiles.workspace, path)
		if _, err := os.Stat(mainPath); err == nil {
			return mainPath, nil
		}
	}

	// Search other workspaces.
	for _, w := range runfiles.workspaces {
		workPath := filepath.Join(runfiles.dir, w, path)
		if _, err := os.Stat(workPath); err == nil {
			return workPath, nil
		}
	}

	return "", fmt.Errorf("Runfile %s: could not locate file", path)
}

// FindBinary returns an absolute path to the binary built from a go_binary
// rule in the given package with the given name. FindBinary is similar to
// Runfile, but it accounts for varying configurations and file extensions,
// which may cause the binary to have different paths on different platforms.
//
// FindBinary may be called from tests invoked with 'bazel test' and
// binaries invoked with 'bazel run'. On Windows,
// only tests invoked with 'bazel test' are supported.
func FindBinary(pkg, name string) (string, bool) {
	if err := ensureRunfiles(); err != nil {
		return "", false
	}

	// If we've gathered a list of runfiles, either by calling ListRunfiles or
	// parsing the manifest on Windows, just use that instead of searching
	// directories. Return the first match. The manifest on Windows may contain
	// multiple entries for the same file.
	if runfiles.list != nil {
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		for _, entry := range runfiles.list {
			if path.Base(entry.ShortPath) != name {
				continue
			}
			pkgDir := path.Dir(path.Dir(entry.ShortPath))
			if pkgDir == "." {
				pkgDir = ""
			}
			if pkgDir != pkg {
				continue
			}
			return entry.Path, true
		}
		return "", false
	}

	dir, err := Runfile(pkg)
	if err != nil {
		return "", false
	}
	var found string
	stopErr := errors.New("stop")
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		base := filepath.Base(path)
		stem := strings.TrimSuffix(base, ".exe")
		if stem != name {
			return nil
		}
		if runtime.GOOS != "windows" {
			if st, err := os.Stat(path); err != nil {
				return err
			} else if st.Mode()&0111 == 0 {
				return nil
			}
		}
		if stem == name {
			found = path
			return stopErr
		}
		return nil
	})
	if err == stopErr {
		return found, true
	} else {
		return "", false
	}
}

// A RunfileEntry describes a runfile.
type RunfileEntry struct {
	// Workspace is the bazel workspace the file came from. For example,
	// this would be "io_bazel_rules_go" for a file in rules_go.
	Workspace string

	// ShortPath is a relative, slash-separated path from the workspace root
	// to the file. For non-binary files, this may be passed to Runfile
	// to locate a file.
	ShortPath string

	// Path is an absolute path to the file.
	Path string
}

// ListRunfiles returns a list of available runfiles.
func ListRunfiles() ([]RunfileEntry, error) {
	if err := ensureRunfiles(); err != nil {
		return nil, err
	}

	if runfiles.list == nil && runfiles.dir != "" {
		runfiles.listOnce.Do(func() {
			var list []RunfileEntry
			haveWorkspaces := strings.HasSuffix(runfiles.dir, ".runfiles") && runfiles.workspace != ""

			err := filepath.Walk(runfiles.dir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				rel, _ := filepath.Rel(runfiles.dir, path)
				rel = filepath.ToSlash(rel)
				if rel == "." {
					return nil
				}

				var workspace, shortPath string
				if haveWorkspaces {
					if i := strings.IndexByte(rel, '/'); i < 0 {
						return nil
					} else {
						workspace, shortPath = rel[:i], rel[i+1:]
					}
				} else {
					workspace, shortPath = "", rel
				}

				list = append(list, RunfileEntry{Workspace: workspace, ShortPath: shortPath, Path: path})
				return nil
			})
			if err != nil {
				runfiles.err = err
				return
			}
			runfiles.list = list
		})
	}
	return runfiles.list, runfiles.err
}

// TestWorkspace returns the name of the Bazel workspace for this test.
// TestWorkspace returns an error if the TEST_WORKSPACE environment variable
// was not set or SetDefaultTestWorkspace was not called.
func TestWorkspace() (string, error) {
	if err := ensureRunfiles(); err != nil {
		return "", err
	}
	if runfiles.workspace != "" {
		return runfiles.workspace, nil
	}
	return "", errors.New("TEST_WORKSPACE not set and SetDefaultTestWorkspace not called")
}

// SetDefaultTestWorkspace allows you to set a fake value for the
// environment variable TEST_WORKSPACE if it is not defined. This is useful
// when running tests on the command line and not through Bazel.
func SetDefaultTestWorkspace(w string) {
	ensureRunfiles()
	runfiles.workspace = w
}

// RunfilesPath return the path to the runfiles tree.
// It will return an error if there is no runfiles tree, for example because
// the executable is run on Windows or was not invoked with 'bazel test'
// or 'bazel run'.
func RunfilesPath() (string, error) {
	if err := ensureRunfiles(); err != nil {
		return "", err
	}
	if runfiles.dir == "" {
		if runtime.GOOS == "windows" {
			return "", errors.New("RunfilesPath: no runfiles directory on windows")
		} else {
			return "", errors.New("could not locate runfiles directory")
		}
	}
	if runfiles.workspace == "" {
		return "", errors.New("could not locate runfiles workspace")
	}
	return filepath.Join(runfiles.dir, runfiles.workspace), nil
}

// EnterRunfiles locates the directory under which a built binary can find its data dependencies
// using relative paths, and enters that directory.
//
// "workspace" indicates the name of the current project, "pkg" indicates the relative path to the
// build package that contains the binary target, "binary" indicates the basename of the binary
// searched for, and "cookie" indicates an arbitrary data file that we expect to find within the
// runfiles tree.
//
// DEPRECATED: use RunfilesPath instead.
func EnterRunfiles(workspace string, pkg string, binary string, cookie string) error {
	runfiles, ok := findRunfiles(workspace, pkg, binary, cookie)
	if !ok {
		return fmt.Errorf("cannot find runfiles tree")
	}
	if err := os.Chdir(runfiles); err != nil {
		return fmt.Errorf("cannot enter runfiles tree: %v", err)
	}
	return nil
}

var runfiles = struct {
	once, listOnce sync.Once

	// list is a list of known runfiles, either loaded from the manifest
	// or discovered by walking the runfile directory.
	list []RunfileEntry

	// index maps runfile short paths to absolute paths.
	index map[string]RunfileEntry

	// dir is a path to the runfile directory. Typically this is a directory
	// named <target>.runfiles, with a subdirectory for each workspace.
	dir string

	// workspace is workspace where the binary or test was built.
	workspace string

	// workspaces is a list of other workspace names.
	workspaces []string

	// err is set when there is an error loading runfiles, for example,
	// parsing the manifest.
	err error
}{}

func ensureRunfiles() error {
	runfiles.once.Do(initRunfiles)
	return runfiles.err
}

func initRunfiles() {
	manifest := os.Getenv("RUNFILES_MANIFEST_FILE")
	if manifest != "" {
		// On Windows, Bazel doesn't create a symlink tree of runfiles because
		// Windows doesn't support symbolic links by default. Instead, runfile
		// locations are written to a manifest file.
		runfiles.index = make(map[string]RunfileEntry)
		data, err := ioutil.ReadFile(manifest)
		if err != nil {
			runfiles.err = err
			return
		}
		lineno := 0
		for len(data) > 0 {
			i := bytes.IndexByte(data, '\n')
			var line []byte
			if i < 0 {
				line = data
				data = nil
			} else {
				line = data[:i]
				data = data[i+1:]
			}
			lineno++
			line = bytes.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			e := bytes.SplitN(line, []byte(" "), 2)
			if len(e) < 2 {
				runfiles.err = fmt.Errorf("error parsing runfiles manifest: %s:%d: no space", manifest, lineno)
				return
			}

			entry := RunfileEntry{ShortPath: string(e[0]), Path: string(e[1])}
			if i := strings.IndexByte(entry.ShortPath, '/'); i >= 0 {
				entry.Workspace = entry.ShortPath[:i]
				entry.ShortPath = entry.ShortPath[i+1:]
			}
			if strings.HasPrefix(entry.ShortPath, "external/") {
				entry.ShortPath = entry.ShortPath[len("external/"):]
				if i := strings.IndexByte(entry.ShortPath, '/'); i >= 0 {
					entry.Workspace = entry.ShortPath[:i]
					entry.ShortPath = entry.ShortPath[i+1:]
				}
			}

			runfiles.list = append(runfiles.list, entry)
			runfiles.index[entry.ShortPath] = entry
		}
	}

	runfiles.workspace = os.Getenv("TEST_WORKSPACE")

	if dir := os.Getenv("RUNFILES_DIR"); dir != "" {
		runfiles.dir = dir
	} else if dir = os.Getenv("TEST_SRCDIR"); dir != "" {
		runfiles.dir = dir
	} else if runtime.GOOS != "windows" {
		dir, err := os.Getwd()
		if err != nil {
			runfiles.err = fmt.Errorf("error localting runfiles dir: %v", err)
			return
		}

		parent := filepath.Dir(dir)
		if strings.HasSuffix(parent, ".runfiles") {
			runfiles.dir = parent
			if runfiles.workspace == "" {
				runfiles.workspace = filepath.Base(dir)
			}
		} else {
			runfiles.err = errors.New("could not locate runfiles directory")
			return
		}
	}

	if runfiles.dir != "" {
		fis, err := ioutil.ReadDir(runfiles.dir)
		if err != nil {
			runfiles.err = fmt.Errorf("could not open runfiles directory: %v", err)
			return
		}
		for _, fi := range fis {
			if fi.IsDir() {
				runfiles.workspaces = append(runfiles.workspaces, fi.Name())
			}
		}
		sort.Strings(runfiles.workspaces)
	}
}

// getCandidates returns the list of all possible "prefix/suffix" paths where there might be an
// optional component in-between the two pieces.
//
// This function exists to cope with issues #1239 because we cannot tell where the built Go
// binaries are located upfront.
//
// DEPRECATED: only used by EnterRunfiles.
func getCandidates(prefix string, suffix string) []string {
	candidates := []string{filepath.Join(prefix, suffix)}
	if entries, err := ioutil.ReadDir(prefix); err == nil {
		for _, entry := range entries {
			candidate := filepath.Join(prefix, entry.Name(), suffix)
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

// findRunfiles locates the directory under which a built binary can find its data dependencies
// using relative paths.
//
// DEPRECATED: only used by EnterRunfiles.
func findRunfiles(workspace string, pkg string, binary string, cookie string) (string, bool) {
	candidates := getCandidates(filepath.Join("bazel-bin", pkg), filepath.Join(binary+".runfiles", workspace))
	candidates = append(candidates, ".")

	for _, candidate := range candidates {
		if _, err := os.Stat(filepath.Join(candidate, cookie)); err == nil {
			return candidate, true
		}
	}
	return "", false
}
