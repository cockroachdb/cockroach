// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/errors"
)

type configuration struct {
	Os   string
	Arch string
}

var (
	workspace                  string
	bazelBin                   string
	archivedCdepConfigurations = []configuration{
		{"linux", "amd64"},
		{"linux", "arm64"},
		{"darwin", "amd64"},
		{"darwin", "arm64"},
		{"windows", "amd64"},
	}
)

func logCommand(cmd string, args ...string) {
	var fullArgs []string
	fullArgs = append(fullArgs, cmd)
	fullArgs = append(fullArgs, args...)
	log.Printf("$ %s", shellescape.QuoteCommand(fullArgs))
}

// getArchivedCdepString returns a non-empty string iff the force_build_cdeps
// config is not being used. This string is the name of the cross config used to
// build the pre-built c-deps, minus the "cross" prefix. This can be used to
// locate the pre-built c-dep in
// $EXECUTION_ROOT/external/archived_cdep_{LIB}_{ARCHIVED_CDEP_STRING}.
// If the returned string is empty then force_build_cdeps is set in which case
// the (non-pre-built) libraries can be found in $BAZEL_BIN/c-deps/{LIB}_foreign.
//
// You MUST build //build/bazelutil:test_force_build_cdeps before calling this
// function.
func getArchivedCdepString(bazelBin string) (string, error) {
	var ret string
	// If force_build_cdeps is set then the prebuilt libraries won't be in
	// the archived location anyway.
	forceBuildCdeps, err := os.ReadFile(filepath.Join(bazelBin, "build", "bazelutil", "test_force_build_cdeps.txt"))
	if err != nil {
		return "", err
	}
	// force_build_cdeps is activated if the length of this file is not 0.
	if len(forceBuildCdeps) == 0 {
		for _, config := range archivedCdepConfigurations {
			if config.Os == runtime.GOOS && config.Arch == runtime.GOARCH {
				ret = config.Os
				if ret == "darwin" {
					ret = "macos"
				}
				if config.Arch == "arm64" {
					ret += "arm"
				}
				break
			}
		}
	}
	return ret, nil
}

func getBazelInfo(info string) (string, error) {
	args := []string{"info", info, "--color=no"}
	logCommand("bazel", args...)
	cmd := exec.Command("bazel", args...)
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return "", errors.Wrapf(err, "Stderr: %s", errBuf.String())
	} else {
		return strings.TrimSpace(outBuf.String()), nil
	}
}

func generateCgo() error {
	if workspace == "" || bazelBin == "" {
		return errors.New("--workspace and --bazel-bin are required")
	}
	args := []string{"build", "//build/bazelutil:test_force_build_cdeps", "//c-deps:libjemalloc", "//c-deps:libproj"}
	if runtime.GOOS == "linux" {
		args = append(args, "//c-deps:libkrb5")
	}
	logCommand("bazel", args...)
	cmd := exec.Command("bazel", args...)
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "Stderr: %s", errBuf.String())
	}

	const cgoTmpl = `// GENERATED FILE DO NOT EDIT

package {{ .Package }}

// #cgo CPPFLAGS: {{ .CPPFlags }}
// #cgo LDFLAGS: {{ .LDFlags }}
import "C"
`

	tpl := template.Must(template.New("source").Parse(cgoTmpl))
	archived, err := getArchivedCdepString(bazelBin)
	if err != nil {
		return err
	}
	// Figure out where to find the c-deps libraries.
	var jemallocDir, projDir, krbDir string
	if archived != "" {
		execRoot, err := getBazelInfo("execution_root")
		if err != nil {
			return err
		}
		jemallocDir = filepath.Join(execRoot, "external", fmt.Sprintf("archived_cdep_libjemalloc_%s", archived))
		projDir = filepath.Join(execRoot, "external", fmt.Sprintf("archived_cdep_libproj_%s", archived))
		if runtime.GOOS == "linux" {
			krbDir = filepath.Join(execRoot, "external", fmt.Sprintf("archived_cdep_libkrb5_%s", archived))
		}
	} else {
		jemallocDir = filepath.Join(bazelBin, "c-deps/libjemalloc_foreign")
		projDir = filepath.Join(bazelBin, "c-deps/libproj_foreign")
		if runtime.GOOS == "linux" {
			krbDir = filepath.Join(bazelBin, "c-deps/libkrb5_foreign")
		}
	}

	srcToPersistentDirs := make(map[string]string, 3)
	srcToPersistentDirs[jemallocDir] = filepath.Join(workspace, "bin", "c-deps", filepath.Base(jemallocDir))
	srcToPersistentDirs[projDir] = filepath.Join(workspace, "bin", "c-deps", filepath.Base(projDir))
	if krbDir != "" {
		srcToPersistentDirs[krbDir] = filepath.Join(workspace, "bin", "c-deps", filepath.Base(krbDir))
	}

	cppFlags := fmt.Sprintf("-I%s", filepath.Join(srcToPersistentDirs[jemallocDir], "include"))
	ldFlags := fmt.Sprintf("-L%s -L%s", filepath.Join(srcToPersistentDirs[jemallocDir], "lib"), filepath.Join(srcToPersistentDirs[projDir], "lib"))
	if krbDir != "" {
		cppFlags += fmt.Sprintf(" -I%s", filepath.Join(srcToPersistentDirs[krbDir], "include"))
		ldFlags += fmt.Sprintf(" -L%s", filepath.Join(srcToPersistentDirs[krbDir], "lib"))
	}

	cgoPkgs := []string{
		"pkg/cli",
		"pkg/cli/clisqlshell",
		"pkg/server/status",
		"pkg/ccl/gssapiccl",
		"pkg/geo/geoproj",
	}

	for _, cgoPkg := range cgoPkgs {
		out, err := os.Create(filepath.Join(workspace, cgoPkg, "zcgo_flags.go"))
		if err != nil {
			return err
		}
		err = tpl.Execute(out, struct {
			Package  string
			CPPFlags string
			LDFlags  string
		}{Package: filepath.Base(cgoPkg), CPPFlags: cppFlags, LDFlags: ldFlags})
		if err != nil {
			return err
		}
	}

	// Copy jemallocDir, projDir, and krbDir to a persistent location (//bin/c-deps).
	if err := os.MkdirAll(filepath.Join(workspace, "bin", "c-deps"), 0755); err != nil {
		return err
	}
	// Ensure that overwriting existing ones is possible to prevent permissions errors.
	chmodCmd := exec.Command("chmod", "-R", "755", filepath.Join(workspace, "bin", "c-deps"))
	var chmodOutBuf, chmodErrBuf strings.Builder
	chmodCmd.Stderr = &chmodErrBuf
	chmodCmd.Stdout = &chmodOutBuf
	if err := chmodCmd.Run(); err != nil {
		return errors.Wrapf(err, "Output: %s - %s", chmodOutBuf.String(), chmodErrBuf.String())
	}
	for dirToCopy := range srcToPersistentDirs {
		if dirToCopy != "" {
			copyCmdArgs := []string{"-r", dirToCopy, filepath.Dir(srcToPersistentDirs[dirToCopy])}
			logCommand("cp", copyCmdArgs...)
			cmd := exec.Command("cp", copyCmdArgs...)
			var outBuf, errBuf strings.Builder
			cmd.Stdout = &outBuf
			cmd.Stderr = &errBuf
			cmd.Dir = workspace
			if err := cmd.Run(); err != nil {
				return errors.Wrapf(err, "Output: %s - %s", outBuf.String(), errBuf.String())
			}
		}
	}
	return nil
}

func main() {
	var err error
	workspace, err = getBazelInfo("workspace")
	if err != nil {
		log.Fatal(err)
	}
	bazelBin, err = getBazelInfo("bazel-bin")
	if err != nil {
		log.Fatal(err)
	}
	if err := generateCgo(); err != nil {
		log.Fatal(err)
	}
}
