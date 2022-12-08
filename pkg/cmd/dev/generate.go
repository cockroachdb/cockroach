// Copyright 2020 The Cockroach Authors.
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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"text/template"

	"github.com/spf13/cobra"
)

const mirrorFlag = "mirror"
const forceFlag = "force"

// makeGenerateCmd constructs the subcommand used to generate the specified
// artifacts.
func makeGenerateCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	generateCmd := &cobra.Command{
		Use:     "generate [target..]",
		Aliases: []string{"gen"},
		Short:   `Generate the specified files`,
		Long:    `Generate the specified files.`,
		Example: `
        dev generate
        dev generate bazel         # DEPS.bzl and BUILD.bazel files
        dev generate cgo           # files that help non-Bazel systems (IDEs, go) link to our C dependencies
        dev generate docs          # generates documentation
        dev generate diagrams      # generates syntax diagrams
        dev generate bnf           # generates syntax bnf files
        dev generate js            # generates JS protobuf client and seeds local tooling
        dev generate go            # generates go code (execgen, stringer, protobufs, etc.), plus everything 'cgo' generates
        dev generate go_nocgo      # generates go code (execgen, stringer, protobufs, etc.)
        dev generate protobuf      # *.pb.go files (subset of 'dev generate go')
        dev generate parser        # sql.go and parser dependencies (subset of 'dev generate go')
        dev generate optgen        # optgen targets (subset of 'dev generate go')
        dev generate execgen       # execgen targets (subset of 'dev generate go')
        dev generate schemachanger # schemachanger targets (subset of 'dev generate go')
        dev generate stringer      # stringer targets (subset of 'dev generate go')
        dev generate testlogic     # logictest generated code (subset of 'dev generate bazel')
`,
		Args: cobra.MinimumNArgs(0),
		// TODO(irfansharif): Errors but default just eaten up. Let's wrap these
		// invocations in something that prints out the appropriate error log
		// (especially considering we've SilenceErrors-ed things away).
		RunE: runE,
	}
	generateCmd.Flags().Bool(mirrorFlag, false, "mirror new dependencies to cloud storage (use if vendoring)")
	generateCmd.Flags().Bool(forceFlag, false, "force regeneration even if relevant files are unchanged from upstream")
	generateCmd.Flags().Bool(shortFlag, false, "if used for the bazel target, only update BUILD.bazel files and skip checks")
	return generateCmd
}

type configuration struct {
	Os   string
	Arch string
}

func (d *dev) generate(cmd *cobra.Command, targets []string) error {
	var generatorTargetMapping = map[string]func(cmd *cobra.Command) error{
		"bazel":         d.generateBazel,
		"bnf":           d.generateBNF,
		"cgo":           d.generateCgo,
		"diagrams":      d.generateDiagrams,
		"docs":          d.generateDocs,
		"execgen":       d.generateExecgen,
		"js":            d.generateJs,
		"go":            d.generateGo,
		"go_nocgo":      d.generateGoNoCgo,
		"logictest":     d.generateLogicTest,
		"protobuf":      d.generateProtobuf,
		"parser":        d.generateParser,
		"optgen":        d.generateOptGen,
		"schemachanger": d.generateSchemaChanger,
		"stringer":      d.generateStringer,
		"testlogic":     d.generateLogicTest,
	}

	if len(targets) == 0 {
		targets = append(targets, "bazel", "go_nocgo", "docs", "cgo")
	}

	targetsMap := make(map[string]struct{})
	for _, target := range targets {
		targetsMap[target] = struct{}{}
	}
	// NB: We have to run the bazel generator first if it's specified.
	if _, ok := targetsMap["bazel"]; ok {
		delete(targetsMap, "bazel")
		if err := generatorTargetMapping["bazel"](cmd); err != nil {
			return err
		}
	}
	{
		// In this case, generating both go and cgo would duplicate work.
		// Generate go_nocgo instead.
		_, includesGo := targetsMap["go"]
		_, includesCgo := targetsMap["cgo"]
		if includesGo && includesCgo {
			delete(targetsMap, "go")
			targetsMap["go_nocgo"] = struct{}{}
		}
	}
	{
		// generateGoAndDocs is a faster way to generate both (non-cgo)
		// go code as well as the docs
		_, includesGonocgo := targetsMap["go_nocgo"]
		_, includesDocs := targetsMap["docs"]
		if includesGonocgo && includesDocs {
			delete(targetsMap, "go_nocgo")
			delete(targetsMap, "docs")
			if err := d.generateGoAndDocs(cmd); err != nil {
				return err
			}
		}
	}

	for target := range targetsMap {
		generator, ok := generatorTargetMapping[target]
		if !ok {
			return fmt.Errorf("unrecognized target: %s", target)
		}
		if err := generator(cmd); err != nil {
			return err
		}
	}

	short := mustGetFlagBool(cmd, shortFlag)
	if short {
		return nil
	}

	ctx := cmd.Context()
	env := os.Environ()
	envvar := "COCKROACH_BAZEL_CHECK_FAST=1"
	d.log.Printf("export %s", envvar)
	env = append(env, envvar)
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	return d.exec.CommandContextWithEnv(ctx, env, filepath.Join(workspace, "build", "bazelutil", "check.sh"))
}

func (d *dev) generateBazel(cmd *cobra.Command) error {
	ctx := cmd.Context()

	short := mustGetFlagBool(cmd, shortFlag)
	if short {
		return d.exec.CommandContextInheritingStdStreams(
			ctx, "bazel", "run", "//:gazelle",
		)
	}

	mirror := mustGetFlagBool(cmd, mirrorFlag)
	force := mustGetFlagBool(cmd, forceFlag)
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	executable := filepath.Join(workspace, "build", "bazelutil", "bazel-generate.sh")
	env := os.Environ()
	if mirror {
		envvar := "COCKROACH_BAZEL_CAN_MIRROR=1"
		d.log.Printf("export %s", envvar)
		env = append(env, envvar)
	}
	if force {
		envvar := "COCKROACH_BAZEL_FORCE_GENERATE=1"
		d.log.Printf("export %s", envvar)
		env = append(env, envvar)
	}
	return d.exec.CommandContextWithEnv(ctx, env, executable)
}

func (d *dev) generateDocs(cmd *cobra.Command) error {
	ctx := cmd.Context()
	return d.generateTarget(ctx, "//pkg/gen:docs")
}

func (d *dev) generateExecgen(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:execgen")
}

func (d *dev) generateGoAndDocs(cmd *cobra.Command) error {
	ctx := cmd.Context()
	return d.generateTarget(ctx, "//pkg/gen")
}

func (d *dev) generateGo(cmd *cobra.Command) error {
	if err := d.generateGoNoCgo(cmd); err != nil {
		return err
	}
	return d.generateCgo(cmd)
}

func (d *dev) generateGoNoCgo(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:code")
}

func (d *dev) generateLogicTest(cmd *cobra.Command) error {
	ctx := cmd.Context()
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	return d.exec.CommandContextInheritingStdStreams(
		ctx, "bazel", "run", "pkg/cmd/generate-logictest", "--", fmt.Sprintf("-out-dir=%s", workspace),
	)
}

func (d *dev) generateProtobuf(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:go_proto")
}

func (d *dev) generateParser(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:parser")
}

func (d *dev) generateOptGen(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:optgen")
}

func (d *dev) generateSchemaChanger(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:schemachanger")
}

func (d *dev) generateStringer(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:stringer")
}

func (d *dev) generateDiagrams(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:diagrams")
}

func (d *dev) generateBNF(cmd *cobra.Command) error {
	return d.generateTarget(cmd.Context(), "//pkg/gen:bnf")
}

func (d *dev) generateTarget(ctx context.Context, target string) error {
	if err := d.exec.CommandContextInheritingStdStreams(
		ctx, "bazel", "run", target,
	); err != nil {
		// nolint:errwrap
		return fmt.Errorf("generating target %s: %s", target, err.Error())
	}
	return nil
}

func (d *dev) generateCgo(cmd *cobra.Command) error {
	ctx := cmd.Context()
	args := []string{"build", "//build/bazelutil:test_force_build_cdeps", "//c-deps:libjemalloc", "//c-deps:libproj"}
	if runtime.GOOS == "linux" {
		args = append(args, "//c-deps:libkrb5")
	}
	logCommand("bazel", args...)
	if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
		return err
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	bazelBin, err := d.getBazelBin(ctx)
	if err != nil {
		return err
	}

	const cgoTmpl = `// GENERATED FILE DO NOT EDIT

package {{ .Package }}

// #cgo CPPFLAGS: {{ .CPPFlags }}
// #cgo LDFLAGS: {{ .LDFlags }}
import "C"
`

	tpl := template.Must(template.New("source").Parse(cgoTmpl))
	archived, err := d.getArchivedCdepString(bazelBin)
	if err != nil {
		return err
	}
	// Figure out where to find the c-deps libraries.
	var jemallocDir, projDir, krbDir string
	if archived != "" {
		execRoot, err := d.getExecutionRoot(ctx)
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
	cppFlags := fmt.Sprintf("-I%s", filepath.Join(jemallocDir, "include"))
	ldFlags := fmt.Sprintf("-L%s -L%s", filepath.Join(jemallocDir, "lib"), filepath.Join(projDir, "lib"))
	if krbDir != "" {
		cppFlags += fmt.Sprintf(" -I%s", filepath.Join(krbDir, "include"))
		ldFlags += fmt.Sprintf(" -L%s", filepath.Join(krbDir, "lib"))
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

	return nil
}

func (d *dev) generateJs(cmd *cobra.Command) error {
	ctx := cmd.Context()

	args := []string{
		"build",
		"//pkg/ui/workspaces/eslint-plugin-crdb:eslint-plugin-crdb",
		"//pkg/ui/workspaces/db-console/src/js:crdb-protobuf-client",
		"//pkg/ui/workspaces/cluster-ui:ts_project",
	}
	logCommand("bazel", args...)
	if err := d.exec.CommandContextInheritingStdStreams(ctx, "bazel", args...); err != nil {
		return fmt.Errorf("building JS development prerequisites: %w", err)
	}

	bazelBin, err := d.getBazelBin(ctx)
	if err != nil {
		return err
	}
	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	eslintPluginDist := "./pkg/ui/workspaces/eslint-plugin-crdb/dist"
	// Delete eslint-plugin output tree that was previously copied out of the
	// sandbox.
	if err := d.os.RemoveAll(filepath.Join(workspace, eslintPluginDist)); err != nil {
		return err
	}

	// Copy the eslint-plugin output tree back out of the sandbox, since eslint
	// plugins in editors default to only searching in ./node_modules for plugins.
	return d.os.CopyAll(
		filepath.Join(bazelBin, eslintPluginDist),
		filepath.Join(workspace, eslintPluginDist),
	)
}
