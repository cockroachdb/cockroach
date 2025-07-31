// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/io/exec"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/spf13/cobra"
)

const (
	doctorStatusFile = "bin/.dev-status"

	// doctorStatusVersion is the current "version" of the status checks
	// performed by `dev doctor``. Increasing it will force doctor to be re-run
	// before other dev commands can be run.
	doctorStatusVersion = 10

	noCacheFlag     = "no-cache"
	interactiveFlag = "interactive"
	autofixFlag     = "autofix"
	remoteFlag      = "remote"
)

// doctorCheck represents a single check that doctor performs, along with
// an associated autofix function that attempts to automatically correct the
// problem.
type doctorCheck struct {
	// A short string used for identifying this check in debug logging.
	name string
	// check returns an empty string if the check passed, or some other
	// string with the failure details if the check failed. This function
	// MAY NOT collect any user input. This function MAY attempt to autofix
	// the problem only if the fix is "trivial" or "safe".
	check func(d *dev, ctx context.Context, cfg doctorConfig) string
	// if autofix is non-nil and the check did not pass, the function will
	// attempt to automatically fix the problem. If a nil value is returned,
	// the problem was automatically fixed. This function MAY solicit input
	// from the user in interactive mode. This function MAY try to
	// automatically solve the problem in autofix mode.
	// It is assumed that if this function returns nil, then the automatic
	// fix works.
	autofix func(d *dev, ctx context.Context, cfg doctorConfig) error
	// true iff all subsequent checks should be skipped if this check fails.
	shortCircuitOnFailure bool
	// true iff this check should be skipped if there were any previous
	// failures.
	requirePreviousSuccesses bool
	// true iff this check should only be run in remote mode.
	remoteOnly bool
	// true iff this check should only be run in NON-remote mode.
	nonRemoteOnly bool
}

// doctorConfig captures configuration information including command-line
// parameters.
type doctorConfig struct {
	workspace, bazelBin string
	// --interactive and --autofix respectively.
	interactive, haveAutofixPermission bool
	// false only if --no-cache or DEV_NO_REMOTE_CACHE is set.
	startCache bool
	// true iff we are running checks in "remote" mode. Certain checks will
	// be skipped in either remote or non-remote mode.
	remote bool
}

// maybePromptForAutofixPermission prompts the user for autofix permission if it
// has not already been provided and we are running in interactive mode. It will
// prompt with the given question with a default ansewr of 'y', call toBoolFuzzy
// with the result, and if permission is given, will set haveAutofixPermission
// on the doctorConfig object before returning.
func (cfg *doctorConfig) maybePromptForAutofixPermission(question string) {
	if !cfg.haveAutofixPermission && cfg.interactive {
		response := promptInteractiveInput(question, "y")
		canAutofix, ok := toBoolFuzzy(response)
		if ok && canAutofix {
			cfg.haveAutofixPermission = true
		}
	}
}

type buildConfig struct {
	name        string
	description string
}

// buildConfigs are the available build configs (typically "dev" and
// "crosslinux"). The first config will always be "dev".
var buildConfigs []buildConfig = func() []buildConfig {
	configs := []buildConfig{
		{name: "dev", description: "uses the host toolchain"},
	}
	if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
		configs = append(configs, buildConfig{
			name:        "crosslinux",
			description: "uses the cross-compiler that we use in CI",
		})
	}

	if runtime.GOOS == "linux" && runtime.GOARCH == "arm64" {
		configs = append(configs, buildConfig{
			name:        "crosslinuxarm",
			description: "uses the cross-compiler that we use in CI",
		})
	}

	return configs
}()

// The list of all checks performed by `dev doctor`.
var allDoctorChecks = []doctorCheck{
	{
		name: "remote_linuxonly",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if runtime.GOOS == "linux" && runtime.GOARCH == "amd64" {
				return ""
			}
			return "--remote mode only available on Linux/amd64"
		},
		remoteOnly:            true,
		shortCircuitOnFailure: true,
	},
	{
		name: "xcode",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if runtime.GOOS != "darwin" {
				return ""
			}
			stdout, err := d.exec.CommandContextSilent(ctx, "/usr/bin/xcodebuild", "-version")
			if err != nil {
				log.Println("Failed to run `/usr/bin/xcodebuild -version`.")
				stdoutStr := strings.TrimSpace(string(stdout))
				printStdoutAndErr(stdoutStr, err)
				return `You must have a full installation of XCode to build with Bazel.
A command-line tools instance does not suffice.
Please perform the following steps:
  1. Install XCode from the App Store.
  2. Launch Xcode.app at least once to perform one-time initialization of developer tools.
  3. Run ` + "`xcode-select -switch /Applications/Xcode.app/`."
			}
			return ""
		},
	},
	{
		name: "submodules",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if _, err := os.Stat("bin/.submodules-initialized"); err == nil {
				return ""
			}
			if output, err := d.exec.CommandContextSilent(ctx, "git", "submodule", "update", "--init", "--recursive"); err != nil {
				return fmt.Sprintf("failed to run `git submodule update --init --recursive`: %+v: got output %s", err, string(output))
			}
			if err := d.os.MkdirAll("bin"); err != nil {
				return err.Error()
			}
			if err := d.os.WriteFile("bin/.submodules-initialized", ""); err != nil {
				return err.Error()
			}
			return ""
		},
	},
	{
		name: "githooks",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if _, err := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--is-inside-work-tree"); err != nil {
				return err.Error()
			}
			stdout, err := d.exec.CommandContextSilent(ctx, "git", "rev-parse", "--git-path", "hooks")
			if err != nil {
				return err.Error()
			}
			gitHooksDir := strings.TrimSpace(string(stdout))
			if err := d.os.RemoveAll(gitHooksDir); err != nil {
				return err.Error()
			}
			if err := d.os.MkdirAll(gitHooksDir); err != nil {
				return err.Error()
			}
			hooks, err := d.os.ListFilesWithSuffix("githooks", "")
			if err != nil {
				return err.Error()
			}
			for _, hook := range hooks {
				if err := d.os.Symlink(path.Join(cfg.workspace, hook), path.Join(gitHooksDir, path.Base(hook))); err != nil {
					return err.Error()
				}
			}
			return ""
		},
	},
	{
		name: "devconfig_local",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			for _, bldCfg := range buildConfigs {
				if d.checkUsingConfig(cfg.workspace, bldCfg.name) {
					// Already configured.
					return ""
				}
			}
			ret := fmt.Sprintf(`
Make sure one of the following lines is in the file %s/.bazelrc.user:
`, cfg.workspace)
			for i, bldCfg := range buildConfigs {
				if i > 0 {
					ret = ret + "\n             OR       \n"
				}
				ret = fmt.Sprintf("%s    build --config=%s  # %s", ret, bldCfg.name, bldCfg.description)
			}
			return ret
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			if len(buildConfigs) > 1 {
				if !cfg.interactive {
					return fmt.Errorf("must be running in --interactive mode to autofix")
				}
				log.Println("DOCTOR >> I can configure your .bazelrc.user to build in one of the following configurations:")
				var names []string
				for _, c := range buildConfigs {
					names = append(names, c.name)
					log.Printf("           - %s: %s\n", c.name, c.description)
				}
				question := fmt.Sprintf("Which config you want to use (%s)?", strings.Join(names, ","))
				response := promptInteractiveInput(question, names[0])
				if !slices.Contains(names, response) {
					return fmt.Errorf("unrecognized configuration option %s", response)
				}
				return d.addLineToBazelRcUser(cfg.workspace, fmt.Sprintf("build --config=%s", response))
			}
			cfg.maybePromptForAutofixPermission("Do you want me to add `build --config=dev` to your .bazelrc.user file for you?")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.addLineToBazelRcUser(cfg.workspace, "build --config=dev")
		},
		nonRemoteOnly: true,
	},
	{
		name: "engflowconfig_local",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if d.checkUsingConfig(cfg.workspace, "engflow") {
				return "Cannot use the engflow build configuration in local mode"
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to remove the engflow configuration from your .bazelrc.user file for you?")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.removeAllInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "build --config=engflow")
		},
		nonRemoteOnly: true,
	},
	{
		name: "configs_remote",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if !d.checkUsingConfig(cfg.workspace, "engflow") {
				return fmt.Sprintf("Make sure the following line is in %s/.bazelrc.user: build --config=engflow", cfg.workspace)
			}
			if !d.checkUsingConfig(cfg.workspace, "crosslinux") {
				return fmt.Sprintf("Make sure the following line is in %s/.bazelrc.user: build --config=crosslinux", cfg.workspace)
			}
			if d.checkUsingConfig(cfg.workspace, "dev") {
				return "In --remote mode, you cannot use the `dev` build configuration."
			}
			if !d.checkLinePresenceInBazelRcUser(cfg.workspace, "build:engflow --jobs=200") {
				return fmt.Sprintf("Make sure the following line is in %s/.bazelrc.user: build:engflow --jobs=200", cfg.workspace)
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will set the crosslinux and engflow configs, specify the number of jobs to use, and remove any usage of the dev config if you have any.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			if !d.checkUsingConfig(cfg.workspace, "engflow") {
				err := d.addLineToBazelRcUser(cfg.workspace, "build --config=engflow")
				if err != nil {
					return err
				}
			}
			if !d.checkUsingConfig(cfg.workspace, "crosslinux") {
				err := d.addLineToBazelRcUser(cfg.workspace, "build --config=crosslinux")
				if err != nil {
					return err
				}
			}
			if d.checkUsingConfig(cfg.workspace, "dev") {
				err := d.removeAllInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "build --config=dev")
				if err != nil {
					return err
				}
			}
			if !d.checkLinePresenceInBazelRcUser(cfg.workspace, "build:engflow --jobs=200") {
				err := d.addLineToBazelRcUser(cfg.workspace, "build:engflow --jobs=200")
				if err != nil {
					return err
				}
			}

			return nil
		},
		remoteOnly: true,
	},
	{
		name: "nogo_configured",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			configured := d.checkUsingConfig(cfg.workspace, "lintonbuild") ||
				d.checkUsingConfig(cfg.workspace, "nolintonbuild")
			if !configured {
				return "Failed to find `--config=lintonbuild` or `--config=nolintonbuild` in .bazelrc.user." + `

Put EXACTLY ONE of the following lines in your .bazelrc.user:
    build --config=lintonbuild
        OR
    build --config=nolintonbuild
The former will run lint checks while you build. This will make incremental builds
slightly slower and introduce a noticeable delay in first-time build setup.`
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			if !cfg.interactive {
				return fmt.Errorf("must be running in --interactive mode to autofix")
			}
			var alreadyHaveSuggestion bool
			for _, str := range []string{"lintonbuild", "nolintonbuild"} {
				alreadyHaveSuggestion = alreadyHaveSuggestion || d.checkUsingConfig(cfg.workspace, str)
			}
			if alreadyHaveSuggestion {
				return fmt.Errorf("your .bazelrc.user looks okay already :/")
			}
			log.Println("DOCTOR >> I can configure your .bazelrc.user to build either in the `lintonbuild` configuration or the `nolintonbuild` configuration.")
			log.Println("DOCTOR >> The former will run lint checks while you build.")
			log.Println("DOCTOR >> Doing so will make incremental rebuilds slightly slower, and will introduce a noticeable delay the first time you build as we build all the linters.")
			response := promptInteractiveInput("Do you want to use the `lintonbuild` configuration?", "y")
			lintOnBuild, ok := toBoolFuzzy(response)
			if !ok {
				return fmt.Errorf("could not understand response %s", response)
			}
			config := "nolintonbuild"
			if lintOnBuild {
				config = "lintonbuild"
			}
			return d.addLineToBazelRcUser(cfg.workspace, fmt.Sprintf("build --config=%s", config))
		},
	},
	{
		name: "engflow_certificates",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if !d.checkLinePresenceInBazelRcUser(cfg.workspace, "build:engflow --tls_client_certificate=") {
				return fmt.Sprintf("Must specify the --tls_client_certificate to use for EngFlow builds in %s/.bazelrc.user. This is a line of the form: `build:engflow --tls_client_certificate=/path/to/file`.", cfg.workspace)
			}
			if !d.checkLinePresenceInBazelRcUser(cfg.workspace, "build:engflow --tls_client_key=") {
				return fmt.Sprintf("Must specify the --tls_client_key to use for EngFlow builds in %s/.bazelrc.user. This is a line of the form: `build:engflow --tls_client_key=/path/to/file`.", cfg.workspace)
			}
			return ""
		},
		remoteOnly: true,
	},
	{
		name: "tmpdir_local",
		check: func(d *dev, _ context.Context, cfg doctorConfig) string {
			present := d.checkLinePresenceInBazelRcUser(cfg.workspace, "test --test_tmpdir=")
			if !present {
				return "You haven't configured a tmpdir for your tests.\n" +
					"Please add a `test --test_tmpdir=/PATH/TO/TMPDIR` line to your .bazelrc.user:\n" +
					"    echo \"test --test_tmpdir=/tmp/cockroach\" >> .bazelrc.user\n" +
					"(You can choose any directory as a tmpdir.)"
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			var tmpdir string
			if cfg.haveAutofixPermission {
				tmpdir = "/tmp/cockroach"
			} else if !cfg.interactive {
				return fmt.Errorf("do not have permission to configure tmpdir")
			}
			if tmpdir == "" {
				tmpdir = promptInteractiveInput("Please set a tmpdir that will be used for storing test artifacts", "/tmp/cockroach")
			}
			if tmpdir == "" {
				return fmt.Errorf("must select a tmpdir")
			}
			return d.addLineToBazelRcUser(cfg.workspace, fmt.Sprintf("test --test_tmpdir=%s", tmpdir))
		},
		nonRemoteOnly: true,
	},
	{
		name: "tmpdir_remote",
		check: func(d *dev, _ context.Context, cfg doctorConfig) string {
			present := d.checkLinePresenceInBazelRcUser(cfg.workspace, "test --test_tmpdir=")
			if present {
				return "Should not set --test_tmpdir for building in remote mode."
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will remove any `test --test_tmpdir=` line from the file.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.removeAllPrefixesInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "test --test_tmpdir=")
		},
		remoteOnly: true,
	},
	{
		name: "sandbox_add_mount_pair_local",
		check: func(d *dev, _ context.Context, cfg doctorConfig) string {
			// This check only matters for Linux machines.
			if runtime.GOOS != "linux" {
				return ""
			}
			if !d.checkLinePresenceInBazelRcUser(cfg.workspace, "test --test_tmpdir=/tmp") {
				return ""
			}
			if d.checkLinePresenceInBazelRcUser(cfg.workspace, "test --sandbox_add_mount_pair=/tmp") {
				return ""
			}
			return "Should set --sandbox_add_mount_pair=/tmp given the use of --test_tmpdir=/tmp"
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will add a line `test --sandbox_add_mount_pair=/tmp`.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.addLineToBazelRcUser(cfg.workspace, "test --sandbox_add_mount_pair=/tmp")
		},
		nonRemoteOnly: true,
	},
	{
		name: "sandbox_add_mount_pair_remote",
		check: func(d *dev, _ context.Context, cfg doctorConfig) string {
			if d.checkLinePresenceInBazelRcUser(cfg.workspace, "test --sandbox_add_mount_pair=/tmp") {
				return "Should not set --sandbox_add_mount_pair in remote mode"
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will remove all --sandbox_add_mount_pair from your .bazelrc.user")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.removeAllPrefixesInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "test --sandbox_add_mount_pair")
		},
		remoteOnly: true,
	},
	{
		name: "patchelf",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if d.checkUsingConfig(cfg.workspace, "crosslinux") {
				_, err := d.exec.LookPath("patchelf")
				if err != nil {
					return "patchelf not found on PATH. patchelf is required when using crosslinux config"
				}
				return ""
			}
			return ""
		},
		// TODO: consider adding an autofix for this.
	},
	{
		name: "cache_local",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if !cfg.startCache {
				return ""
			}
			d.log.Println("DOCTOR >> setting up cache")
			bazelRcLine, err := d.setUpCache(ctx)
			if err != nil {
				return err.Error()
			}
			found := d.checkPresenceInBazelRc(bazelRcLine)
			if found {
				return fmt.Sprintf("Found line `%s` in ~/.bazelrc; should instead be in .bazelrc.user", bazelRcLine)
			}
			if d.checkLinePresenceInBazelRcUser(cfg.workspace, bazelRcLine) {
				return ""
			}
			return fmt.Sprintf("Please add the string `%s` to your .bazelrc.user", bazelRcLine)
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user for you to configure the loopback cache? I will also update ~/.bazelrc if necessary.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to configure the cache")
			}
			bazelRcLine, err := d.setUpCache(ctx)
			if err != nil {
				return err
			}
			found := d.checkPresenceInBazelRc(bazelRcLine)
			if found {
				homeDir, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				bazelrcUserFile := filepath.Join(homeDir, ".bazelrc")
				err = d.removeAllInFile(bazelrcUserFile, bazelRcLine)
				if err != nil {
					return err
				}
			}
			if d.checkLinePresenceInBazelRcUser(cfg.workspace, bazelRcLine) {
				return nil
			}
			return d.addLineToBazelRcUser(cfg.workspace, bazelRcLine)
		},
		requirePreviousSuccesses: true,
		nonRemoteOnly:            true,
	},
	{
		name: "cache_remote",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			if d.checkLinePresenceInBazelRcUser(cfg.workspace, "build --remote_cache=") {
				return "Should not set a --remote_cache if using remote builds"
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will remove any `build --remote_cache=` line from the file.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			return d.removeAllPrefixesInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "build --remote_cache=")
		},
		remoteOnly: true,
	},
	{
		name: "lintonbuild_remote",
		check: func(d *dev, ctx context.Context, cfg doctorConfig) string {
			var found bool
			for _, str := range []string{"lintonbuild", "nolintonbuild"} {
				found = found || d.checkUsingConfig(cfg.workspace, str)
			}
			if found {
				return "Should not have lintonbuild or nolintonbuild set for remote builds"
			}
			return ""
		},
		autofix: func(d *dev, ctx context.Context, cfg doctorConfig) error {
			cfg.maybePromptForAutofixPermission("Do you want me to update your .bazelrc.user file for you? I will remove any `build --config=lintonbuild` or `build --config=nolintonbuild` line from the file.")
			if !cfg.haveAutofixPermission {
				return fmt.Errorf("do not have permission to update .bazelrc.user")
			}
			err := d.removeAllInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "build --config=lintonbuild")
			if err != nil {
				return err
			}
			return d.removeAllInFile(filepath.Join(cfg.workspace, ".bazelrc.user"), "build --config=nolintonbuild")
		},
		remoteOnly: true,
	},
}

func promptInteractiveInput(question string, defaultResponse string) string {
	fmt.Print("DOCTOR >> ", question)
	if defaultResponse != "" {
		fmt.Print(" [", defaultResponse, "]")
	}
	fmt.Print(" > ")
	responseChar := make([]byte, 1)
	var responseChars []byte
	for {
		// We specifically don't want to buffer stdin.
		_, err := os.Stdin.Read(responseChar)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if responseChar[0] == '\n' {
			break
		}
		responseChars = append(responseChars, responseChar[0])
	}
	result := strings.TrimSpace(string(responseChars))
	if result == "" {
		return defaultResponse
	}
	return result
}

// toBoolFuzzy converst a string like
func toBoolFuzzy(response string) (result, ok bool) {
	lowered := strings.TrimSpace(strings.ToLower(response))
	if lowered == "yes" || lowered == "y" {
		return true, true
	}
	if lowered == "no" || lowered == "n" {
		return false, true
	}
	return false, false
}

// getDoctorStatus returns the current doctor status number. This function only
// returns an error in exceptional situations -- if the status file does not
// already exist (as would be the case for a clean checkout), this function
// simply returns 0, nil.
func (d *dev) getDoctorStatus(ctx context.Context) (int, error) {
	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return -1, err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)
	content, err := os.ReadFile(statusFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			content = []byte("0")
		} else {
			return -1, err
		}
	}
	return strconv.Atoi(strings.TrimSpace(string(content)))
}

// checkDoctorStatus returns an error iff the current doctor status is not the
// latest.
func (d *dev) checkDoctorStatus(ctx context.Context) error {
	if buildutil.CrdbTestBuild {
		return nil
	}

	status, err := d.getDoctorStatus(ctx)
	if err != nil {
		return err
	}

	if status < doctorStatusVersion {
		return errors.New("please run `dev doctor` to refresh dev status, then try again")
	}
	return nil
}

func (d *dev) writeDoctorStatus(ctx context.Context) error {
	prevStatus, err := d.getDoctorStatus(ctx)
	if err != nil {
		return err
	}
	if prevStatus <= 0 {
		// In this case recommend the user `bazel clean --expunge`.
		log.Println("DOCTOR >> It is recommended to run `bazel clean --expunge` to avoid any spurious failures now that your machine is set up. (You only have to do this once.)")
	}
	dir, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}
	statusFile := filepath.Join(dir, doctorStatusFile)

	return os.WriteFile(statusFile, []byte(strconv.Itoa(doctorStatusVersion)), 0600)
}

func printStdoutAndErr(stdoutStr string, err error) {
	if len(stdoutStr) > 0 {
		log.Printf("stdout:   %s", stdoutStr)
	}
	var cmderr *exec.ExitError
	if errors.As(err, &cmderr) {
		stderrStr := strings.TrimSpace(string(cmderr.Stderr))
		if len(stderrStr) > 0 {
			log.Printf("stderr:   %s", stderrStr)
		}
	}
}

// makeDoctorCmd constructs the subcommand used to build the specified binaries.
func makeDoctorCmd(runE func(cmd *cobra.Command, args []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "doctor",
		Aliases: []string{"setup"},
		Short:   "Check whether your machine is ready to build",
		Long:    "Check whether your machine is ready to build.",
		Example: "dev doctor",
		Args:    cobra.ExactArgs(0),
		RunE:    runE,
	}
	cmd.Flags().Bool(noCacheFlag, false, "do not set up remote cache as part of doctor")
	cmd.Flags().Bool(interactiveFlag, true, "set up machine in interactive mode")
	cmd.Flags().Bool(autofixFlag, false, "attempt to solve problems automatically if possible")
	cmd.Flags().String(remoteFlag, "auto", "set this machine up for use with remote execution")
	cmd.Flags().Lookup(remoteFlag).NoOptDefVal = "yes"
	return cmd
}

func (d *dev) doctor(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	failures := []string{}
	interactive := mustGetFlagBool(cmd, interactiveFlag)
	autofix := mustGetFlagBool(cmd, autofixFlag)
	noCache := mustGetFlagBool(cmd, noCacheFlag)
	remoteStr := mustGetFlagString(cmd, remoteFlag)
	noCacheEnv := d.os.Getenv("DEV_NO_REMOTE_CACHE")
	if noCacheEnv != "" {
		noCache = true
	}

	workspace, err := d.getWorkspace(ctx)
	if err != nil {
		return err
	}

	var remote bool
	if remoteStr == "yes" {
		remote = true
	} else if remoteStr == "auto" {
		remote = d.checkUsingConfig(workspace, "engflow")
	} else if remoteStr != "no" {
		return fmt.Errorf("unknown value for -remote flag %s; must be `auto`, `no`, or `yes`", remoteStr)
	}

	bazelBin, err := d.getBazelBin(ctx, []string{})
	if err != nil {
		return err
	}

	log.Println("=============================")
	log.Println("=== RUNNING DOCTOR CHECKS ===")
	log.Println("=============================")

	cfg := doctorConfig{
		workspace:             workspace,
		bazelBin:              bazelBin,
		interactive:           interactive,
		haveAutofixPermission: autofix,
		startCache:            !noCache,
		remote:                remote,
	}
	var hasFailures bool
	for _, doctorCheck := range allDoctorChecks {
		if doctorCheck.requirePreviousSuccesses && hasFailures {
			fmt.Printf("NOTE: skipping check %s as there are previous failures\n", doctorCheck.name)
			continue
		}

		if doctorCheck.remoteOnly && !cfg.remote {
			fmt.Printf("NOTE: skipping check %s as we are not running in remote mode, and the check is remote-only\n", doctorCheck.name)
			continue
		}
		if doctorCheck.nonRemoteOnly && cfg.remote {
			fmt.Printf("NOTE: skipping check %s as we are running in remote mode, and the check is non-remote-only\n", doctorCheck.name)
			continue
		}
		d.log.Printf("DOCTOR >> running %s check", doctorCheck.name)
		msg := doctorCheck.check(d, ctx, cfg)
		if msg == "" {
			// Success.
			continue
		}
		if doctorCheck.autofix != nil {
			// Try to autofix.
			err := doctorCheck.autofix(d, ctx, cfg)
			if err == nil {
				// This is a success.
				continue
			}
			failureMsg := fmt.Sprintf("Original error:\n\t%s\n\nI tried to fix the error but encountered the following problem:\n\t%s", msg, err.Error())
			failures = append(failures, failureMsg)
		} else {
			// Failure and no way to autofix means that we are done with this check.
			failures = append(failures, msg)
		}
		if doctorCheck.shortCircuitOnFailure {
			fmt.Println("NOTE: skipping remaining checks due to failure")
			break
		}
	}

	log.Println("======================================")
	log.Println("=== FINISHED RUNNING DOCTOR CHECKS ===")
	log.Println("======================================")

	if len(failures) > 0 {
		log.Printf("DOCTOR >> encountered %d errors", len(failures))
		for _, failure := range failures {
			log.Println(failure)
		}
		return errors.New("please address the errors described above and try again")
	}

	if err := d.writeDoctorStatus(ctx); err != nil {
		return err
	}
	log.Println("You are ready to build :)")
	return nil
}

// checkPresenceInBazelRc checks whether the given line is in ~/.bazelrc.
// If it is, this function returns true. Otherwise, it returns false.
// Errors opening the file (e.g. if it doesn't exist) are ignored.
func (d *dev) checkPresenceInBazelRc(expectedBazelRcLine string) bool {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return false
	}
	bazelRcContents, err := d.os.ReadFile(filepath.Join(homeDir, ".bazelrc"))
	if err != nil {
		return false
	}
	found := false
	for _, line := range strings.Split(bazelRcContents, "\n") {
		if !strings.Contains(line, expectedBazelRcLine) {
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		found = true
	}
	return found
}

// checkLinePresenceInBazelRcUser checks whether the .bazelrc.user file
// contains a line starting with the given prefix. Returns true iff a matching
// line is in the file. Failures to find the file are ignored.
func (d *dev) checkLinePresenceInBazelRcUser(workspace, expectedSubstr string) bool {
	contents, err := d.os.ReadFile(filepath.Join(workspace, ".bazelrc.user"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(contents, "\n") {
		if strings.HasPrefix(line, expectedSubstr) {
			return true
		}
	}
	return false
}

// Given a line of text, add it to the end of the .bazelrc.user file.
func (d *dev) addLineToBazelRcUser(workspace, line string) error {
	bazelrcUserFile := filepath.Join(workspace, ".bazelrc.user")
	contents, err := d.os.ReadFile(bazelrcUserFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if len(contents) > 0 && contents[len(contents)-1] != '\n' {
		contents = contents + "\n"
	}
	if line[len(line)-1] != '\n' {
		line = line + "\n"
	}
	contents = contents + line
	return d.os.WriteFile(bazelrcUserFile, contents)
}

// Given a config, check whether .bazelrc.user is configured to use it.
func (d *dev) checkUsingConfig(workspace, config string) bool {
	for _, delim := range []byte{' ', '='} {
		if d.checkLinePresenceInBazelRcUser(workspace, fmt.Sprintf("build --config%c%s", delim, config)) {
			return true
		}
	}
	return false
}

// Given a filename, remove all instances of the string toRemove from the file.
// If the file does not exist, this file returns no error. If toRemove contains
// an equals sign (=), we will also try replacing the first instance of the
// equals sign with a space ' ' and also remove that string. This is to handle
// .bazelrc configurations that can be of the form `build --foo=bar` or
// `build --foo bar`.
func (d *dev) removeAllInFile(filename, toRemove string) error {
	contents, err := d.os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	lines := strings.Split(contents, "\n")
	otherToRemove := strings.Replace(toRemove, "=", " ", 1)
	var out strings.Builder
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != toRemove && line != otherToRemove {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	outStr := out.String()
	outStr = strings.TrimSpace(outStr) + "\n"
	return d.os.WriteFile(filename, outStr)
}

// Given a filename, remove all lines from the file beginning with the given
// prefix. If the file does not exist, this returns no error. If toRemove
// contains an equals sign (=), we will also try replacing the first instance
// of the equals sign with a space ' ' and also remove that string. This is to
// handle .bazelrc configurations that can be of the form `build --foo=bar` or
// `build --foo bar`.
func (d *dev) removeAllPrefixesInFile(filename, prefixToRemove string) error {
	contents, err := d.os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	lines := strings.Split(contents, "\n")
	otherPrefixToRemove := strings.Replace(prefixToRemove, "=", " ", 1)
	var out strings.Builder
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefixToRemove) &&
			!strings.HasPrefix(line, otherPrefixToRemove) {
			out.WriteString(line)
			out.WriteByte('\n')
		}
	}
	outStr := out.String()
	outStr = strings.TrimSpace(outStr) + "\n"
	return d.os.WriteFile(filename, outStr)
}
