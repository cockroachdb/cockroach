// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tofu

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// planFileName is the file name used to save the tofu plan output.
const planFileName = "plan.tfplan"

// IExecutor defines the interface for executing OpenTofu commands.
// Each method operates on a working directory that must contain the template
// HCL files and a backend.tf configuration.
type IExecutor interface {
	// Init runs tofu init in the given working directory.
	Init(ctx context.Context, l *logger.Logger, workingDir string, envVars map[string]string) error

	// Plan runs tofu plan and returns whether changes are detected along with
	// the structured plan JSON output (from tofu show -json).
	// vars are passed as -var flags; secret vars should be in envVars as
	// TF_VAR_ instead of in vars.
	Plan(
		ctx context.Context, l *logger.Logger, workingDir string,
		vars map[string]string, envVars map[string]string,
	) (hasChanges bool, planJSON json.RawMessage, err error)

	// Apply applies infrastructure changes. If a saved plan file exists in
	// the working directory (from a prior Plan call), it is used directly.
	// Otherwise, vars are passed as -var flags.
	Apply(
		ctx context.Context, l *logger.Logger, workingDir string,
		vars map[string]string, envVars map[string]string,
	) error

	// Destroy tears down all infrastructure managed in the working directory.
	Destroy(
		ctx context.Context, l *logger.Logger, workingDir string,
		vars map[string]string, envVars map[string]string,
	) error

	// Output reads terraform outputs and returns them as a map of output
	// names to their values.
	Output(
		ctx context.Context, l *logger.Logger, workingDir string,
		envVars map[string]string,
	) (map[string]interface{}, error)
}

// Executor wraps the OpenTofu (tofu) binary for executing terraform operations.
// The tofu binary must be pre-installed and available at the configured path.
type Executor struct {
	binaryPath string
}

// Verify that Executor implements IExecutor.
var _ IExecutor = (*Executor)(nil)

// NewExecutor creates a new Executor with the given binary path.
// If binaryPath is empty, it defaults to "tofu" (looked up on $PATH).
func NewExecutor(binaryPath string) *Executor {
	if binaryPath == "" {
		binaryPath = "tofu"
	}
	return &Executor{binaryPath: binaryPath}
}

// Init runs tofu init in the given working directory. Init is safe to re-run
// (idempotent). envVars should include cloud provider credentials needed by
// the backend (e.g., GOOGLE_CREDENTIALS for GCS).
func (e *Executor) Init(
	ctx context.Context, l *logger.Logger, workingDir string, envVars map[string]string,
) error {
	args := []string{"init", "-no-color"}
	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars)
	if err != nil {
		return errors.Wrapf(err, "tofu init: %s", strings.TrimSpace(stderr))
	}
	return nil
}

// Plan runs tofu plan with -detailed-exitcode to determine whether changes
// are needed, then runs tofu show -json to capture the structured plan output.
//
// Exit code handling for tofu plan -detailed-exitcode:
//   - 0: no changes
//   - 1: error
//   - 2: changes detected (not an error)
//
// vars are converted to -var key=value arguments. Secret variables should be
// passed via envVars as TF_VAR_* entries instead to avoid appearing in
// process listings.
func (e *Executor) Plan(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	vars map[string]string,
	envVars map[string]string,
) (bool, json.RawMessage, error) {
	// Step 1: Run tofu plan to detect changes and save the plan file.
	args := []string{"plan", "-out=" + planFileName, "-no-color", "-detailed-exitcode"}
	args = appendVarArgs(args, vars)

	_, stderr, exitCode, err := e.run(ctx, l, workingDir, args, envVars)

	var hasChanges bool
	switch exitCode {
	case 0:
		hasChanges = false
	case 2:
		hasChanges = true
	default:
		return false, nil, errors.Wrapf(err, "tofu plan: %s", strings.TrimSpace(stderr))
	}

	// Step 2: Run tofu show -json to get the structured plan output.
	showArgs := []string{"show", "-json", planFileName, "-no-color"}
	stdout, stderr, _, showErr := e.run(ctx, l, workingDir, showArgs, envVars)
	if showErr != nil {
		return false, nil, errors.Wrapf(showErr, "tofu show: %s", strings.TrimSpace(stderr))
	}

	return hasChanges, json.RawMessage(stdout), nil
}

// Apply applies infrastructure changes. If a saved plan file (plan.tfplan)
// exists in the working directory from a prior Plan call, it is used directly
// and vars are ignored (they are baked into the plan). If no saved plan
// exists, vars are passed as -var flags with -auto-approve.
func (e *Executor) Apply(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	vars map[string]string,
	envVars map[string]string,
) error {
	planPath := filepath.Join(workingDir, planFileName)

	var args []string
	if _, statErr := os.Stat(planPath); statErr == nil {
		// Saved plan exists — apply it directly. No -auto-approve needed
		// because applying a saved plan is non-interactive.
		args = []string{"apply", "-no-color", planFileName}
	} else {
		// No saved plan — apply with vars directly.
		args = []string{"apply", "-no-color", "-auto-approve"}
		args = appendVarArgs(args, vars)
	}

	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars)
	if err != nil {
		return errors.Wrapf(err, "tofu apply: %s", strings.TrimSpace(stderr))
	}
	return nil
}

// Destroy tears down all infrastructure managed in the working directory.
// vars are passed as -var key=value arguments to match the variables used
// during apply.
func (e *Executor) Destroy(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	vars map[string]string,
	envVars map[string]string,
) error {
	args := []string{"destroy", "-no-color", "-auto-approve"}
	args = appendVarArgs(args, vars)

	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars)
	if err != nil {
		return errors.Wrapf(err, "tofu destroy: %s", strings.TrimSpace(stderr))
	}
	return nil
}

// outputEntry represents a single terraform output value as returned by
// tofu output -json.
type outputEntry struct {
	Value     interface{} `json:"value"`
	Type      interface{} `json:"type"`
	Sensitive bool        `json:"sensitive"`
}

// Output reads terraform outputs from the state and returns them as a map of
// output names to their values. Only the value field of each output is
// returned; type and sensitivity metadata are discarded.
func (e *Executor) Output(
	ctx context.Context, l *logger.Logger, workingDir string, envVars map[string]string,
) (map[string]interface{}, error) {
	args := []string{"output", "-json", "-no-color"}
	stdout, stderr, _, err := e.run(ctx, l, workingDir, args, envVars)
	if err != nil {
		return nil, errors.Wrapf(err, "tofu output: %s", strings.TrimSpace(stderr))
	}

	var outputs map[string]outputEntry
	if err := json.Unmarshal([]byte(stdout), &outputs); err != nil {
		return nil, errors.Wrap(err, "parse tofu output JSON")
	}

	result := make(map[string]interface{}, len(outputs))
	for name, entry := range outputs {
		result[name] = entry.Value
	}
	return result, nil
}

// buildCommand constructs an exec.Cmd for the tofu binary with the given
// arguments. The command's working directory is set to workingDir, and its
// environment is built from the current process environment with envVars
// overlaid on top.
func (e *Executor) buildCommand(
	ctx context.Context, workingDir string, args []string, envVars map[string]string,
) *exec.Cmd {
	cmd := exec.CommandContext(ctx, e.binaryPath, args...)
	cmd.Dir = workingDir

	// Run tofu in its own process group so that on context cancellation we
	// can signal the entire group (tofu + provider plugin children).
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Send SIGTERM to the process group first, giving tofu a chance to
	// shut down providers gracefully. If it doesn't exit within WaitDelay,
	// Go escalates to SIGKILL automatically.
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = 10 * time.Second

	// Start from the current process environment so PATH, HOME, etc. are
	// available to the tofu binary and its providers.
	cmd.Env = os.Environ()
	for k, v := range envVars {
		cmd.Env = append(cmd.Env, k+"="+v)
	}

	return cmd
}

// run executes a tofu command and returns its stdout, stderr, exit code,
// and any error. The exit code is extracted from *exec.ExitError; if the
// error is not an ExitError (e.g., binary not found), exitCode is -1.
func (e *Executor) run(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	args []string,
	envVars map[string]string,
) (stdout, stderr string, exitCode int, err error) {
	cmd := e.buildCommand(ctx, workingDir, args, envVars)

	var stdoutBuf, stderrBuf bytes.Buffer
	stdoutLW := l.NewLineWriter(slog.LevelInfo, slog.String("source", "tofu"), slog.String("stream", "stdout"))
	stderrLW := l.NewLineWriter(slog.LevelWarn, slog.String("source", "tofu"), slog.String("stream", "stderr"))
	defer func() {
		err := stdoutLW.Close()
		if err != nil {
			l.Error("failed to close stdout line writer", slog.Any("error", err))
		}
	}()
	defer func() {
		err := stderrLW.Close()
		if err != nil {
			l.Error("failed to close stderr line writer", slog.Any("error", err))
		}
	}()
	cmd.Stdout = io.MultiWriter(&stdoutBuf, stdoutLW)
	cmd.Stderr = io.MultiWriter(&stderrBuf, stderrLW)

	// Log the subcommand being executed but not the full arguments or env
	// vars, which may contain credentials.
	if len(args) > 0 {
		l.Info("executing tofu command", slog.String("subcommand", args[0]))
	}

	err = cmd.Run()
	stdout = stdoutBuf.String()
	stderr = stderrBuf.String()

	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
		return stdout, stderr, exitCode, err
	}

	return stdout, stderr, 0, nil
}

// appendVarArgs appends -var key=value arguments to args for each entry in
// vars. Keys are sorted for deterministic command lines.
func appendVarArgs(args []string, vars map[string]string) []string {
	if len(vars) == 0 {
		return args
	}

	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		args = append(args, "-var", k+"="+vars[k])
	}
	return args
}
