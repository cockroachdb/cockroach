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

// noLockFlag disables OpenTofu's built-in state locking. We rely on the task
// system's concurrency keys (enforced by a unique index on running tasks) to
// guarantee at-most-one execution per provisioning, making the backend lock
// redundant. Disabling it avoids orphaned locks when Cloud Run kills a worker
// mid-apply (SIGKILL after 10 s), which would otherwise block all subsequent
// runs until a manual force-unlock.
//
// Only init, plan, apply, and destroy support -lock; output and show do not.
const noLockFlag = "-lock=false"

// IExecutor defines the interface for executing OpenTofu commands.
// Each method operates on a working directory that must contain the template
// HCL files and a backend.tf configuration.
type IExecutor interface {
	// Init runs tofu init in the given working directory.
	Init(ctx context.Context, l *logger.Logger, workingDir string, envVars map[string]string) error

	// Plan runs tofu plan and returns whether changes are detected along with
	// the structured plan JSON output (from tofu show -json).
	// vars are passed as -var flags (auto-injected variables only).
	// User-provided and environment variables should be in envVars as
	// TF_VAR_* entries.
	Plan(
		ctx context.Context, l *logger.Logger, workingDir string,
		vars map[string]string, envVars map[string]string,
	) (hasChanges bool, err error)

	// WritePlanJSON streams the saved plan file as structured JSON to dst.
	WritePlanJSON(
		ctx context.Context, l *logger.Logger, workingDir string,
		envVars map[string]string, dst io.Writer,
	) error

	// Apply applies infrastructure changes. If a saved plan file exists in
	// the working directory (from a prior Plan call), it is used directly.
	// Otherwise, vars (auto-injected) are passed as -var flags.
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
	args := []string{"init", "-no-color", noLockFlag}
	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars, true)
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
// vars are converted to -var key=value arguments (auto-injected variables
// only). User-provided and environment variables should be passed via envVars
// as TF_VAR_* entries.
func (e *Executor) Plan(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	vars map[string]string,
	envVars map[string]string,
) (bool, error) {
	// Step 1: Run tofu plan to detect changes and save the plan file.
	args := []string{"plan", "-out=" + planFileName, "-no-color", "-detailed-exitcode", noLockFlag}
	args = appendVarArgs(args, vars)

	_, stderr, exitCode, err := e.run(ctx, l, workingDir, args, envVars, true)

	var hasChanges bool
	switch exitCode {
	case 0:
		hasChanges = false
	case 2:
		hasChanges = true
	default:
		return false, errors.Wrapf(err, "tofu plan: %s", strings.TrimSpace(stderr))
	}

	return hasChanges, nil
}

// WritePlanJSON streams the saved plan file as structured JSON to dst.
func (e *Executor) WritePlanJSON(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	envVars map[string]string,
	dst io.Writer,
) error {
	args := []string{"show", "-json", planFileName, "-no-color"}
	cmd := e.buildCommand(ctx, workingDir, args, envVars)

	stderrTail := newTailBuffer(64 << 10)
	stderrLW := l.NewLineWriter(slog.LevelWarn, slog.String("source", "tofu"), slog.String("stream", "stderr"))
	defer func() {
		err := stderrLW.Close()
		if err != nil {
			l.Error("failed to close stderr line writer", slog.Any("error", err))
		}
	}()

	cmd.Stdout = dst
	cmd.Stderr = io.MultiWriter(stderrTail, stderrLW)

	l.Info("executing tofu command",
		slog.String("command", e.binaryPath+" "+strings.Join(args, " ")),
	)

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "tofu show: %s", strings.TrimSpace(stderrTail.String()))
	}
	return nil
}

// Apply applies infrastructure changes. If a saved plan file (plan.tfplan)
// exists in the working directory from a prior Plan call, it is used directly
// and vars are ignored (they are baked into the plan). If no saved plan
// exists, vars (auto-injected) are passed as -var flags with -auto-approve.
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
		args = []string{"apply", "-no-color", noLockFlag, planFileName}
	} else {
		// No saved plan — apply with vars directly.
		args = []string{"apply", "-no-color", "-auto-approve", noLockFlag}
		args = appendVarArgs(args, vars)
	}

	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars, true)
	if err != nil {
		return errors.Wrapf(err, "tofu apply: %s", strings.TrimSpace(stderr))
	}
	return nil
}

// Destroy tears down all infrastructure managed in the working directory.
// vars (auto-injected) are passed as -var key=value arguments to match the
// variables used during apply.
func (e *Executor) Destroy(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	vars map[string]string,
	envVars map[string]string,
) error {
	args := []string{"destroy", "-no-color", "-auto-approve", noLockFlag}
	args = appendVarArgs(args, vars)

	_, stderr, _, err := e.run(ctx, l, workingDir, args, envVars, true)
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
	stdout, stderr, _, err := e.run(ctx, l, workingDir, args, envVars, false)
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

type tailBuffer struct {
	data  []byte
	limit int
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit}
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	if len(p) >= b.limit {
		b.data = append(b.data[:0], p[len(p)-b.limit:]...)
		return len(p), nil
	}
	total := len(b.data) + len(p)
	if total > b.limit {
		drop := total - b.limit
		b.data = append(b.data[:0], b.data[drop:]...)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *tailBuffer) String() string {
	return string(b.data)
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
//
// When logStdout is false, stdout is captured in the buffer but not logged
// through the sink. This is useful for commands like "tofu show -json" and
// "tofu output -json" whose structured output is already captured and
// available via API, and would otherwise dump large unformatted JSON into
// task logs.
func (e *Executor) run(
	ctx context.Context,
	l *logger.Logger,
	workingDir string,
	args []string,
	envVars map[string]string,
	logStdout bool,
) (stdout, stderr string, exitCode int, err error) {
	cmd := e.buildCommand(ctx, workingDir, args, envVars)

	var stdoutBuf, stderrBuf bytes.Buffer
	stderrLW := l.NewLineWriter(slog.LevelWarn, slog.String("source", "tofu"), slog.String("stream", "stderr"))
	defer func() {
		err := stderrLW.Close()
		if err != nil {
			l.Error("failed to close stderr line writer", slog.Any("error", err))
		}
	}()

	if logStdout {
		// Stream stdout directly to the logger without buffering. Callers
		// that pass logStdout=true (Init, Apply, Destroy) never use the
		// returned stdout string, so buffering it wastes memory — especially
		// for long-running apply/destroy on large templates.
		stdoutLW := l.NewLineWriter(slog.LevelInfo, slog.String("source", "tofu"), slog.String("stream", "stdout"))
		defer func() {
			err := stdoutLW.Close()
			if err != nil {
				l.Error("failed to close stdout line writer", slog.Any("error", err))
			}
		}()
		cmd.Stdout = stdoutLW
	} else {
		cmd.Stdout = &stdoutBuf
	}
	cmd.Stderr = io.MultiWriter(&stderrBuf, stderrLW)

	// Log the command being executed. Environment variables are omitted
	// because they may contain credentials.
	l.Info("executing tofu command",
		slog.String("command", e.binaryPath+" "+strings.Join(args, " ")),
	)

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
