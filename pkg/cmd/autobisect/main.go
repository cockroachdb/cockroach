// Copyright 2023 The Cockroach Authors.
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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/spf13/cobra"
)

type optsT struct {
	cmd           []string // os.Args[1:]
	artifacts     string
	filePredicate string
	maxSteps      int64
	cont          bool
	log           func(string, ...interface{})

	fileSeq int
}

func init() {
	// TODO(tbg): support this once needed.
	//	rootCmd.PersistentFlags().StringVar(&opts.cherryPick, "cherry-pick", "", "Commit to (temporarily) cherry-pick onto each commit being tested")
	rootCmd.PersistentFlags().StringVar(&opts.filePredicate, "only", "", "Only consider commits that changed the provided pattern (all others will be skipped); use like `git log -- <pat>`")
	rootCmd.PersistentFlags().Int64Var(&opts.maxSteps, "max-age", 10000, "Backtrack up to BAD~<max> but throw an error if no bad commit could be found until then")
	rootCmd.PersistentFlags().BoolVar(&opts.cont, "continue", false, "Additional args to `git log` to narrow down the commits of interest (all others will be skipped)")
}

var opts = optsT{
	log: func(format string, args ...interface{}) {
		_, _ = fmt.Fprintf(os.Stdout, "> "+format, args...)
		if n := len(format); n > 0 && format[n-1] != '\n' {
			_, _ = fmt.Fprintln(os.Stdout)
		}
	},
	artifacts: filepath.Join("artifacts/autobisect"),
}

func (opts *optsT) validate() error {
	return nil
}

var rootCmd = &cobra.Command{
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		opts.cmd = args
		err := opts.runBisect()
		if f, l, _, ok := errors.GetOneLineSource(err); ok {
			err = errors.Wrapf(err, "%s:%d", f, l)
		}
		return err
	},
	Example: `autobisect \
  --cherry-pick tmp-logging-patch \
  -- ./dev test --stress pkg/kv/kvserver --filter TestTimeSeriesMaintenanceQueue --stress-args '-maxtime 15m -p 12 -failure FAIL'
`,
}

// execWrap runs a command that is expected to succeed. If it doesn't succeed,
// the error will contain interleaved stdout and stderr. On success, the output
// is returned instead. Should only be used for commands that don't produce a
// ton of output. Produces artifact with the output in either case.
func (opts *optsT) execWrap(arg0 string, args ...string) (_ string, _ error) {
	f, seq, err := opts.nextFile(arg0, args...)
	if err != nil {
		return "", err
	}
	_, err = opts.execInto(f, f, arg0, args...)
	_ = f.Sync()
	_ = f.Close()
	output, _ := os.ReadFile(f.Name())

	if err != nil {
		return "", errors.Wrapf(err,
			"#%05d: %s", seq, output)
	}
	return string(output), nil
}

func (opts *optsT) execInto(
	stdout, stderr io.Writer, arg0 string, args ...string,
) (_exitCode int, _ error) {
	opts.fileSeq++
	args = append([]string{arg0}, args...)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()

	var exitCode int
	if err != nil {
		exitCode = 1
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
	}

	return exitCode, err
}

func (opts *optsT) head() (plumbing.Hash, error) {
	var buf bytes.Buffer
	if _, err := opts.execInto(&buf, io.Discard, `git`, `rev-parse`, `HEAD`); err != nil {
		return plumbing.ZeroHash, err
	}
	return plumbing.NewHash(strings.TrimSpace(buf.String())), nil
}

func (opts *optsT) nextFile(arg0 string, args ...string) (_ *os.File, seq int, _ error) {
	opts.fileSeq++

	sl := []string{fmt.Sprintf("%05d", opts.fileSeq)}
	for _, c := range append([]string{arg0}, args...) {
		sl = append(sl, regexp.MustCompile(`[^0-9a-zA-Z]+`).ReplaceAllString(c, "_"))
	}
	path := filepath.Join(
		os.ExpandEnv(opts.artifacts), strings.Join(sl, "_")+".txt",
	)
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.Create(path)
	return f, opts.fileSeq, err
}

func (opts *optsT) exec(arg0 string, args ...string) (_outputPath string, _exitCode int, _ error) {
	f, _, err := opts.nextFile(arg0, args...)
	if err != nil {
		return "", 0, nil
	}
	defer func() { _ = f.Close() }()

	code, err := opts.execInto(f, f, arg0, args...)
	return f.Name(), code, err
}

func (opts *optsT) backwards(h plumbing.Hash, steps int64) (plumbing.Hash, error) {
	args := []string{
		`git`, `log`, `--first-parent`, `-n`, `1`, `--pretty=format:%H`, fmt.Sprintf("%s~%d", h, steps)}
	if opts.filePredicate != "" {
		// NB: "-- ." behaves differently in that it skips empty commits. By default, we
		// treat empty commits like regular commits.
		args = append(args, "--", opts.filePredicate)
	}
	output, err := opts.execWrap(args[0], args[1:]...)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	sl := strings.Split(strings.TrimSpace(output), "\n")
	hash := strings.TrimSpace(sl[0])
	if hash == "" {
		return plumbing.ZeroHash, errors.Wrapf(io.EOF, "no commits returned from %q", args)

	}
	return plumbing.NewHash(hash), nil
}

func (opts *optsT) readBisectLog() (good, bad plumbing.Hash, _ error) {
	out, err := opts.execWrap(`git`, `bisect`, `log`)
	if err != nil {
		return plumbing.ZeroHash, plumbing.ZeroHash, err
	}
	if !opts.cont {
		return plumbing.ZeroHash, plumbing.ZeroHash, errors.Errorf("bisection already in progress, consider --cont")
	}
	// NB: this breaks bisections which use a nonstandard term for `good`/`bad`.
	// Tough luck. `--continue` will just do extra work.
	sl := regexp.MustCompile(`git bisect (good|bad) (.*)`).FindAllStringSubmatch(out, -1)
	for _, match := range sl {
		switch match[1] {
		case "good":
			good = plumbing.NewHash(match[2])
		case "bad":
			bad = plumbing.NewHash(match[2])
		}
	}
	return good, bad, nil
}

func (opts *optsT) runBisect() error {
	if err := opts.validate(); err != nil {
		return err
	}

	// TODO(tbg): auto-tune duration to run with. When confirming bad commit,
	// measure time until confirmed as bad. Run unknown commits in a loop until
	// exceeding N*<bad_time>. When --cont is provided, also force --bad-time to
	// be provided.

	// Start bisection if not already in one.
	var good, bad plumbing.Hash
	{
		var err error
		good, bad, err = opts.readBisectLog()
		if ae := (*exec.ExitError)(nil); errors.As(err, &ae) && ae.ExitCode() == 1 {
			// NB: we tolerate passing --continue when there's no bisection ongoing,
			// to make it "just work".
			_, err = opts.execWrap(`git`, `bisect`, `start`)
		}
		if err != nil {
			return err
		}
	}

	// If we don't know a bad commit yet, test HEAD expecting it to be bad.
	if bad.IsZero() {
		opts.log("confirming that HEAD is a bad commit")
		head, err := opts.head()
		if err != nil {
			return err
		}
		bad = head

		tr, err := opts.test()
		if err != nil {
			return err
		}
		if tr != testResultBad {
			return errors.Errorf("starting commit %s is %s", bad, tr)
		}
		if err := opts.report(testResultBad); err != nil {
			return err
		}
	} else {
		opts.log("found existing bad commit %s in bisect log", bad)
	}

	// If we don't have a good commit, search backwards in exponential steps to find a good commit.
	if good.IsZero() {
		opts.log("finding a good commit")
		c, err := opts.findGood(bad)
		if err != nil {
			return err
		}
		good = c
		if err := opts.report(testResultGood); err != nil {
			return err
		}
		opts.log("determined good commit %s", good)
	} else {
		opts.log("found existing good commit %s in bisect log", good)
	}
	args := []string{`bisect`, `run`}
	args = append(args, opts.cmd...)
	opts.log("delegating to `git bisect run`")
	// TODO need a redirect-wrapper here that makes sure the output continues to go to artifacts,
	// something like os.Args[0] redirect <artifacts-dir> -- <args>
	_, err := opts.execInto(os.Stdout, os.Stderr, `git`, args...)
	return err
}

func (opts *optsT) findGood(bad plumbing.Hash) (plumbing.Hash, error) {
	// Start at known bad commit, walk back in doubling steps.
	for exp := 1; ; exp++ {
		steps := int64(1 << exp)

		if steps > opts.maxSteps {
			steps = opts.maxSteps
		}

		cur, err := opts.backwards(bad, steps)
		if errors.Is(err, io.EOF) {
			// We ended up testing the very first commit passing our predicates
			// without finding a good commit, so they're all bad - would need to seek
			// back further, which we're not configured to do.
			return plumbing.ZeroHash, errors.Wrap(err, "no good commit found")
		}
		if err != nil {
			return plumbing.ZeroHash, err
		}

		if err := opts.checkout(cur); err != nil {
			return plumbing.ZeroHash, err
		}

		tr, err := opts.test()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if err := opts.report(tr); err != nil {
			return plumbing.ZeroHash, err
		}
		if tr == testResultGood {
			return cur, nil
		}

		if steps >= opts.maxSteps {
			return plumbing.ZeroHash, errors.Errorf("exceeded --max-steps limit trying to find a good commit")
		}
	}
}

func (opts *optsT) checkout(h plumbing.Hash) error {
	out, err := opts.execWrap(`git`, `checkout`, h.String())
	if err != nil {
		return err
	}
	_, _ = fmt.Fprint(os.Stdout, out)
	return nil
}

type testResult byte

func (t testResult) String() string {
	if t == testResultGood {
		return "good"
	}
	if t == testResultBad {
		return "bad"
	}
	return "skip"
}

const (
	testResultSkip testResult = iota
	testResultGood
	testResultBad
)

func (opts *optsT) report(tr testResult) error {
	if _, err := opts.execWrap(`git`, `bisect`, tr.String()); err != nil {
		return err
	}
	return nil
}

func (opts *optsT) test() (testResult, error) {
	path, code, err := opts.exec(opts.cmd[0], opts.cmd[1:]...)

	if err != nil {
		if code == 0 {
			// Failed on something that wasn't the command.
			return 0, err
		}
		_, err := os.ReadFile(path) // TODO
		if err != nil {
			return 0, err
		}
		if true { // TODO do we need a flag to grep out the failures we care about? stress does this already.
			return testResultBad, nil
		}
		return testResultSkip, nil
	}

	return testResultGood, nil
}

func main() {
	if err := rootCmd.PersistentFlags().Parse(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
