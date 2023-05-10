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
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/google/shlex"
	"github.com/spf13/cobra"
)

type optsT struct {
	startBad     plumbing.Hash
	startBadDate time.Time

	artifacts     string
	cmd           string
	grep          string
	grepNot       string
	cherryPick    string
	filePredicate string
	maxAge        time.Duration
	cont          bool
	log           func(string, ...interface{})

	reGrep, reGrepNot *regexp.Regexp // TODO grepNot remove or impl
}

func init() {
	rootCmd.PersistentFlags().StringVar(&opts.cmd, "cmd", "", "Command to run for each commit")
	rootCmd.PersistentFlags().StringVar(&opts.grep, "grep", ".", "Consider failure only if it matches regexp (skip commit otherwise)")
	rootCmd.PersistentFlags().StringVar(&opts.grepNot, "grep-not", "", "Consider failure only if it does not match regexp (skip commit otherwise)")
	rootCmd.PersistentFlags().StringVar(&opts.cherryPick, "cherry-pick", "", "Commit to (temporarily) cherry-pick onto each commit being tested")
	rootCmd.PersistentFlags().StringVar(&opts.filePredicate, "only", ".", "Only consider commits that changed the provided pattern (all others will be skipped); use like `git log -- <pat>`")
	rootCmd.PersistentFlags().DurationVar(&opts.maxAge, "max-age", 31*24*time.Hour, "Oldest commit to consider (autobisect will fail if oldest commit is bad)")
	rootCmd.PersistentFlags().BoolVar(&opts.cont, "continue", false, "Additional args to `git log` to narrow down the commits of interest (all others will be skipped)")
}

var opts = optsT{
	log: func(format string, args ...interface{}) {
		_, _ = fmt.Fprintf(os.Stdout, format, args...)
		if n := len(format); n > 0 && format[n-1] != '\n' {
			_, _ = fmt.Fprintln(os.Stdout)
		}
		// log.InfofDepth(context.Background(), 1, format, args...)
	},
	artifacts: filepath.Join("artifacts/autobisect"),
}

func (opts *optsT) validate() error {
	if opts.grep == "" {
		return errors.Errorf("please provide --grep to guard against runaway bisect, " +
			"if you really want to accept anything, use `.`")
	}
	opts.reGrep = regexp.MustCompile(opts.grep)
	if opts.grepNot != "" {
		opts.reGrepNot = regexp.MustCompile(opts.grep)
	}
	return nil
}

var rootCmd = &cobra.Command{
	RunE: func(cmd *cobra.Command, args []string) error {
		return runBisect(opts)
	},
	Example: `autobisect
  --cmd './dev test pkg/kv/kvserver \
  --filter TestTimeSeriesMaintenanceQueue' \
  --grep 'MaintainTimeSeries called [0-9]+ times' \
  --cherry-pick tmp-logging-patch \
  --only ./pkg/kv/kvserver
`,
}

func (opts *optsT) execInto(
	stdout, stderr io.Writer, arg0 string, args ...string,
) (_exitCode int, _ error) {
	// TODO(tbg) escape shell args.
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

func (opts *optsT) exec(arg0 string, args ...string) (_out, _err string, _exitCode int, _ error) {
	var outBuf strings.Builder
	var errBuf strings.Builder
	code, err := opts.execInto(&outBuf, &errBuf, arg0, args...)
	return outBuf.String(), errBuf.String(), code, err
}

func (opts *optsT) backwards(exclusiveHead plumbing.Hash, mult int) (plumbing.Hash, error) {
	const stepDuration = 26 * time.Hour // TODO(tbg): make configurable
	before := opts.startBadDate.Add(-1 * time.Duration(mult) * stepDuration)
	after := opts.startBadDate.Add(-1 * opts.maxAge)
	if after.After(before) {
		return plumbing.ZeroHash, io.EOF
	}
	// NB: --reverse and --max-count don't work as you'd think they would. The
	// combination will return what will be printed last (i.e. newest commit)
	// rather than oldest. So we don't use either and pick the oldest commit
	// from the output manually.
	sOut, _, _, err := opts.exec(`git`, `log`, `--merges`, `--first-parent`,
		`--after=`+after.Format(time.RFC3339), `--before=`+before.Format(time.RFC3339),
		"--pretty=format:%H",
		exclusiveHead.String()+"^",
		"--", opts.filePredicate)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	sl := strings.Split(strings.TrimSpace(sOut), "\n")
	hash := strings.TrimSpace(sl[0])
	if hash == "" {
		return plumbing.ZeroHash, io.EOF
	}
	return plumbing.NewHash(hash), nil
}

func runBisect(opts optsT) error {
	if err := opts.validate(); err != nil {
		return err
	}
	r, err := git.PlainOpen(".")
	if err != nil {
		return err
	}
	startBad, err := r.Head()
	if err != nil {
		return err
	}
	opts.startBad = startBad.Hash()

	{
		sOut, _, _, err := opts.exec(`git`, `log`, `--pretty=%ct`, `--max-count=1`, startBad.Hash().String())
		if err != nil {
			return err
		}
		secs, err := strconv.ParseInt(strings.TrimSpace(sOut), 10, 64)
		if err != nil {
			return err
		}
		opts.startBadDate = time.Unix(secs, 0)
	}

	tr, err := opts.test(opts.startBad)
	if err != nil {
		return err
	}
	if tr != testResultBad {
		return errors.Errorf("starting commit %s is %s", opts.startBad, tr)
	}

	if err := opts.report(opts.startBad, testResultBad); err != nil {
		return err
	}

	c, err := findGood(opts)
	if err != nil {
		return err
	}
	_ = c
	return nil
}

func findGood(opts optsT) (plumbing.Hash, error) {
	// Start at known bad commit, walk back in doubling steps.
	for exp := 1; ; exp++ {
		mult := (1 << exp) - 1 // 1, 3, 7, 15, ...

		// Basically HEAD^<mult> but measured in commit time.
		cur, err := opts.backwards(opts.startBad, mult)
		if errors.Is(err, io.EOF) {
			// We ended up testing the very first commit passing our predicates
			// without finding a good commit, so they're all bad - would need to seek
			// back further, which we're not configured to do.
			return plumbing.ZeroHash, errors.Errorf("no good commit found")
		}
		if err != nil {
			return plumbing.ZeroHash, err
		}

		tr, err := opts.testAndReport(cur)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		opts.log("^-- %s", tr)
		if tr == testResultGood {
			return cur, nil
		}
	}
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

func (opts *optsT) report(h plumbing.Hash, tr testResult) error {
	if _, _, _, err := opts.exec(`git`, `bisect`, tr.String(), h.String()); err != nil {
		return err
	}
	return nil
}

func (opts *optsT) testAndReport(h plumbing.Hash) (testResult, error) {
	tr, err := opts.test(h)
	if err != nil {
		return 0, err
	}
	if err := opts.report(h, tr); err != nil {
		return 0, err
	}
	return tr, nil
}

func (opts *optsT) test(h plumbing.Hash) (testResult, error) {
	// NB: I tried git-go's Checkout for this, but it's very slow (painfully so
	// for this, but also slow for everything like git log), don't recommend it.
	if _, _, _, err := opts.exec(`git`, `checkout`, h.String()); err != nil {
		return 0, err
	}

	args, err := shlex.Split(opts.cmd)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(os.ExpandEnv(opts.artifacts), h.String())
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()
	_, err = opts.execInto(f, f, args[0], args[1:]...)
	if err == nil {
		return testResultGood, nil
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	b, err := os.ReadFile(path)
	if opts.reGrep.Match(b) {
		return testResultBad, nil
	}
	return testResultSkip, nil
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
