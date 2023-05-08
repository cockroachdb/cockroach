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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/spf13/cobra"
)

type optsT struct {
	now time.Time

	cmd           string
	grep          string
	grepNot       string
	cherryPick    string
	filePredicate string
	maxAge        time.Duration
	cont          bool
	log           func(string, ...interface{})
}

func init() {
	rootCmd.PersistentFlags().StringVar(&opts.cmd, "cmd", "", "Command to run for each commit")
	rootCmd.PersistentFlags().StringVar(&opts.grep, "grep", ".", "Consider result only if it matches regexp (skip commit otherwise)")
	rootCmd.PersistentFlags().StringVar(&opts.grepNot, "grep-not", "", "Consider result only if it does not match regexp (skip commit otherwise)")
	rootCmd.PersistentFlags().StringVar(&opts.cherryPick, "cherry-pick", "", "Commit to (temporarily) cherry-pick onto each commit being tested")
	rootCmd.PersistentFlags().StringVar(&opts.filePredicate, "only", ".", "Only consider commits that changed the provided pattern (all others will be skipped); use like `git log -- <pat>`")
	rootCmd.PersistentFlags().DurationVar(&opts.maxAge, "max-age", 31*24*time.Hour, "Oldest commit to consider (autobisect will fail if oldest commit is bad)")
	rootCmd.PersistentFlags().BoolVar(&opts.cont, "continue", false, "Additional args to `git log` to narrow down the commits of interest (all others will be skipped)")
}

var opts = optsT{
	now: time.Now().UTC(),
	log: func(format string, args ...interface{}) {
		log.InfofDepth(context.Background(), 2, format, args...)
	},
}

func (opts *optsT) validate() error {
	if opts.grep == "" {
		return errors.Errorf("please provide --grep to guard against runaway bisect, " +
			"if you really want to accept anything, use `.`")
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

func (opts *optsT) exec(arg0 string, args ...string) (_out, _err string, _exitCode int, _ error) {
	var errBuf strings.Builder
	var outBuf strings.Builder
	// TODO(tbg) escape shell args.
	args = append([]string{arg0}, args...)
	opts.log("%q", args)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = &errBuf
	cmd.Stdout = &outBuf
	err := cmd.Run()

	var exitCode int
	if err != nil {
		exitCode = 1
		if ee := (*exec.ExitError)(nil); errors.As(err, &ee) {
			exitCode = ee.ExitCode()
		}
		opts.log("%q: %v\n%s", args, err, errBuf.String())
	}

	return outBuf.String(), errBuf.String(), exitCode, err
}

func (opts *optsT) backwards(exclusiveHead plumbing.Hash, mult int) (plumbing.Hash, error) {
	const stepDuration = 26 * time.Hour // TODO(tbg): make configurable
	dateLE := opts.now.Add(-1 * time.Duration(mult) * stepDuration)
	if minDateLE := opts.now.Add(-1 * opts.maxAge); minDateLE.After(dateLE) || dateLE.After(opts.now) {
		dateLE = minDateLE
	}
	// NB: --reverse and --max-count don't work as you'd think they would. The
	// combination will return what will be printed last (i.e. newest commit)
	// rather than oldest. So we don't use either and pick the oldest commit
	// from the output manually.
	sOut, _, _, err := opts.exec(`git`, `log`, `--merges`, `--first-parent`,
		`--after=`+dateLE.Format(time.RFC3339), "--pretty=format:%H",
		exclusiveHead.String()+"^", "--", opts.filePredicate)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	sl := strings.Split(strings.TrimSpace(sOut), "\n")
	hash := strings.TrimSpace(sl[len(sl)-1])
	if hash == "" {
		return plumbing.ZeroHash, io.EOF
	}
	return plumbing.NewHash(hash), nil
}

func runBisect(opts optsT) error {
	c, err := firstBad(opts)
	if err != nil {
		return err
	}
	_ = c
	return nil
}

func firstLine(msg string) string {
	idx := strings.Index(msg, "\n")
	if idx < 0 {
		return msg
	}
	return msg[:idx]
}

func firstBad(opts optsT) (*object.Commit, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	r, err := git.PlainOpen(".")
	if err != nil {
		return nil, err
	}
	h, err := r.Head()
	if err != nil {
		return nil, err
	}

	// Start at head, walk back in doubling steps.
	cur := h.Hash()
	for done, mult, exp := false, 0, 1; !done; exp++ {
		// First iteration goes back 1, next iteration goes back an additional 3, etc,
		// so in iteration `exp` we are jumping a width of 2^<exp>.
		mult += (1 << exp) - (1 << (exp - 1))

		// Update curC by walking its parents until we have covered the width
		// for the current step.
		next, err := opts.backwards(cur, mult)
		if errors.Is(err, io.EOF) {
			// We ended up testing the very first commit passing our predicates
			// without finding a good commit, so they're all bad - would need to seek
			// back further, which we're not configured to do.
			return nil, errors.Errorf("no good commit found")
		}
		if err != nil {
			return nil, err
		}
		cur = next
		curC, err := r.CommitObject(cur)
		opts.log("%s %s: %s", curC.Committer.When, curC.Hash.String(), firstLine(curC.Message))
		tr, err := opts.test(curC)
		if err != nil {
			return nil, err
		}
		opts.log("^-- %s", tr)
		_ = tr
	}

	return nil, errors.Errorf("all commits tested are bad")
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

func (opts *optsT) test(c *object.Commit) (testResult, error) {
	// NB: I tried git-go's Checkout for this, but it's very slow (painfully so
	// for this, but also slow for everything like git log), don't recommend it.
	if _, _, _, err := opts.exec(`git`, `checkout`, c.Hash.String()); err != nil {
		return 0, err
	}

	sout, serr, code, err := opts.exec("echo", strings.Split(opts.cmd, " ")...) // TODO https://pkg.go.dev/github.com/google/shlex
	if err != nil {
		return 0, err
	}
	_ = sout
	_ = serr
	_ = code
	return testResultSkip, err // TODO
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
