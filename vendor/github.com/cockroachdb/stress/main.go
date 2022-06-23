// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The stress utility is intended for catching of episodic failures.
// It runs a given process in parallel in a loop and collects any failures.
// Usage:
// 	$ stress ./fmt.test -test.run=TestSometing -test.cpu=10
// You can also specify a number of parallel processes with -p flag;
// instruct the utility to not kill hanged processes for gdb attach;
// or specify the failure output you are looking for (if you want to
// ignore some other episodic failures).
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	flags             = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagP             = flags.Int("p", runtime.NumCPU(), "run `N` processes in parallel")
	flagTimeout       = flags.Duration("timeout", 0, "timeout each process after `duration`")
	flagKill          = flags.Bool("kill", true, "kill timed out processes if true, otherwise just print pid (to attach with gdb)")
	flagFailure       = flags.String("failure", "", "fail only if output matches `regexp`")
	flagIgnore        = flags.String("ignore", "", "ignore failure if output matches `regexp`")
	flagMaxTime       = flags.Duration("maxtime", 0, "maximum time to run")
	flagMaxRuns       = flags.Int("maxruns", 0, "maximum number of runs")
	flagMaxFails      = flags.Int("maxfails", 1, "maximum number of failures")
	flagStdErr        = flags.Bool("stderr", true, "output failures to STDERR instead of to a temp file")
	flagBazel         = flags.Bool("bazel", false, "run in bazel compatibility mode (propagates TEST_TMPDIR to TMPDIR)")
	flagShardableVars = flags.String("shardable-artifacts", "", "comma-separated list of ENV_VAR=program pairs; sub-processes will get their own copy of each shardable artifact, and those artifacts will be merged with `program` when all sub-processes have completed. It is valid for `program` to be an executable plus a list of command-line arguments separated by spaces.")
)

type runResult struct {
	// true iff the run succeeded
	success bool
	// the combined stdout and stderr from the run
	output []byte
	// if shardable-artifacts is set, this will be the path to a
	// temporary directory containing the sharded artifacts for this run
	shardDir string
}

type mergeProgram struct {
	program string
	args    []string
}

func roundToSeconds(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

func run() error {
	if err := flags.Parse(os.Args[1:]); err != nil {
		return err
	}
	if *flagBazel {
		testTmpdir := os.Getenv("TEST_TMPDIR")
		if testTmpdir == "" {
			return fmt.Errorf("-bazel set but TEST_TMPDIR not set")
		}
		if err := os.Setenv("TMPDIR", testTmpdir); err != nil {
			return err
		}
	}
	// map of environment variable to the merge program for that shardable artifact.
	shardableArtifacts := make(map[string]mergeProgram)
	for _, envProgramPair := range strings.Split(*flagShardableVars, ",") {
		if envProgramPair == "" {
			continue
		}
		if !strings.Contains(envProgramPair, "=") {
			return fmt.Errorf("expected -shardable-artifacts argument to contain =; got %s", envProgramPair)
		}
		envProgramAndArgs := strings.Split(envProgramPair, "=")
		env := envProgramAndArgs[0]
		programArgs := strings.Split(envProgramAndArgs[1], " ")
		shardableArtifacts[env] = mergeProgram{program: programArgs[0], args: programArgs[1:]}
	}
	var shardableArtifactsDir string
	if len(shardableArtifacts) != 0 {
		var err error
		shardableArtifactsDir, err = ioutil.TempDir("", "go-stress-shards")
		if err != nil {
			return err
		}
	}
	environ := os.Environ()
	environ = environ[0:len(environ):len(environ)]
	if *flagP <= 0 || *flagTimeout < 0 || len(flags.Args()) == 0 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.New(b.String())
	}
	var failureRe, ignoreRe *regexp.Regexp
	if *flagFailure != "" {
		var err error
		if failureRe, err = regexp.Compile(*flagFailure); err != nil {
			return fmt.Errorf("bad failure regexp: %s", err)
		}
	}
	if *flagIgnore != "" {
		var err error
		if ignoreRe, err = regexp.Compile(*flagIgnore); err != nil {
			return fmt.Errorf("bad ignore regexp: %s", err)
		}
	}

	c := make(chan os.Signal)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	// TODO(tamird): put this behind a !windows build tag.
	signal.Notify(c, syscall.SIGHUP, syscall.SIGTERM)
	defer signal.Stop(c)
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := func(ctx context.Context) (context.Context, context.CancelFunc) {
		if *flagMaxTime > 0 {
			return context.WithTimeout(ctx, *flagMaxTime)
		}
		return context.WithCancel(ctx)
	}(context.Background())
	defer cancel()
	go func() {
		for range c {
			cancel()
		}
	}()

	startTime := time.Now()

	res := make(chan runResult)
	wg.Add(*flagP)
	for i := 0; i < *flagP; i++ {
		go func(ctx context.Context, shardNum int) {
			defer wg.Done()
			run := 1
			for {
				var shardDir string
				if len(shardableArtifacts) > 0 {
					shardDir = filepath.Join(shardableArtifactsDir, fmt.Sprintf("%d-%d", shardNum, run))
					if err := os.MkdirAll(shardDir, 0755); err != nil {
						panic(err)
					}
				}
				select {
				case <-ctx.Done():
					if shardDir != "" {
						if err := os.RemoveAll(shardDir); err != nil {
							panic(err)
						}
					}
					return
				case res <- func(ctx context.Context) runResult {
					var result runResult
					result.shardDir = shardDir
					subenviron := environ
					for env, _ := range shardableArtifacts {
						shardpath := filepath.Join(result.shardDir, env)
						subenviron = append(subenviron, fmt.Sprintf("%s=%s", env, shardpath))
					}
					var cmd *exec.Cmd
					if *flagTimeout > 0 {
						if *flagKill {
							var cancel context.CancelFunc
							ctx, cancel = context.WithTimeout(ctx, *flagTimeout)
							defer cancel()
						} else {
							defer time.AfterFunc(*flagTimeout, func() {
								fmt.Printf("process %v timed out\n", cmd.Process.Pid)
							}).Stop()
						}
					}
					combinedOutput, err := ioutil.TempFile("", "stress-stdouterr")
					if err != nil {
						panic(err)
					}
					defer func() { _ = os.Remove(combinedOutput.Name()) }()
					cmd = exec.CommandContext(ctx, flags.Args()[0], flags.Args()[1:]...)
					cmd.Env = subenviron
					cmd.Stdout = combinedOutput
					cmd.Stderr = combinedOutput
					cmdErr := cmd.Run()
					_, err = combinedOutput.Seek(0, 0)
					if err != nil {
						result.output = []byte("stress: could not seek to beginning of stdout/stderr for test")
					} else {
						out, err := ioutil.ReadAll(combinedOutput)
						if err != nil {
							result.output = []byte("stress: could not read stdout/stderr for test")
						} else {
							result.output = out
						}
					}
					err = combinedOutput.Close()
					if err != nil {
						panic(err)
					}
					if cmdErr != nil && (failureRe == nil || failureRe.Match(result.output)) && (ignoreRe == nil || !ignoreRe.Match(result.output)) {
						result.output = append(result.output, fmt.Sprintf("\n\nERROR: %v\n", cmdErr)...)
					} else {
						result.success = true
					}
					return result
				}(ctx):
				}
				run += 1
			}
		}(ctx, i)
	}
	runs, fails := 0, 0
	ticker := time.NewTicker(5 * time.Second).C
	// Map of environment variable -> list of files to merge for that environment variable.
	filesToMerge := make(map[string][]string)
	var extraDirToDelete string
	for {
		select {
		case out := <-res:
			runs++
			if *flagMaxRuns > 0 && runs >= *flagMaxRuns {
				cancel()
			}
			if !out.success || len(filesToMerge) == 0 {
				for env, _ := range shardableArtifacts {
					shardedArtifact := filepath.Join(out.shardDir, env)
					files, ok := filesToMerge[env]
					if !ok {
						filesToMerge[env] = []string{shardedArtifact}
					} else {
						filesToMerge[env] = append(files, shardedArtifact)
					}
				}
				// Note: if there are only successful runs, we want to run the
				// merge program with the artifacts from the successful run, but
				// delete it afterward.
				if out.success {
					extraDirToDelete = out.shardDir
				}
			} else if out.shardDir != "" {
				if err := os.RemoveAll(out.shardDir); err != nil {
					return err
				}
			}
			if out.success {
				continue
			}
			fails++
			if *flagMaxFails > 0 && fails >= *flagMaxFails {
				cancel()
			}
			if *flagStdErr {
				fmt.Fprintf(os.Stderr, "\n%s\n", out.output)
			} else {
				f, err := ioutil.TempFile("", "go-stress")
				if err != nil {
					return fmt.Errorf("failed to create temp file: %v", err)
				}
				if _, err := f.Write(out.output); err != nil {
					return fmt.Errorf("failed to write temp file: %v", err)
				}
				if err := f.Close(); err != nil {
					return fmt.Errorf("failed to close temp file: %v", err)
				}
				if len(out.output) > 2<<10 {
					out.output = out.output[:2<<10]
				}
				fmt.Printf("\n%s\n%s\n", f.Name(), out.output)
			}
		case <-ticker:
			fmt.Printf("%v runs so far, %v failures, over %s\n",
				runs, fails, roundToSeconds(time.Since(startTime)))
		case <-ctx.Done():

			fmt.Printf("%v runs completed, %v failures, over %s\n",
				runs, fails, roundToSeconds(time.Since(startTime)))

			// Merge sharded artifacts.
			for env, mergeProgram := range shardableArtifacts {
				if len(filesToMerge[env]) == 0 {
					fmt.Printf("stress: skipping merge for artifact %s\n", env)
					continue
				}
				output, err := os.Create(os.Getenv(env))
				if err != nil {
					return err
				}
				allArgs := append(mergeProgram.args, filesToMerge[env]...)
				cmd := exec.Command(mergeProgram.program, allArgs...)
				cmd.Stdout = output
				cmd.Stderr = os.Stderr
				err = cmd.Run()
				if err != nil {
					return err
				}
				err = output.Close()
				if err != nil {
					return err
				}
			}
			if extraDirToDelete != "" {
				if err := os.RemoveAll(extraDirToDelete); err != nil {
					return err
				}
			}
			switch err := ctx.Err(); err {
			// A context timeout in this case is indicative of no failures
			// being detected in the allotted duration.
			case context.DeadlineExceeded:
				return nil
			case context.Canceled:
				if *flagMaxRuns > 0 && runs >= *flagMaxRuns {
					return nil
				}
				return err
			default:
				return fmt.Errorf("unexpected context error: %v", err)
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("SUCCESS")
	}
}
