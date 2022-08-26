// Copyright 2018 The Cockroach Authors.
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
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ssh"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var (
	l           *logger.Logger
	flags       = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagP       = flags.Int("p", runtime.GOMAXPROCS(0), "run `N` processes in parallel")
	flagTimeout = flags.Duration("timeout", 0, "timeout each process after `duration`")
	_           = flags.Bool("kill", true, "kill timed out processes if true, otherwise just print pid (to attach with gdb)")
	flagFailure = flags.String("failure", "", "fail only if output matches `regexp`")
	flagIgnore  = flags.String("ignore", "", "ignore failure if output matches `regexp`")
	flagMaxTime = flags.Duration("maxtime", 0, "maximum time to run")
	flagMaxRuns = flags.Int("maxruns", 0, "maximum number of runs")
	_           = flags.Int("maxfails", 1, "maximum number of failures")
	flagStderr  = flags.Bool("stderr", true, "output failures to STDERR instead of to a temp file")
)

func init() {
	_ = roachprod.InitProviders()
	loggerCfg := logger.Config{Stdout: os.Stdout, Stderr: os.Stderr}
	var loggerError error
	l, loggerError = loggerCfg.NewLogger("")
	if loggerError != nil {
		fmt.Fprintf(os.Stderr, "unable to configure logger: %s\n", loggerError)
		os.Exit(1)
	}
	if _, err := roachprod.Sync(l); err != nil {
		l.Printf("Failed to sync roachprod data - %v", err)
		os.Exit(1)
	}
}

func roundToSeconds(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

func roachprodRun(clusterName string, cmdArray []string) error {
	return roachprod.Run(context.Background(), l, clusterName, "", "", false, os.Stdout, os.Stderr, cmdArray)
}

func run() error {
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "usage: %s <cluster> <pkg> [<flags>] -- [<args>]\n", flags.Name())
		flags.PrintDefaults()
	}

	if len(os.Args) < 2 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.Newf("%s", b.String())
	}

	cluster := os.Args[1]
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}

	if !*flagStderr {
		return errors.New("-stderr=false is unsupported, please tee to a file (or implement the feature)")
	}

	pkg := os.Args[2]
	localTestBin := filepath.Base(pkg) + ".test"
	{
		fi, err := os.Stat(pkg)
		if err != nil {
			return errors.Wrapf(err, "the pkg flag %q is not a directory relative to the current working directory", pkg)
		}
		if !fi.Mode().IsDir() {
			return fmt.Errorf("the pkg flag %q is not a directory relative to the current working directory", pkg)
		}

		// Verify that the test binary exists.
		fi, err = os.Stat(localTestBin)
		if err != nil {
			return errors.Wrapf(err, "test binary %q does not exist", localTestBin)
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("test binary %q is not a file", localTestBin)
		}
	}
	flagsAndArgs := os.Args[3:]
	stressArgs := flagsAndArgs
	var testArgs []string
	for i, arg := range flagsAndArgs {
		if arg == "--" {
			stressArgs = flagsAndArgs[:i]
			testArgs = flagsAndArgs[i+1:]
			break
		}
	}

	if *flagP <= 0 || *flagTimeout < 0 || len(flags.Args()) == 0 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.Newf("%s", b.String())
	}
	if *flagFailure != "" {
		if _, err := regexp.Compile(*flagFailure); err != nil {
			return errors.Wrap(err, "bad failure regexp")
		}
	}
	if *flagIgnore != "" {
		if _, err := regexp.Compile(*flagIgnore); err != nil {
			return errors.Wrap(err, "bad ignore regexp")
		}
	}

	statuses, err := roachprod.Status(context.Background(), l, cluster, "")
	if err != nil {
		return err
	}
	nodes := len(statuses)

	const stressBin = "bin.docker_amd64/stress"
	if err := roachprod.Put(context.Background(), l, cluster, stressBin, "stress", true); err != nil {
		return err
	}

	const localLibDir = "lib.docker_amd64/"
	if fi, err := os.Stat(localLibDir); err == nil && fi.IsDir() {
		if err := roachprod.Put(context.Background(), l, cluster, localLibDir, "lib", true); err != nil {
			return err
		}
	}

	if err := roachprodRun(cluster, []string{"mkdir", "-p", pkg}); err != nil {
		return err
	}

	testdataPath := filepath.Join(pkg, "testdata")
	if _, err := os.Stat(testdataPath); err == nil {
		if err := roachprodRun(cluster, []string{"rm", "-rf", testdataPath}); err != nil {
			return errors.Wrap(err, "failed to remove old testdata")
		}
		if err := roachprod.Put(context.Background(), l, cluster, testdataPath, testdataPath, true); err != nil {
			return errors.Wrap(err, "failed to copy testdata")
		}
	}
	testBin := filepath.Join(pkg, localTestBin)
	if err := roachprod.Put(context.Background(), l, cluster, localTestBin, testBin, true); err != nil {
		return errors.Wrap(err, "failed to copy testdata")
	}

	c := make(chan os.Signal)
	defer close(c)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGTERM)
	defer signal.Stop(c)

	startTime := timeutil.Now()
	ctx, cancel := func(ctx context.Context) (context.Context, context.CancelFunc) {
		if *flagMaxTime > 0 {
			return context.WithTimeout(ctx, *flagMaxTime)
		}
		return context.WithCancel(ctx)
	}(context.Background())
	defer cancel()

	go func() {
		<-ctx.Done()
		fmt.Printf("shutting down\n")
		_ = roachprod.Stop(context.Background(), l, cluster, roachprod.DefaultStopOpts())
	}()

	go func() {
		for range c {
			cancel()
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	var runs, fails int32
	res := make(chan string)
	error := func(s string) {
		select {
		case <-ctx.Done():
		case res <- s:
		}
	}

	statusRE := regexp.MustCompile(`(\d+) runs (so far|completed), (\d+) failures, over .*`)

	wg.Add(nodes)
	for i := 1; i <= nodes; i++ {
		go func(i int) {
			stdoutR, stdoutW := io.Pipe()
			defer func() {
				_ = stdoutW.Close()
				wg.Done()
			}()

			go func() {
				defer func() {
					_ = stdoutR.Close()
				}()

				var lastRuns, lastFails int
				scanner := bufio.NewScanner(stdoutR)
				for scanner.Scan() {
					m := statusRE.FindStringSubmatch(scanner.Text())
					if m == nil {
						continue
					}
					curRuns, err := strconv.Atoi(m[1])
					if err != nil {
						error(fmt.Sprintf("%s", err))
						return
					}
					curFails, err := strconv.Atoi(m[3])
					if err != nil {
						error(fmt.Sprintf("%s", err))
						return
					}
					if m[2] == "completed" {
						break
					}

					atomic.AddInt32(&runs, int32(curRuns-lastRuns))
					atomic.AddInt32(&fails, int32(curFails-lastFails))
					lastRuns, lastFails = curRuns, curFails

					if *flagMaxRuns > 0 && int(atomic.LoadInt32(&runs)) >= *flagMaxRuns {
						cancel()
					}
				}
			}()

			cmdArray := []string{
				fmt.Sprintf("cd %s; GOTRACEBACK=all ~/stress %s ./%s %s",
					pkg,
					strings.Join(stressArgs, " "),
					filepath.Base(testBin),
					strings.Join(testArgs, " ")),
			}
			if err := roachprodRun(fmt.Sprintf("%s:%d", cluster, i), cmdArray); err != nil {
				error(err.Error())
			}
		}(i)
	}

	ticker := time.NewTicker(5 * time.Second).C
	for {
		select {
		case out := <-res:
			cancel()
			fmt.Fprintf(os.Stderr, "\n%s\n", out)
		case <-ticker:
			fmt.Printf("%v runs so far, %v failures, over %s\n",
				atomic.LoadInt32(&runs), atomic.LoadInt32(&fails),
				roundToSeconds(timeutil.Since(startTime)))
		case <-ctx.Done():
			fmt.Printf("%v runs completed, %v failures, over %s\n",
				atomic.LoadInt32(&runs), atomic.LoadInt32(&fails),
				roundToSeconds(timeutil.Since(startTime)))

			err := ctx.Err()
			switch {
			// A context timeout in this case is indicative of no failures
			// being detected in the allotted duration.
			case errors.Is(err, context.DeadlineExceeded):
				return nil
			case errors.Is(err, context.Canceled):
				if *flagMaxRuns > 0 && int(atomic.LoadInt32(&runs)) >= *flagMaxRuns {
					return nil
				}
				return err
			default:
				return errors.Wrap(err, "unexpected context error")
			}
		}
	}
}

func main() {
	ssh.InsecureIgnoreHostKey = true
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("FAIL")
		os.Exit(1)
	} else {
		fmt.Println("SUCCESS")
	}
}
