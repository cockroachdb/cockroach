// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	flags       = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flagP       = flags.Int("p", runtime.NumCPU(), "run `N` processes in parallel")
	flagTimeout = flags.Duration("timeout", 0, "timeout each process after `duration`")
	_           = flags.Bool("kill", true, "kill timed out processes if true, otherwise just print pid (to attach with gdb)")
	flagFailure = flags.String("failure", "", "fail only if output matches `regexp`")
	flagIgnore  = flags.String("ignore", "", "ignore failure if output matches `regexp`")
	flagMaxTime = flags.Duration("maxtime", 0, "maximum time to run")
	flagMaxRuns = flags.Int("maxruns", 0, "maximum number of runs")
	_           = flags.Int("maxfails", 1, "maximum number of failures")
	flagStderr  = flags.Bool("stderr", true, "output failures to STDERR instead of to a temp file")
)

func roundToSeconds(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

func run() error {
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "usage: %s <cluster> [<flags>] <test> [<args>]\n", flags.Name())
		flags.PrintDefaults()
	}

	if len(os.Args) < 2 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.New(b.String())
	}

	cluster := os.Args[1]
	if err := flags.Parse(os.Args[2:]); err != nil {
		return err
	}

	if !*flagStderr {
		return errors.New("-stderr=false is unsupported, please tee to a file (or implement the feature)")
	}

	if *flagP <= 0 || *flagTimeout < 0 || len(flags.Args()) == 0 {
		var b bytes.Buffer
		flags.SetOutput(&b)
		flags.Usage()
		return errors.New(b.String())
	}
	if *flagFailure != "" {
		if _, err := regexp.Compile(*flagFailure); err != nil {
			return fmt.Errorf("bad failure regexp: %s", err)
		}
	}
	if *flagIgnore != "" {
		if _, err := regexp.Compile(*flagIgnore); err != nil {
			return fmt.Errorf("bad ignore regexp: %s", err)
		}
	}

	cmd := exec.Command("roachprod", "status", cluster)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v\n%s", err, out)
	}
	nodes := strings.Count(string(out), "\n") - 1

	const stressBin = "bin.docker_amd64/stress"
	cmd = exec.Command("roachprod", "put", cluster, stressBin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	testBin := flags.Args()[0]
	cmd = exec.Command("roachprod", "put", cluster, testBin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
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

	// NB: We don't use CommandContext below because it will `kill -9` the
	// `roachprod ssh` processes. Rather, we watch for the context being canceled
	// (or timing out) and explicitly stop the remote stress tests.
	go func() {
		<-ctx.Done()
		fmt.Printf("shutting down\n")
		_ = exec.Command("roachprod", "stop", cluster).Run()
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

			var stderr bytes.Buffer
			cmd := exec.Command("roachprod",
				"ssh", fmt.Sprintf("%s:%d", cluster, i), "--",
				fmt.Sprintf("GOTRACEBACK=all ./stress %s", strings.Join(os.Args[2:], " ")))
			cmd.Stdout = stdoutW
			cmd.Stderr = &stderr
			if err := cmd.Run(); err != nil {
				error(stderr.String())
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

			switch err := ctx.Err(); err {
			// A context timeout in this case is indicative of no failures
			// being detected in the allotted duration.
			case context.DeadlineExceeded:
				return nil
			case context.Canceled:
				if *flagMaxRuns > 0 && int(atomic.LoadInt32(&runs)) >= *flagMaxRuns {
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
