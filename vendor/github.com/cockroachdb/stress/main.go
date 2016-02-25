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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	flagP        = flag.Int("p", runtime.NumCPU(), "run `N` processes in parallel")
	flagTimeout  = flag.Duration("timeout", 10*time.Minute, "timeout each process after `duration`")
	flagKill     = flag.Bool("kill", true, "kill timed out processes if true, otherwise just print pid (to attach with gdb)")
	flagFailure  = flag.String("failure", "", "fail only if output matches `regexp`")
	flagIgnore   = flag.String("ignore", "", "ignore failure if output matches `regexp`")
	flagMaxTime  = flag.Duration("maxtime", 0, "maximum time to run")
	flagMaxRuns  = flag.Int("maxruns", 0, "maximum number of runs")
	flagMaxFails = flag.Int("maxfails", 0, "maximum number of failures")
	flagStdErr   = flag.Bool("stderr", false, "output failures to STDERR instead of to a temp file")
)

func roundToSeconds(d time.Duration) time.Duration {
	return time.Duration(d.Seconds()+0.5) * time.Second
}

func main() {
	flag.Parse()
	if *flagP <= 0 || *flagTimeout <= 0 || len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(1)
	}
	var failureRe, ignoreRe *regexp.Regexp
	if *flagFailure != "" {
		var err error
		if failureRe, err = regexp.Compile(*flagFailure); err != nil {
			fmt.Println("bad failure regexp:", err)
			os.Exit(1)
		}
	}
	if *flagIgnore != "" {
		var err error
		if ignoreRe, err = regexp.Compile(*flagIgnore); err != nil {
			fmt.Println("bad ignore regexp:", err)
			os.Exit(1)
		}
	}
	startTime := time.Now()
	finishTime := time.Now().Add(*flagMaxTime)
	res := make(chan []byte)
	var exitFlag int32
	for i := 0; i < *flagP; i++ {
		go func() {
			for atomic.LoadInt32(&exitFlag) == 0 {
				cmd := exec.Command(flag.Args()[0], flag.Args()[1:]...)
				done := make(chan bool)
				if *flagTimeout > 0 {
					go func() {
						select {
						case <-done:
							return
						case <-time.After(*flagTimeout):
						}
						if !*flagKill {
							fmt.Printf("process %v timed out\n", cmd.Process.Pid)
							return
						}
						cmd.Process.Signal(syscall.SIGABRT)
						select {
						case <-done:
							return
						case <-time.After(10 * time.Second):
						}
						cmd.Process.Kill()
					}()
				}
				out, err := cmd.CombinedOutput()
				close(done)
				if err != nil && (failureRe == nil || failureRe.Match(out)) && (ignoreRe == nil || !ignoreRe.Match(out)) {
					out = append(out, fmt.Sprintf("\n\nERROR: %v\n", err)...)
				} else {
					out = []byte{}
				}
				res <- out
			}
		}()
	}
	runs, fails := 0, 0
	ticker := time.NewTicker(5 * time.Second).C
	for atomic.LoadInt32(&exitFlag) == 0 {
		select {
		case out := <-res:
			runs++
			if ((*flagMaxTime > 0) && (time.Now().After(finishTime))) ||
				((*flagMaxRuns > 0) && (runs >= *flagMaxRuns)) {
				atomic.StoreInt32(&exitFlag, 1)
			}
			if len(out) == 0 {
				continue
			}
			fails++
			if (*flagMaxFails > 0) && (fails >= *flagMaxFails) {
				atomic.StoreInt32(&exitFlag, 1)
			}
			if *flagStdErr {
				fmt.Fprintf(os.Stderr, "\n%s\n", out)
			} else {
				f, err := ioutil.TempFile("", "go-stress")
				if err != nil {
					fmt.Printf("failed to create temp file: %v\n", err)
					os.Exit(1)
				}
				f.Write(out)
				f.Close()
				if len(out) > 2<<10 {
					out = out[:2<<10]
				}
				fmt.Printf("\n%s\n%s\n", f.Name(), out)
			}
		case <-ticker:
			fmt.Printf("%v runs so far, %v failures, over %s\n",
				runs, fails, roundToSeconds(time.Since(startTime)))
		}
	}
	fmt.Printf("%v runs completed, %v failures, over %s\n",
		runs, fails, roundToSeconds(time.Since(startTime)))
	if fails > 0 {
		fmt.Println("FAIL")
	} else {
		fmt.Println("SUCCESS")
	}
}
