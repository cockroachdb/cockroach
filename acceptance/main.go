// Copyright 2015 The Cockroach Authors.
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
//
// Author: Peter Mattis (peter.mattis@gmail.com)

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"time"
)

type stopper chan struct{}

var numNodes = flag.Int("n", 3, "the number of nodes to start (if not otherwise specified by a test)")
var repeat = flag.Int("r", 1, "the number of times to run each test")
var tests = map[string]func(stopper){}

func registerTest(name string, f func(stopper)) int {
	if _, ok := tests[name]; ok {
		log.Fatalf("\"%s\" already registered", name)
	}
	tests[name] = f
	return 0
}

func runTests(args []string) {
	if len(args) == 0 {
		args = []string{"."}
	}
	re := regexp.MustCompile(strings.Join(args, "|"))

	names := make([]string, 0, len(tests))
	for name := range tests {
		if re.MatchString(name) {
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		log.Fatalf("no matching tests: %s", args)
	}
	sort.Strings(names)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	stopper := make(stopper)
	go func() {
		<-sig
		close(stopper)
	}()

	for i := 1; i <= *repeat; i++ {
		for _, name := range names {
			log.Printf("RUNNING %s/%d", name, i)
			start := time.Now()
			tests[name](stopper)
			select {
			case <-stopper:
				os.Exit(1)
			default:
			}
			log.Printf("PASS %s/%d: %0.1fs\n\n", name, i, time.Since(start).Seconds())
		}
	}
}

func main() {
	log.SetFlags(log.Ltime)
	flag.Parse()
	runTests(flag.Args())
}
