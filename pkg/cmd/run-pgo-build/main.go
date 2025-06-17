// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//
// run-pgo-build triggers a run of a special roachtest job in TeamCity on the
// current branch, waits for the job to complete, downloads the associated
// artifacts, extracts all the CPU (.pprof) profiles, merges them, and places
// the merged result in a location on disk.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/google/pprof/profile"
)

var outFile = flag.String("out", "", "where to store the result pprof profile")

// processProfiles reads all the parsed profiles from the given channel,
// normalizes and merges all the profiles, and returns the merged result.
func processProfiles(profilesChan chan *profile.Profile) (*profile.Profile, error) {
	var profiles []*profile.Profile
	var first *profile.Profile
	// Normalize all profiles to the first one, meaning that we give equal weight
	// to each profile, regardless of how much CPU time it represents. This
	// prevents large machines or longer profiles from skewing the results.
	for prof := range profilesChan {
		if first == nil {
			first = prof
		} else {
			if err := prof.Normalize(first); err != nil {
				return nil, err
			}
		}
		for i := range prof.Sample {
			// Drop labels, which are not used by pprof but can inflate the profile
			// size significantly.
			prof.Sample[i].Label = nil
		}
		profiles = append(profiles, prof.Compact())
	}
	if len(profiles) == 0 {
		return nil, errors.Errorf("no profiles found")
	}
	return profile.Merge(profiles)
}

func main() {
	flag.Parse()

	if *outFile == "" {
		panic("must supply -out")
	}

	var readWg sync.WaitGroup
	readWg.Add(1)
	profilesChan := make(chan *profile.Profile)
	defer readWg.Wait()
	defer close(profilesChan)

	go func() {
		defer readWg.Done()
		res, err := processProfiles(profilesChan)
		if err != nil {
			panic(err)
		}
		w, err := os.Create(*outFile)
		if err != nil {
			panic(err)
		}
		defer w.Close()
		err = res.Write(w)
		if err != nil {
			panic(err)
		}
	}()

	if err := filepath.WalkDir("./artifacts", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || d.Name() != "merged.cpu.pb.gz" {
			return nil
		}
		if !strings.Contains(path, "/run_1/") {
			fmt.Printf("ignoring profile %s (not from run_1)\n", path)
			return nil
		}
		fmt.Printf("found profile %s\n", path)
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		prof, err := profile.Parse(bytes.NewReader(b))
		if err != nil {
			return err
		}
		profilesChan <- prof
		return nil
	}); err != nil {
		panic(err)
	}
}
