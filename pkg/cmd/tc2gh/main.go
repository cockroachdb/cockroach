// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// tc2gh provides the glue to post Github issues from failing bleeding edge
// builds. Posting issues requires access to a Github token, but for security
// reasons we cannot make this token available to any build that could be
// triggered via a pull request (since the pull request could adapt
// build/teamcity-* to print out the token). Instead, the token is only known to
// a higher-level job that is only triggered on merge to mainline (i.e. master
// or release branches). To post issues, this higher level job invokes tc2gh
// which will recurse through the build and all of its dependencies and pipes
// all of the (failed) build outputs to github-post.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func mustEnv(name string) string {
	s := os.Getenv(name)
	if s == "" {
		fail(fmt.Errorf("environment variable %s must be set", name))
	}
	return s
}

var user = mustEnv("TC_API_USER")
var token = mustEnv("TC_API_PASSWORD")
var _ = mustEnv("GITHUB_API_TOKEN")

type Build struct {
	ID          uint64
	BuildTypeID string // "Cockroach_UnitTests" etc
	Status      string // FAILURE when failed
}

func discover(ctx context.Context, rootID uint64) ([]Build, error) {
	resp, err := get(ctx, tcurl("/httpAuth/app/rest/builds/", rootID))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var msg struct {
		Build
		Deps struct {
			Build []Build
		} `json:"snapshot-dependencies"`
	}
	if err := json.Unmarshal(b, &msg); err != nil {
		return nil, err
	}
	builds := []Build{msg.Build}
	for _, dep := range msg.Deps.Build {
		transitiveDepBuilds, err := discover(ctx, dep.ID)
		if err != nil {
			return nil, err
		}
		builds = append(builds, transitiveDepBuilds...)
	}
	return builds, nil
}

func walk(ctx context.Context, id uint64, consumer func(closer io.Reader)) error {
	resp, err := get(ctx, tcurl("/downloadBuildLog.html?buildId=", id))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	ls := bufio.NewScanner(resp.Body)
	rd, wr := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer rd.Close()
		consumer(rd)
	}()
	defer wg.Wait()

	for ls.Scan() {
		line, ok := extractJSON(ls.Text())
		if !ok {
			continue
		}
		fmt.Fprintln(wr, line)
	}
	wr.Close()
	return nil
}

func fail(err error) {
	fmt.Println("error:", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 2 {
		fail(errors.New("must pass a build ID"))
	}
	rootID, err := strconv.ParseUint(os.Args[1], 10, 64)
	if err != nil {
		fail(err)
	}
	ctx := context.Background()
	builds, err := discover(ctx, rootID)
	if err != nil {
		panic(err)
	}

	for _, build := range builds {
		if strings.ToLower(build.Status) != "failure" {
			fmt.Printf("%s passed, ignoring\n", build.BuildTypeID)
			continue
		}
		// NB: there are tons of env var requirements for github-post (which
		// calls pkg/cmd/internal/issues), but those are satisfied in CI.
		fmt.Printf("%s failed, invoking github-post on its build log\n", build.BuildTypeID)
		var runErr error
		consumer := func(reader io.Reader) {
			cmd := exec.Command("github-post")
			cmd.Stdin = reader
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			runErr = cmd.Run()
		}
		if err := walk(ctx, build.ID, consumer); err != nil {
			fail(err)
		}
		if runErr != nil {
			fail(runErr)
		}
	}
}

func tcurl(segments ...interface{}) string {
	var buf strings.Builder
	buf.WriteString("https://")
	buf.WriteString(user)
	buf.WriteString(":")
	buf.WriteString(token)
	buf.WriteString("@teamcity.cockroachdb.com")
	for _, s := range segments {
		fmt.Fprint(&buf, s)
	}
	return buf.String()
}

func extractJSON(line string) (string, bool) {
	// TeamCity doesn't give us access to unadulterated stdout/stderr, which
	// is what we'd want to pass to github-post. Instead we have to
	// recover
	//     {"Time":"2019-12-05T01:11:41.338639484-05:00","Action":"pass","Package":"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl","Elapsed":0.137}
	// from
	//     [06:11:41]i:			 [run] {"Time":"2019-12-05T01:11:41.338639484-05:00","Action":"pass","Package":"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/licenseccl","Elapsed":0.137}
	// which isn't too bad.
	pos := strings.Index(line, `{"`)
	ok := pos > -1 &&
		strings.HasSuffix(line, "}") &&
		strings.Contains(line, `"Action":`)
	if ok {
		return line[pos:], ok
	}
	return "", false
}

func get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	return http.DefaultClient.Do(req)
}
