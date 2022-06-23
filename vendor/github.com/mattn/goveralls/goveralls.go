// Copyright (c) 2013 Yasuhiro Matsumoto, Jason McVetta.
// This is Free Software,  released under the MIT license.
// See http://mattn.mit-license.org/2013 for details.

// goveralls is a Go client for Coveralls.io.
package main

import (
	"bytes"
	_ "crypto/sha512"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/tools/cover"
)

/*
	https://coveralls.io/docs/api_reference
*/

type Flags []string

func (a *Flags) String() string {
	return strings.Join(*a, ",")
}

func (a *Flags) Set(value string) error {
	*a = append(*a, value)
	return nil
}

var (
	extraFlags Flags
	pkg        = flag.String("package", "", "Go package")
	verbose    = flag.Bool("v", false, "Pass '-v' argument to 'go test' and output to stdout")
	race       = flag.Bool("race", false, "Pass '-race' argument to 'go test'")
	debug      = flag.Bool("debug", false, "Enable debug output")
	coverprof  = flag.String("coverprofile", "", "If supplied, use a go cover profile (comma separated)")
	covermode  = flag.String("covermode", "count", "sent as covermode argument to go test")
	repotoken  = flag.String("repotoken", os.Getenv("COVERALLS_TOKEN"), "Repository Token on coveralls")
	endpoint   = flag.String("endpoint", "https://coveralls.io", "Hostname to submit Coveralls data to")
	service    = flag.String("service", "travis-ci", "The CI service or other environment in which the test suite was run. ")
	shallow    = flag.Bool("shallow", false, "Shallow coveralls internal server errors")
	ignore     = flag.String("ignore", "", "Comma separated files to ignore")
)

// usage supplants package flag's Usage variable
var usage = func() {
	cmd := os.Args[0]
	// fmt.Fprintf(os.Stderr, "Usage of %s:\n", cmd)
	s := "Usage: %s [options]\n"
	fmt.Fprintf(os.Stderr, s, cmd)
	flag.PrintDefaults()
}

// A SourceFile represents a source code file and its coverage data for a
// single job.
type SourceFile struct {
	Name     string        `json:"name"`     // File path of this source file
	Source   string        `json:"source"`   // Full source code of this file
	Coverage []interface{} `json:"coverage"` // Requires both nulls and integers
}

// A Job represents the coverage data from a single run of a test suite.
type Job struct {
	RepoToken          *string       `json:"repo_token,omitempty"`
	ServiceJobId       string        `json:"service_job_id"`
	ServicePullRequest string        `json:"service_pull_request,omitempty"`
	ServiceName        string        `json:"service_name"`
	SourceFiles        []*SourceFile `json:"source_files"`
	Git                *Git          `json:"git,omitempty"`
	RunAt              time.Time     `json:"run_at"`
}

// A Response is returned by the Coveralls.io API.
type Response struct {
	Message string `json:"message"`
	URL     string `json:"url"`
	Error   bool   `json:"error"`
}

// getPkgs returns packages for mesuring coverage. Returned packages doesn't
// contain vendor packages.
func getPkgs(pkg string) ([]string, error) {
	if pkg == "" {
		pkg = "./..."
	}
	out, err := exec.Command("go", "list", pkg).CombinedOutput()
	if err != nil {
		return nil, err
	}
	allPkgs := strings.Split(strings.Trim(string(out), "\n"), "\n")
	pkgs := make([]string, 0, len(allPkgs))
	for _, p := range allPkgs {
		if !strings.Contains(p, "/vendor/") {
			pkgs = append(pkgs, p)
		}
	}
	return pkgs, nil
}

func getCoverage() ([]*SourceFile, error) {
	if *coverprof != "" {
		return parseCover(*coverprof)
	}

	// pkgs is packages to run tests and get coverage.
	pkgs, err := getPkgs(*pkg)
	if err != nil {
		return nil, err
	}
	coverpkg := fmt.Sprintf("-coverpkg=%s", strings.Join(pkgs, ","))
	var pfss [][]*cover.Profile
	for _, line := range pkgs {
		f, err := ioutil.TempFile("", "goveralls")
		if err != nil {
			return nil, err
		}
		f.Close()
		cmd := exec.Command("go")
		outBuf := new(bytes.Buffer)
		cmd.Stdout = outBuf
		cmd.Stderr = outBuf
		coverm := *covermode
		if *race {
			coverm = "atomic"
		}
		args := []string{"go", "test", "-covermode", coverm, "-coverprofile", f.Name(), coverpkg}
		if *verbose {
			args = append(args, "-v")
			cmd.Stdout = os.Stdout
		}
		if *race {
			args = append(args, "-race")
		}
		args = append(args, extraFlags...)
		args = append(args, line)
		cmd.Args = args

		err = cmd.Run()
		if err != nil {
			return nil, fmt.Errorf("%v: %v", err, outBuf.String())
		}

		pfs, err := cover.ParseProfiles(f.Name())
		if err != nil {
			return nil, err
		}
		err = os.Remove(f.Name())
		if err != nil {
			return nil, err
		}
		pfss = append(pfss, pfs)
	}

	sourceFiles, err := toSF(mergeProfs(pfss))
	if err != nil {
		return nil, err
	}

	return sourceFiles, nil
}

var vscDirs = []string{".git", ".hg", ".bzr", ".svn"}

func findRepositoryRoot(dir string) (string, bool) {
	for _, vcsdir := range vscDirs {
		if d, err := os.Stat(filepath.Join(dir, vcsdir)); err == nil && d.IsDir() {
			return dir, true
		}
	}
	nextdir := filepath.Dir(dir)
	if nextdir == dir {
		return "", false
	}
	return findRepositoryRoot(nextdir)
}

func getCoverallsSourceFileName(name string) string {
	if dir, ok := findRepositoryRoot(name); !ok {
		return name
	} else {
		filename := strings.TrimPrefix(name, dir+string(os.PathSeparator))
		return filename
	}
}

func process() error {
	log.SetFlags(log.Ltime | log.Lshortfile)
	//
	// Parse Flags
	//
	flag.Usage = usage
	flag.Var(&extraFlags, "flags", "extra flags to the tests")
	flag.Parse()
	if len(flag.Args()) > 0 {
		flag.Usage()
		os.Exit(1)
	}

	//
	// Setup PATH environment variable
	//
	paths := filepath.SplitList(os.Getenv("PATH"))
	if goroot := os.Getenv("GOROOT"); goroot != "" {
		paths = append(paths, filepath.Join(goroot, "bin"))
	}
	if gopath := os.Getenv("GOPATH"); gopath != "" {
		for _, path := range filepath.SplitList(gopath) {
			paths = append(paths, filepath.Join(path, "bin"))
		}
	}
	os.Setenv("PATH", strings.Join(paths, string(filepath.ListSeparator)))

	//
	// Initialize Job
	//
	var jobId string
	if travisJobId := os.Getenv("TRAVIS_JOB_ID"); travisJobId != "" {
		jobId = travisJobId
	} else if circleCiJobId := os.Getenv("CIRCLE_BUILD_NUM"); circleCiJobId != "" {
		jobId = circleCiJobId
	} else if appveyorJobId := os.Getenv("APPVEYOR_JOB_ID"); appveyorJobId != "" {
		jobId = appveyorJobId
	}

	if *repotoken == "" {
		repotoken = nil // remove the entry from json
	}
	var pullRequest string
	if prNumber := os.Getenv("CIRCLE_PR_NUMBER"); prNumber != "" {
		// for Circle CI (pull request from forked repo)
		pullRequest = prNumber
	} else if prNumber := os.Getenv("TRAVIS_PULL_REQUEST"); prNumber != "" && prNumber != "false" {
		pullRequest = prNumber
	} else if prURL := os.Getenv("CI_PULL_REQUEST"); prURL != "" {
		// for Circle CI
		pullRequest = regexp.MustCompile(`[0-9]+$`).FindString(prURL)
	} else if prNumber := os.Getenv("APPVEYOR_PULL_REQUEST_NUMBER"); prNumber != "" {
		pullRequest = prNumber
	}

	sourceFiles, err := getCoverage()
	if err != nil {
		return err
	}

	j := Job{
		RunAt:              time.Now(),
		RepoToken:          repotoken,
		ServicePullRequest: pullRequest,
		Git:                collectGitInfo(),
		SourceFiles:        sourceFiles,
	}

	// Only include a job ID if it's known, otherwise, Coveralls looks
	// for the job and can't find it.
	if jobId != "" {
		j.ServiceJobId = jobId
		j.ServiceName = *service
	}

	// Ignore files
	if len(*ignore) > 0 {
		patterns := strings.Split(*ignore, ",")
		for i, pattern := range patterns {
			patterns[i] = strings.TrimSpace(pattern)
		}
		var files []*SourceFile
	Files:
		for _, file := range j.SourceFiles {
			for _, pattern := range patterns {
				match, err := filepath.Match(pattern, file.Name)
				if err != nil {
					return err
				}
				if match {
					fmt.Printf("ignoring %s\n", file.Name)
					continue Files
				}
			}
			files = append(files, file)
		}
		j.SourceFiles = files
	}

	if *debug {
		b, err := json.MarshalIndent(j, "", "  ")
		if err != nil {
			return err
		}
		log.Printf("Posting data: %s", b)
	}

	b, err := json.Marshal(j)
	if err != nil {
		return err
	}

	params := make(url.Values)
	params.Set("json", string(b))
	res, err := http.PostForm(*endpoint+"/api/v1/jobs", params)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("Unable to read response body from coveralls: %s", err)
	}

	if res.StatusCode >= http.StatusInternalServerError && *shallow {
		fmt.Println("coveralls server failed internally")
		return nil
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("Bad response status from coveralls: %d\n%s", res.StatusCode, bodyBytes)
	}
	var response Response
	if err = json.Unmarshal(bodyBytes, &response); err != nil {
		return fmt.Errorf("Unable to unmarshal response JSON from coveralls: %s\n%s", err, bodyBytes)
	}
	if response.Error {
		return errors.New(response.Message)
	}
	fmt.Println(response.Message)
	fmt.Println(response.URL)
	return nil
}

func main() {
	if err := process(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}
