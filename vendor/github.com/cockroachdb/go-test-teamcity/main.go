package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	TEAMCITY_TIMESTAMP_FORMAT = "2006-01-02T15:04:05.000"
)

type Test struct {
	Start    string
	Name     string
	Output   string
	Details  []string
	Duration time.Duration
	Status   string
	Race     bool
	Suite    bool
	Package  string
}

var (
	input  = os.Stdin
	output = os.Stdout

	additionalTestName = ""
	artifacts          = ""
	useJSON            = false
	verbose            = false

	run  = regexp.MustCompile("^=== RUN\\s+([a-zA-Z_]\\S*)")
	end  = regexp.MustCompile("^(\\s*)--- (PASS|SKIP|FAIL):\\s+([a-zA-Z_]\\S*) \\((-?[\\.\\ds]+)\\)")
	pkg  = regexp.MustCompile("^(ok|PASS|FAIL|exit status|Found)")
	race = regexp.MustCompile("^WARNING: DATA RACE")
)

func init() {
	flag.BoolVar(&useJSON, "json", false, "Parse input from JSON (as emitted from go tool test2json)")
	flag.StringVar(&additionalTestName, "name", "", "Add prefix to test name")
	flag.BoolVar(&verbose, "v", false, "verbose (print output even for passing tests)")
	flag.StringVar(&artifacts, "artifacts", "", "if nonempty, file to write test outputs to (combines with verbose flag)")

}

func escapeLines(lines []string) string {
	return escape(strings.Join(lines, "\n"))
}

func escape(s string) string {
	s = strings.Replace(s, "|", "||", -1)
	s = strings.Replace(s, "\n", "|n", -1)
	s = strings.Replace(s, "\r", "|n", -1)
	s = strings.Replace(s, "'", "|'", -1)
	s = strings.Replace(s, "]", "|]", -1)
	s = strings.Replace(s, "[", "|[", -1)
	return s
}

func getNow() string {
	return time.Now().Format(TEAMCITY_TIMESTAMP_FORMAT)
}

func outputTest(w io.Writer, test *Test) {
	now := getNow()
	testName := escape(additionalTestName + test.Name)
	fmt.Fprintf(w, "##teamcity[testStarted timestamp='%s' name='%s' captureStandardOutput='true']\n", test.Start, testName)

	if verbose || test.Status != "PASS" || test.Race {
		fmt.Fprint(w, test.Output)
		if artifacts != "" {
			_ = os.MkdirAll(filepath.Dir(artifacts), 0755)
			f, err := os.OpenFile(artifacts, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
			if err == nil {
				fmt.Fprintln(f, test.Output)
				_ = f.Close()
			}
		}
	}

	if test.Status == "SKIP" {
		fmt.Fprintf(w, "##teamcity[testIgnored timestamp='%s' name='%s']\n", now, testName)
	} else {
		if test.Race {
			fmt.Fprintf(w, "##teamcity[testFailed timestamp='%s' name='%s' message='Race detected!' details='%s']\n",
				now, testName, escapeLines(test.Details))
		} else {
			switch test.Status {
			case "FAIL":
				fmt.Fprintf(w, "##teamcity[testFailed timestamp='%s' name='%s' details='%s']\n",
					now, testName, escapeLines(test.Details))
			case "PASS":
				// ignore
			case "UNKNOWN":
				// This can happen when a data race is detected, in which case the test binary
				// exits apruptly assuming GORACE="halt_on_error=1" is specified.
				// CockroachDB CI does this at the time of writing:
				// https://github.com/cockroachdb/cockroach/pull/14590
				fmt.Fprintf(w, "##teamcity[testIgnored timestamp='%s' name='%s' message='"+
					"Test framework exited prematurely. Likely another test panicked or encountered a data race']\n",
					now, testName)
			default:
				fmt.Fprintf(w, "##teamcity[testFailed timestamp='%s' name='%s' message='Test ended in panic.' details='%s']\n",
					now, testName, escapeLines(test.Details))
			}
		}
		fmt.Fprintf(w, "##teamcity[testFinished timestamp='%s' name='%s' duration='%d']\n",
			now, testName, test.Duration/time.Millisecond)
	}
}

func startSuite(w io.Writer, name string) {
	fmt.Fprintf(w, "##teamcity[testSuiteStarted name='%s']\n", escape(name))
}

func finishSuite(w io.Writer, name string) {
	fmt.Fprintf(w, "##teamcity[testSuiteFinished name='%s']\n", escape(name))
}

func suite(name string) string {
	if idx := strings.LastIndex(name, "/"); idx != -1 {
		return name[:idx]
	}
	return ""
}

func processReader(r *bufio.Reader, w io.Writer) {
	tests := map[string]*Test{}
	suites := []string{}
	var test *Test
	newTest := func(name string) *Test {
		t := &Test{
			Name:  name,
			Start: getNow(),
		}
		tests[t.Name] = t
		for n := suite(name); n != ""; n = suite(n) {
			if p := tests[n]; p != nil {
				p.Suite = true
			}
		}
		return t
	}
	var final string
	prefix := "\t"
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}

		runOut := run.FindStringSubmatch(line)
		endOut := end.FindStringSubmatch(line)
		pkgOut := pkg.FindStringSubmatch(line)

		if test != nil && test.Status != "" && (runOut != nil || endOut != nil || pkgOut != nil) {
			for j := len(suites) - 1; j >= 0; j-- {
				if !strings.HasPrefix(test.Name, suites[j]) {
					finishSuite(w, suites[j])
					suites = suites[:j]
				}
			}
			if test.Suite {
				startSuite(w, test.Name)
				suites = append(suites, test.Name)
			}
			outputTest(w, test)
			delete(tests, test.Name)
			test = nil
		}

		if runOut != nil {
			test = newTest(runOut[1])
		} else if endOut != nil {
			test = tests[endOut[3]]
			if test == nil {
				test = newTest(endOut[3])
			}
			prefix = endOut[1] + "\t"
			test.Status = endOut[2]
			test.Duration, _ = time.ParseDuration(endOut[4])
		} else if pkgOut != nil {
			final += line
		} else if test != nil && race.MatchString(line) {
			test.Race = true
		} else if test != nil && test.Status != "" && strings.HasPrefix(line, prefix) {
			line = line[:len(line)-1]
			line = strings.TrimPrefix(line, prefix)
			test.Details = append(test.Details, line)
		} else if test != nil {
			test.Output += line
		} else {
			fmt.Fprint(w, line)
		}
	}
	if test != nil {
		outputTest(w, test)
		delete(tests, test.Name)
	}
	for j := len(suites) - 1; j >= 0; j-- {
		finishSuite(w, suites[j])
	}
	for _, t := range tests {
		outputTest(w, t)
	}

	fmt.Fprint(w, final)
}

func main() {
	flag.Parse()

	if len(additionalTestName) > 0 {
		additionalTestName += " "
	}

	reader := bufio.NewReader(input)

	if useJSON {
		processJSON(reader, output)
	} else {
		processReader(reader, output)
	}
}

// TestEvent is a message as emitted by `go tool test2json`.
type TestEvent struct {
	Time    time.Time // encodes as an RFC3339-format string
	Action  string
	Package string
	Test    string
	Elapsed float64 // seconds
	Output  string
}

func processJSON(r *bufio.Reader, w io.Writer) {
	openTests := map[string]*Test{}
	output := func(name string) {
		test := openTests[name]
		delete(openTests, name)
		outputTest(w, test)
	}

	defer func() {
		sorted := make([]*Test, 0, len(openTests))
		for _, test := range openTests {
			sorted = append(sorted, test)
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Name < sorted[j].Name
		})
		for _, test := range sorted {
			test.Output += "(test not terminated explicitly)\n"
			test.Status = "UNKNOWN"
			outputTest(w, test)
		}
	}()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(strings.TrimSpace(line), "{") {
			// Not the JSON we're looking for.
			fmt.Fprintln(w, line)
			continue
		}
		// Might be JSON, try to decode it.
		// Would be nice to not allocate for every such line, but there is
		// no API for that.
		dec := json.NewDecoder(strings.NewReader(line))
		var event TestEvent
		if err := dec.Decode(&event); err != nil {
			// Wasn't JSON after all.
			fmt.Fprintln(w, line)
			continue
		}

		if openTests[event.Test] == nil {
			if event.Test == "" {
				// We're about to start a new test, but this line doesn't correspond to one.
				// It's probably a package-level info (coverage etc).
				fmt.Fprint(w, event.Output)
				continue
			}
			openTests[event.Test] = &Test{}
		}

		test := openTests[event.Test]
		if test.Name == "" {
			test.Name = event.Test
		}
		test.Output += event.Output
		test.Race = test.Race || race.MatchString(event.Output)
		test.Duration += time.Duration(event.Elapsed * 1E9)
		if test.Package == "" {
			test.Package = event.Package
		}

		switch event.Action {
		case "run":
		case "pause":
		case "cont":
		case "bench":
		case "output":
		case "skip":
			test.Status = "SKIP"
			output(event.Test)
		case "pass":
			test.Status = "PASS"
			output(event.Test)
		case "fail":
			test.Status = "FAIL"
			output(event.Test)
		default:
			log.Fatalf("unknown event type: %+v", event)
		}
	}
}
