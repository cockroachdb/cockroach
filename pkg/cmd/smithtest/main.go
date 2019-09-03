// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// smithtest is a tool to execute sqlsmith tests on cockroach demo
// instances. Failures are tracked, de-duplicated, reduced. Issues are
// prefilled for GitHub.
package main

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/internal/sqlsmith"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/google/go-github/github"
	"github.com/lib/pq"
	"github.com/pkg/browser"
)

var (
	flags     = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	cockroach = flags.String("cockroach", "./cockroach", "path to cockroach binary")
	reduce    = flags.String("reduce", "./bin/reduce", "path to reduce binary")
	num       = flags.Int("num", 1, "number of parallel testers")
)

func usage() {
	fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
	flags.PrintDefaults()
	os.Exit(1)
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}

	ctx := context.Background()
	setup := Setup{
		cockroach: *cockroach,
		reduce:    *reduce,
		initSQL:   sqlsmith.SeedTable,
		github:    github.NewClient(nil),
	}
	rand.Seed(timeutil.Now().UnixNano())

	// Prepopulate seen issues from GitHub.
	var opts github.SearchOptions
	for {
		results, _, err := setup.github.Search.Issues(ctx, "is:issue is:open label:C-bug label:O-sqlsmith", &opts)
		if err != nil {
			log.Fatal(err)
		}
		for _, issue := range results.Issues {
			title := filterIssueTitle(issue.GetTitle())
			seen[title] = true
			fmt.Println("pre populate", title)
		}
		if results.GetIncompleteResults() {
			opts.Page++
			continue
		}
		break
	}
	fmt.Println("running...")

	g := ctxgroup.WithContext(ctx)
	for i := 0; i < *num; i++ {
		g.GoCtx(setup.work)
	}
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}

// Setup contains initialization and configuration for running smithers.
type Setup struct {
	cockroach, reduce string
	initSQL           string
	github            *github.Client
}

func (s Setup) work(ctx context.Context) error {
	rnd := rand.New(rand.NewSource(rand.Int63()))
	for {
		if err := s.run(ctx, rnd); err != nil {
			return err
		}
	}
}

var (
	connRE  = regexp.MustCompile(`postgres://root@[\d\.]+:\d+\?.*$`)
	panicRE = regexp.MustCompile(`(?m)^(panic: .*?)( \[recovered\])?$`)
	stackRE = regexp.MustCompile(`panic: .*\n\ngoroutine \d+ \[running\]:\n(?s:(.*))$`)
)

// run is a single sqlsmith worker. It starts a new sqlsmither and cockroach
// demo instance. If an error is found it reduces and submits the issue. If
// an issue is successfully found, this function returns, causing the started
// cockroach instance to shut down. An error is only returned if something
// unexpected happened. That is, panics and internal errors will return
// nil, since they are expected. Something unexpected would be like the
// initialization SQL was unable to run.
func (s Setup) run(ctx context.Context, rnd *rand.Rand) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, s.cockroach, "demo", "--empty")
	cmd.Env = []string{
		"COCKROACH_FORCE_INTERACTIVE=true",
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	if err := cmd.Start(); err != nil {
		return err
	}

	var db *gosql.DB
	// Look for the connection string.
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if match := connRE.FindString(line); match != "" {
			connector, err := pq.NewConnector(match)
			if err != nil {
				return err
			}
			db = gosql.OpenDB(connector)
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	if _, err := db.ExecContext(ctx, s.initSQL); err != nil {
		return err
	}

	opts := []sqlsmith.SmitherOption{
		sqlsmith.DisableMutations(),
	}
	smither, err := sqlsmith.NewSmither(db, rnd, opts...)
	if err != nil {
		return err
	}
	for {
		// If lock is locked for writing (due to a found bug in another
		// go routine), block here until it has finished reducing.
		lock.RLock()
		stmt := smither.Generate()
		_, err := db.ExecContext(ctx, stmt)
		lock.RUnlock()
		if err != nil {
			if strings.Contains(err.Error(), "internal error") {
				// Return from this function on internal
				// errors. This causes the current cockroach
				// instance to shut down and we start a new
				// one. This is not strictly necessary, since
				// internal errors don't mess up the rest of
				// cockroach, but it's just easier to have a
				// single logic flow in case of a found error,
				// which is to shut down and start over (just
				// like the panic case below).
				return s.failure(ctx, stmt, err)
			}

		}
		// If we can't ping, check if the statement caused a panic.
		if err := db.PingContext(ctx); err != nil {
			input := fmt.Sprintf("%s; %s;", s.initSQL, stmt)
			out, _ := exec.CommandContext(ctx, s.cockroach, "demo", "--empty", "-e", input).CombinedOutput()
			var pqerr pq.Error
			if match := stackRE.FindStringSubmatch(string(out)); match != nil {
				pqerr.Detail = strings.TrimSpace(match[1])
			}
			if match := panicRE.FindStringSubmatch(string(out)); match != nil {
				// We found a panic as expected.
				pqerr.Message = match[1]
				return s.failure(ctx, stmt, &pqerr)
			}
			// A panic was not found. Shut everything down by returning an error so it can be investigated.
			fmt.Printf("output:\n%s\n", out)
			fmt.Printf("Ping stmt:\n%s;\n", stmt)
			return err
		}
	}
}

var (
	// lock is used to both protect the seen map from concurrent access
	// and prevent overuse of system resources. When the reducer needs to
	// run it gets the exclusive write lock. When normal queries are being
	// smithed, they use the communal read lock. Thus, the reducer being
	// executed will pause the other testing queries and prevent 2 reducers
	// from running at the same time. This should greatly speed up the time
	// it takes for a single reduction run.
	lock syncutil.RWMutex
	seen = map[string]bool{}
)

// failure de-duplicates, reduces, and files errors. It generally returns nil
// indicating that this was successfully filed and we should continue looking
// for errors.
func (s Setup) failure(ctx context.Context, stmt string, err error) error {
	var message, stack string
	if pqerr, ok := err.(*pq.Error); ok {
		stack = pqerr.Detail
		message = pqerr.Message
	} else {
		message = err.Error()
	}
	filteredMessage := filterIssueTitle(regexp.QuoteMeta(message))
	message = fmt.Sprintf("sql: %s", message)

	lock.Lock()
	// Keep this locked for the remainder of the function so that smither
	// tests won't run during the reducer, and only one reducer can run
	// at once.
	defer lock.Unlock()
	sqlFilteredMessage := fmt.Sprintf("sql: %s", filteredMessage)
	alreadySeen := seen[sqlFilteredMessage]
	if !alreadySeen {
		seen[sqlFilteredMessage] = true
	}
	if alreadySeen {
		fmt.Println("already found", message)
		return nil
	}
	fmt.Println("found", message)
	input := fmt.Sprintf("%s %s", s.initSQL, stmt)

	// Run reducer.
	cmd := exec.CommandContext(ctx, s.reduce, "-v", "-unknown", "-contains", filteredMessage)
	cmd.Stdin = strings.NewReader(input)
	cmd.Stderr = os.Stderr
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		fmt.Println(stmt)
		return err
	}

	// Generate the pre-filled github issue.
	makeBody := func() string {
		return fmt.Sprintf("```\n%s\n```\n\n```\n%s\n```", strings.TrimSpace(out.String()), strings.TrimSpace(stack))
	}
	query := url.Values{
		"title":  []string{message},
		"labels": []string{"C-bug,O-sqlsmith"},
		"body":   []string{makeBody()},
	}
	url := url.URL{
		Scheme:   "https",
		Host:     "github.com",
		Path:     "/cockroachdb/cockroach/issues/new",
		RawQuery: query.Encode(),
	}
	const max = 8000
	// Remove lines from the stack trace to shorten up the request so it's
	// under the github limit.
	for len(url.String()) > max {
		last := strings.LastIndex(stack, "\n")
		if last < 0 {
			break
		}
		stack = stack[:last]
		query["body"][0] = makeBody()
		url.RawQuery = query.Encode()
	}
	if len(url.String()) > max {
		fmt.Println(stmt)
		return errors.New("request could not be shortened to max length")
	}

	if err := browser.OpenURL(url.String()); err != nil {
		return err
	}

	return nil
}

// filterIssueTitle handles issue title where some words in the title can
// vary for identical issues. Usually things like number of bytes, IDs, or
// counts. These are converted into their regex equivalent so they can be
// correctly de-duplicated.
func filterIssueTitle(s string) string {
	for _, reS := range []string{
		`\d+`,
		`given: .*, expected .*`,
		`\*tree\.D\w+`,
	} {
		re := regexp.MustCompile(reS)
		s = re.ReplaceAllString(s, reS)
	}
	return s
}
