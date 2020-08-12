// Copyright 2020 The Cockroach Authors.
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
	"bytes"
	"flag"
	"fmt"
	"go/build"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/ghemawat/stream"
	"github.com/nlopes/slack"
)

var slackToken = flag.String("slack-token", "", "Slack bot token")
var slackChannel = flag.String("slack-channel", "test-infra-ops", "Slack channel")

type skippedTest struct {
	file string
	test string
}

func dirCmd(dir string, name string, args ...string) stream.Filter {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	switch {
	case err == nil:
	case errors.HasType(err, (*exec.ExitError)(nil)):
		// Non-zero exit is expected.
	default:
		log.Fatal(err)
	}
	return stream.ReadLines(bytes.NewReader(out))
}

func makeSlackClient() *slack.Client {
	if *slackToken == "" {
		return nil
	}
	return slack.New(*slackToken)
}

func findChannel(client *slack.Client, name string) (string, error) {
	if client != nil {
		channels, err := client.GetChannels(true)
		if err != nil {
			return "", err
		}
		for _, channel := range channels {
			if channel.Name == name {
				return channel.ID, nil
			}
		}
	}
	return "", fmt.Errorf("not found")
}

func postReport(skipped []skippedTest) {
	client := makeSlackClient()
	if client == nil {
		fmt.Printf("no slack client\n")
		return
	}

	channel, _ := findChannel(client, *slackChannel)
	if channel == "" {
		fmt.Printf("unable to find slack channel: %q\n", *slackChannel)
		return
	}

	params := slack.PostMessageParameters{
		Username: "Craig Cockroach",
	}

	status := "good"
	switch n := len(skipped); {
	case n >= 100:
		status = "danger"
	case n > 10:
		status = "warning"
	}

	message := fmt.Sprintf("%d skipped tests", len(skipped))
	fmt.Println(message)

	params.Attachments = append(params.Attachments,
		slack.Attachment{
			Color:    status,
			Title:    message,
			Fallback: message,
		})

	fileMap := make(map[string]int)
	for i := range skipped {
		fileMap[skipped[i].file]++
	}
	files := make([]string, 0, len(fileMap))
	for file := range fileMap {
		files = append(files, file)
	}
	sort.Strings(files)

	var buf bytes.Buffer
	for _, file := range files {
		fmt.Fprintf(&buf, "%3d %s\n", fileMap[file], file)
	}
	fmt.Print(buf.String())

	params.Attachments = append(params.Attachments,
		slack.Attachment{
			Color: status,
			Text:  fmt.Sprintf("```\n%s```\n", buf.String()),
		})

	if _, _, err := client.PostMessage(channel, "", params); err != nil {
		fmt.Printf("unable to post slack report: %v\n", err)
		return
	}

	fmt.Printf("posted slack report\n")
}

func main() {
	flag.Parse()

	const root = "github.com/cockroachdb/cockroach"

	crdb, err := build.Import(root, "", build.FindOnly)
	if err != nil {
		log.Fatal(err)
	}
	pkgDir := filepath.Join(crdb.Dir, "pkg")

	// NB: This only matches tests, not benchmarks. We can change this to match
	// benchmarks if we want to report on skipped benchmarks too.
	testnameRE := regexp.MustCompile(`([^:]+):func (Test[^(]*).*`)

	// We grep for all test and benchmark names, along with calls to Skip and
	// Skipf. We then loop over this output keeping track of the most recent
	// test/benchmark seen so we can associate the Skip call with the correct
	// test.
	filter := stream.Sequence(
		dirCmd(pkgDir, "git", "grep", "-E", `^func (Test|Benchmark)|\.Skipf?\(`),
		// Short-list of skip reasons to exclude from reporting.
		//
		// TODO(peter): We should probably have an explicit exclude marker.
		stream.GrepNot(`short|PKG specified`),
	)

	var skipped []skippedTest
	var lastTest string
	if err := stream.ForEach(filter, func(s string) {
		switch {
		case strings.Contains(s, ":func "):
			lastTest = s
		case strings.Contains(s, ".Skip"):
			m := testnameRE.FindStringSubmatch(lastTest)
			if m != nil {
				file, test := m[1], m[2]
				skipped = append(skipped, skippedTest{
					file: file,
					test: test,
				})
				// Clear the test so we don't report it more than once.
				lastTest = ""
			}
		}
	}); err != nil {
		log.Fatal(err)
	}

	postReport(skipped)
}
