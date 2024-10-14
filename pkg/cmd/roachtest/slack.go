// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestflags"
	"github.com/slack-go/slack"
)

var slackToken string

func makeSlackClient() *slack.Client {
	if slackToken == "" {
		return nil
	}
	return slack.New(slackToken)
}

func findChannel(client *slack.Client, name string, nextCursor string) (string, error) {
	if client != nil {
		channels, cursor, err := client.GetConversationsForUser(
			&slack.GetConversationsForUserParameters{Cursor: nextCursor},
		)
		if err != nil {
			return "", err
		}
		for _, channel := range channels {
			if channel.Name == name {
				return channel.ID, nil
			}
		}
		if cursor != "" {
			return findChannel(client, name, cursor)
		}
	}
	return "", fmt.Errorf("not found")
}

func sortTests(tests []*testImpl) {
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name() < tests[j].Name()
	})
}

func postSlackReport(pass, fail, skip map[*testImpl]struct{}) {
	client := makeSlackClient()
	if client == nil {
		return
	}

	channel, _ := findChannel(client, "production", "")
	if channel == "" {
		return
	}

	branch := "<unknown branch>"
	if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
		branch = b
	}

	var prefix string
	switch {
	case roachtestflags.Cloud.IsSet():
		prefix = strings.ToUpper(roachtestflags.Cloud.String())
	default:
		prefix = "GCE"
	}
	message := fmt.Sprintf("[%s] %s: %d passed, %d failed, %d skipped",
		prefix, branch, len(pass), len(fail), len(skip))

	var attachments []slack.Attachment
	{
		status := "good"
		if len(fail) > 0 {
			status = "warning"
		}
		var link string
		if buildID := os.Getenv("TC_BUILD_ID"); buildID != "" {
			link = fmt.Sprintf("https://teamcity.cockroachdb.com/viewLog.html?"+
				"buildId=%s&buildTypeId=Cockroach_Nightlies_WorkloadNightly",
				buildID)
		}
		attachments = append(attachments,
			slack.Attachment{
				Color:     status,
				Title:     message,
				TitleLink: link,
				Fallback:  message,
			})
	}

	data := []struct {
		tests map[*testImpl]struct{}
		title string
		color string
	}{
		{pass, "Successes", "good"},
		{fail, "Failures", "danger"},
		{skip, "Skipped", "warning"},
	}
	for _, d := range data {
		tests := make([]*testImpl, 0, len(d.tests))
		for t := range d.tests {
			tests = append(tests, t)
		}
		sortTests(tests)

		var buf bytes.Buffer
		for _, t := range tests {
			fmt.Fprintf(&buf, "%s\n", t.Name())
		}
		attachments = append(attachments,
			slack.Attachment{
				Color:    d.color,
				Title:    fmt.Sprintf("%s: %d", d.title, len(tests)),
				Text:     buf.String(),
				Fallback: message,
			})
	}

	if _, _, err := client.PostMessage(
		channel,
		slack.MsgOptionUsername("roachtest"),
		slack.MsgOptionAttachments(attachments...),
	); err != nil {
		fmt.Println("unable to post slack report: ", err)
	}
}
