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
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/nlopes/slack"
)

var slackToken string

func makeSlackClient() *slack.Client {
	if slackToken == "" {
		return nil
	}
	return slack.New(slackToken)
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

func sortTests(tests []*test) {
	sort.Slice(tests, func(i, j int) bool {
		return tests[i].Name() < tests[j].Name()
	})
}

func postSlackReport(pass, fail, skip map[*test]struct{}) {
	var stablePass []*test
	var stableFail []*test
	var unstablePass []*test
	var unstableFail []*test
	var skipped []*test
	for t := range pass {
		if t.spec.Stable {
			stablePass = append(stablePass, t)
		} else {
			unstablePass = append(unstablePass, t)
		}
	}
	for t := range fail {
		if t.spec.Stable {
			stableFail = append(stableFail, t)
		} else {
			unstableFail = append(unstableFail, t)
		}
	}
	for t := range skip {
		skipped = append(skipped, t)
	}

	client := makeSlackClient()
	if client == nil {
		return
	}

	channel, _ := findChannel(client, "production")
	if channel == "" {
		return
	}

	params := slack.PostMessageParameters{
		Username: "roachtest",
	}

	branch := "<unknown branch>"
	if b := os.Getenv("TC_BUILD_BRANCH"); b != "" {
		branch = b
	}

	var prefix string
	switch {
	case cloud != "":
		prefix = strings.ToUpper(cloud)
	case local:
		prefix = "LOCAL"
	default:
		prefix = "GCE"
	}
	message := fmt.Sprintf("[%s] %s: %d passed, %d failed, %d skipped",
		prefix, branch, len(stablePass), len(stableFail), len(skipped))

	{
		status := "good"
		if len(stableFail) > 0 {
			status = "warning"
		}
		var link string
		if buildID := os.Getenv("TC_BUILD_ID"); buildID != "" {
			link = fmt.Sprintf("https://teamcity.cockroachdb.com/viewLog.html?"+
				"buildId=%s&buildTypeId=Cockroach_Nightlies_WorkloadNightly",
				buildID)
		}
		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color:     status,
				Title:     message,
				TitleLink: link,
				Fallback:  message,
			})
	}

	data := []struct {
		tests []*test
		title string
		color string
	}{
		{stableFail, "Failures", "danger"},
		{unstablePass, "Successes [unstable]", "good"},
		{unstableFail, "Failures [unstable]", "warning"},
		{skipped, "Skipped", "warning"},
	}
	for _, d := range data {
		if len(d.tests) > 0 {
			sortTests(d.tests)
			var buf bytes.Buffer
			for _, t := range d.tests {
				fmt.Fprintf(&buf, "%s\n", t.Name())
			}
			params.Attachments = append(params.Attachments,
				slack.Attachment{
					Color:    d.color,
					Title:    d.title,
					Text:     buf.String(),
					Fallback: message,
				})
		}
	}

	if _, _, err := client.PostMessage(channel, "", params); err != nil {
		fmt.Println("unable to post slack report: ", err)
	}
}
