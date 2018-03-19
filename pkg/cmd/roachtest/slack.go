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

func postSlackReport(pass, fail map[*test]struct{}, skip int) {
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
	message := fmt.Sprintf("%s: %d passed, %d failed, %d skipped",
		branch, len(pass), len(fail), skip)

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
	params.Attachments = append(params.Attachments,
		slack.Attachment{
			Color:     status,
			Title:     message,
			TitleLink: link,
			Fallback:  message,
		})

	if len(fail) > 0 {
		var failTests []*test
		for t := range fail {
			failTests = append(failTests, t)
		}
		sort.Slice(failTests, func(i, j int) bool {
			return failTests[i].Name() < failTests[j].Name()
		})
		var failures bytes.Buffer
		for _, t := range failTests {
			fmt.Fprintf(&failures, "%s\n", t.Name())
		}

		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color:    "danger",
				Title:    "Failures",
				Text:     failures.String(),
				Fallback: message,
			})
	}

	if _, _, err := client.PostMessage(channel, "", params); err != nil {
		fmt.Println("unable to post slack report: ", err)
	}
}
