// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/slack-go/slack"
)

type Status string

const (
	StatusStarting  Status = "Starting"
	StatusCompleted Status = "Completed"
	StatusFailed    Status = "Failed"
)

// Notifier is an interface for sending notifications for target and step status updates
type Notifier interface {
	// SendTargetNotification sends a notification to Slack when a target starts or finishes
	SendTargetNotification(targetName string, status Status) error
	// SendStepNotification sends a notification to Slack when a step starts or finishes
	SendStepNotification(targetName, command string, status Status) error
}

// SlackNotifier implements the Notifier interface for Slack
type SlackNotifier struct {
	channel     string // Slack channel to post messages to
	enabled     bool   // Whether Slack integration is enabled
	slackClient *slack.Client
	// Map to store slack thread timestamps for each target combination
	// This is useful to group messages for the same target in a thread
	threadTimestamps     map[string]string
	threadTimestampsLock syncutil.Mutex
}

// NewSlackNotifier creates a new SlackNotifier
func NewSlackNotifier(botToken, channel string) Notifier {
	sn := &SlackNotifier{
		threadTimestamps: make(map[string]string),
	}
	sn.initSlackIntegration(botToken, channel)
	return sn
}

// InitSlackIntegration initializes the Slack integration
func (sn *SlackNotifier) initSlackIntegration(botToken, channel string) {
	// Check if Slack integration is enabled
	if botToken == "" || channel == "" {
		return
	}

	// Create Slack client
	sn.slackClient = slack.New(botToken)
	sn.enabled = true
	fmt.Println("Slack integration initialized successfully")
}

// SendStepNotification sends a notification to Slack when a step starts or finishes
func (sn *SlackNotifier) SendStepNotification(targetName, command string, status Status) error {
	if !sn.enabled || sn.slackClient == nil {
		return nil
	}

	var blocks []slack.Block

	blocks = append(blocks, slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn", fmt.Sprintf("%v Command: `%s`", status, command), false, false),
		nil,
		nil,
	))

	// Check if we have a thread timestamp for this workflow+target
	threadTS := sn.getThreadTimestamp(targetName)

	var options []slack.MsgOption
	options = append(options, slack.MsgOptionBlocks(blocks...))

	// If we have a thread timestamp, add it to the options
	if threadTS != "" {
		options = append(options, slack.MsgOptionTS(threadTS))
	}

	// Send the message to Slack
	_, timestamp, err := sn.slackClient.PostMessage(
		sn.channel,
		options...,
	)

	// If this is the first message for this workflow+target, store the timestamp
	if threadTS == "" && err == nil {
		sn.setThreadTimestamp(targetName, timestamp)
	}

	return err
}

// SendTargetNotification sends a notification to Slack when a step starts or finishes
func (sn *SlackNotifier) SendTargetNotification(targetName string, status Status) error {
	if !sn.enabled || sn.slackClient == nil {
		return nil
	}
	messageText := fmt.Sprintf("%v Target *%s*.", status, targetName)

	// Create buttons for continue, retry (if failure), and abort
	var blocks []slack.Block
	blocks = append(blocks, slack.NewSectionBlock(
		slack.NewTextBlockObject("mrkdwn", messageText, false, false),
		nil,
		nil,
	))
	// Check if we have a thread timestamp for this workflow+target
	threadTS := sn.getThreadTimestamp(targetName)

	var options []slack.MsgOption
	options = append(options, slack.MsgOptionBlocks(blocks...))

	// If we have a thread timestamp, add it to the options
	if threadTS != "" {
		options = append(options, slack.MsgOptionTS(threadTS))
	}

	// Send the message to Slack
	_, timestamp, err := sn.slackClient.PostMessage(
		sn.channel,
		options...,
	)

	// If this is the first message for this workflow+target, store the timestamp
	if threadTS == "" && err == nil {
		sn.setThreadTimestamp(targetName, timestamp)
	}

	return err
}

// getThreadTimestamp returns the thread timestamp for a workflow+target combination
// If no thread timestamp exists, it returns an empty string
func (sn *SlackNotifier) getThreadTimestamp(targetName string) string {
	sn.threadTimestampsLock.Lock()
	defer sn.threadTimestampsLock.Unlock()
	return sn.threadTimestamps[targetName]
}

// setThreadTimestamp sets the thread timestamp for a workflow+target combination
func (sn *SlackNotifier) setThreadTimestamp(targetName, timestamp string) {
	sn.threadTimestampsLock.Lock()
	defer sn.threadTimestampsLock.Unlock()
	sn.threadTimestamps[targetName] = timestamp
}
