// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/nlopes/slack"
)

var errNoSlackClient = fmt.Errorf("no Slack client")

type status struct {
	good    []*Cluster
	warn    []*Cluster
	destroy []*Cluster
}

func (s *status) add(c *Cluster, now time.Time) {
	exp := c.ExpiresAt()
	if exp.After(now) {
		if exp.Before(now.Add(2 * time.Hour)) {
			s.warn = append(s.warn, c)
		} else {
			s.good = append(s.good, c)
		}
	} else {
		s.destroy = append(s.destroy, c)
	}
}

// messageHash computes a base64-encoded hash value to show whether
// or not two status values would result in a duplicate
// notification to a user.
func (s *status) notificationHash() string {
	// Use stdlib hash function, since we don't need any crypto guarantees
	hash := fnv.New32a()

	for i, list := range [][]*Cluster{s.good, s.warn, s.destroy} {
		_, _ = hash.Write([]byte{byte(i)})

		var data []string
		for _, c := range list {
			// Deduplicate by cluster name and expiration time
			data = append(data, fmt.Sprintf("%s %s", c.Name, c.ExpiresAt()))
		}
		// Ensure results are stable
		sort.Strings(data)

		for _, d := range data {
			_, _ = hash.Write([]byte(d))
		}
	}

	bytes := hash.Sum(nil)
	return base64.StdEncoding.EncodeToString(bytes)
}

func makeSlackClient() *slack.Client {
	if config.SlackToken == "" {
		return nil
	}
	client := slack.New(config.SlackToken)
	// client.SetDebug(true)
	return client
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

func findUserChannel(client *slack.Client, email string) (string, error) {
	if client == nil {
		return "", errNoSlackClient
	}
	u, err := client.GetUserByEmail(email)
	if err != nil {
		return "", err
	}
	_, _, channelID, err := client.OpenIMChannel(u.ID)
	return channelID, err
}

func postStatus(client *slack.Client, channel string, dryrun bool, s *status, badVMs vm.List) {
	if dryrun {
		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
		for _, c := range s.good {
			fmt.Fprintf(tw, "good:\t%s\t%s\t(%s)\n", c.Name,
				c.GCAt().Format(time.Stamp),
				c.LifetimeRemaining().Round(time.Second))
		}
		for _, c := range s.warn {
			fmt.Fprintf(tw, "warn:\t%s\t%s\t(%s)\n", c.Name,
				c.GCAt().Format(time.Stamp),
				c.LifetimeRemaining().Round(time.Second))
		}
		for _, c := range s.destroy {
			fmt.Fprintf(tw, "destroy:\t%s\t%s\t(%s)\n", c.Name,
				c.GCAt().Format(time.Stamp),
				c.LifetimeRemaining().Round(time.Second))
		}
		_ = tw.Flush()
	}

	if client == nil || channel == "" {
		return
	}

	// Debounce messages, unless we have badVMs since that indicates
	// a problem that needs manual intervention
	if len(badVMs) == 0 {
		send, err := shouldSend(channel, s)
		if err != nil {
			log.Printf("unable to deduplicate notification: %s", err)
		}
		if !send {
			return
		}
	}

	makeStatusFields := func(clusters []*Cluster) []slack.AttachmentField {
		var names []string
		var expirations []string
		for _, c := range clusters {
			names = append(names, c.Name)
			expirations = append(expirations,
				fmt.Sprintf("<!date^%[1]d^{date_short_pretty} {time}|%[2]s>",
					c.GCAt().Unix(),
					c.LifetimeRemaining().Round(time.Second)))
		}
		return []slack.AttachmentField{
			{
				Title: "name",
				Value: strings.Join(names, "\n"),
				Short: true,
			},
			{
				Title: "expiration",
				Value: strings.Join(expirations, "\n"),
				Short: true,
			},
		}
	}

	params := slack.PostMessageParameters{
		Username: "roachprod",
	}
	fallback := fmt.Sprintf("clusters: %d live, %d expired, %d destroyed",
		len(s.good), len(s.warn), len(s.destroy))
	if len(s.good) > 0 {
		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color:    "good",
				Title:    "Live Clusters",
				Fallback: fallback,
				Fields:   makeStatusFields(s.good),
			})
	}
	if len(s.warn) > 0 {
		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color:    "warning",
				Title:    "Expiring Clusters",
				Fallback: fallback,
				Fields:   makeStatusFields(s.warn),
			})
	}
	if len(s.destroy) > 0 {
		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color:    "danger",
				Title:    "Destroyed Clusters",
				Fallback: fallback,
				Fields:   makeStatusFields(s.destroy),
			})
	}
	if len(badVMs) > 0 {
		var names []string
		for _, vm := range badVMs {
			names = append(names, vm.Name)
		}
		sort.Strings(names)
		params.Attachments = append(params.Attachments,
			slack.Attachment{
				Color: "danger",
				Title: "Bad VMs",
				Text:  strings.Join(names, "\n"),
			})
	}

	_, _, err := client.PostMessage(channel, "", params)
	if err != nil {
		log.Println(err)
	}
}

func postError(client *slack.Client, channel string, err error) {
	log.Println(err)
	if client == nil || channel == "" {
		return
	}

	params := slack.PostMessageParameters{
		Username:   "roachprod",
		Markdown:   true,
		EscapeText: false,
	}
	_, _, err = client.PostMessage(channel, fmt.Sprintf("`%s`", err), params)
	if err != nil {
		log.Println(err)
	}
}

// shouldSend determines whether or not the given status was previously
// sent to the channel.  The error returned by this function is
// advisory; the boolean value is always a reasonable behavior.
func shouldSend(channel string, status *status) (bool, error) {
	hashDir := os.ExpandEnv(filepath.Join("${HOME}", ".roachprod", "slack"))
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return true, err
	}
	hashPath := os.ExpandEnv(filepath.Join(hashDir, "notification-"+channel))
	fileBytes, err := ioutil.ReadFile(hashPath)
	if err != nil && !oserror.IsNotExist(err) {
		return true, err
	}
	oldHash := string(fileBytes)
	newHash := status.notificationHash()

	if newHash == oldHash {
		return false, nil
	}

	return true, ioutil.WriteFile(hashPath, []byte(newHash), 0644)
}

// GCClusters checks all cluster to see if they should be deleted. It only
// fails on failure to perform cloud actions. All others actions (load/save
// file, email) do not abort.
func GCClusters(cloud *Cloud, dryrun bool) error {
	now := timeutil.Now()

	var names []string
	for name := range cloud.Clusters {
		if name != config.Local {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var s status
	users := make(map[string]*status)
	for _, name := range names {
		c := cloud.Clusters[name]
		u := users[c.User]
		if u == nil {
			u = &status{}
			users[c.User] = u
		}
		s.add(c, now)
		u.add(c, now)
	}

	// Compile list of "bad vms" and destroy them.
	var badVMs vm.List
	for _, vm := range cloud.BadInstances {
		// We only delete "bad vms" if they were created more than 1h ago.
		if now.Sub(vm.CreatedAt) >= time.Hour {
			badVMs = append(badVMs, vm)
		}
	}

	// Send out notification to #roachprod-status.
	client := makeSlackClient()
	channel, _ := findChannel(client, "roachprod-status")
	postStatus(client, channel, dryrun, &s, badVMs)

	// Send out user notifications if any of the user's clusters are expired or
	// will be destroyed.
	for user, status := range users {
		if len(status.warn) > 0 || len(status.destroy) > 0 {
			userChannel, err := findUserChannel(client, user+config.EmailDomain)
			if err == nil {
				postStatus(client, userChannel, dryrun, status, nil)
			} else if !errors.Is(err, errNoSlackClient) {
				log.Printf("could not deliver Slack DM to %s: %v", user+config.EmailDomain, err)
			}
		}
	}

	if !dryrun {
		if len(badVMs) > 0 {
			// Destroy bad VMs.
			err := vm.FanOut(badVMs, func(p vm.Provider, vms vm.List) error {
				return p.Delete(vms)
			})
			if err != nil {
				postError(client, channel, err)
			}
		}

		// Destroy expired clusters.
		for _, c := range s.destroy {
			if err := DestroyCluster(c); err != nil {
				postError(client, channel, err)
			}
		}
	}
	return nil
}
