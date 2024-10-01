// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/slack-go/slack"
	"golang.org/x/exp/maps"
)

var errNoSlackClient = fmt.Errorf("no Slack client")

type status struct {
	good    []*Cluster
	warn    []*Cluster
	destroy []*Cluster
}

func (s *status) add(c *Cluster, now time.Time) {
	exp := c.ExpiresAt()
	// Clusters without VMs shouldn't exist and are likely dangling resources.
	if c.IsEmptyCluster() {
		// Give a one-hour grace period to avoid any race conditions where a cluster
		// was created but the VMs are still initializing.
		if now.After(c.CreatedAt.Add(time.Hour)) {
			s.destroy = append(s.destroy, c)
		} else {
			s.good = append(s.good, c)
		}
	} else if exp.After(now) {
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

func findUserChannel(client *slack.Client, email string) (string, error) {
	if client == nil {
		return "", errNoSlackClient
	}
	u, err := client.GetUserByEmail(email)
	if err != nil {
		return "", err
	}
	return u.ID, nil
}

func slackClusterExpirationDate(c *Cluster) string {
	return fmt.Sprintf("<!date^%[1]d^{date_short_pretty} {time}|%[2]s>",
		c.GCAt().Unix(),
		c.LifetimeRemaining().Round(time.Second))
}

func postStatus(
	l *logger.Logger, client *slack.Client, channel string, dryrun bool, s *status, badVMs vm.List,
) {
	if dryrun {
		tw := tabwriter.NewWriter(l.Stdout, 0, 8, 2, ' ', 0)
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
			l.Printf("unable to deduplicate notification: %s", err)
		}
		if !send {
			return
		}
	}

	makeStatusFields := func(clusters []*Cluster, elideExpiration bool) []slack.AttachmentField {
		var names []string
		var expirations []string
		for _, c := range clusters {
			names = append(names, c.Name)
			expirations = append(expirations, slackClusterExpirationDate(c))
		}
		fields := []slack.AttachmentField{
			{
				Title: "name",
				Value: strings.Join(names, "\n"),
				Short: true,
			},
		}
		if !elideExpiration {
			fields = append(fields, slack.AttachmentField{
				Title: "expiration",
				Value: strings.Join(expirations, "\n"),
				Short: true,
			})
		}
		return fields
	}

	var attachments []slack.Attachment
	fallback := fmt.Sprintf("clusters: %d live, %d expired, %d destroyed",
		len(s.good), len(s.warn), len(s.destroy))
	if len(s.good) > 0 {
		attachments = append(attachments,
			slack.Attachment{
				Color:    "good",
				Title:    "Live Clusters",
				Fallback: fallback,
				Fields:   makeStatusFields(s.good, false),
			})
	}
	if len(s.warn) > 0 {
		attachments = append(attachments,
			slack.Attachment{
				Color:    "warning",
				Title:    "Expiring Clusters",
				Fallback: fallback,
				Fields:   makeStatusFields(s.warn, false),
			})
	}
	if len(s.destroy) > 0 {
		// N.B. split into empty and non-empty clusters; use a different Title for empty cluster, and elide expiration.
		var emptyClusters []*Cluster
		var nonEmptyClusters []*Cluster
		for _, c := range s.destroy {
			if c.IsEmptyCluster() {
				emptyClusters = append(emptyClusters, c)
			} else {
				nonEmptyClusters = append(nonEmptyClusters, c)
			}
		}
		if len(nonEmptyClusters) > 0 {
			attachments = append(attachments,
				slack.Attachment{
					Color:    "danger",
					Title:    "Destroyed Clusters",
					Fallback: fallback,
					Fields:   makeStatusFields(nonEmptyClusters, false),
				})
		}
		if len(emptyClusters) > 0 {
			attachments = append(attachments,
				slack.Attachment{
					Color:    "danger",
					Title:    "Destroyed Empty Clusters/Dangling Resources",
					Fallback: fallback,
					Fields:   makeStatusFields(emptyClusters, true),
				})
		}
	}
	if len(badVMs) > 0 {
		var names []string
		for _, vm := range badVMs {
			names = append(names, vm.Name)
		}
		sort.Strings(names)
		attachments = append(attachments,
			slack.Attachment{
				Color: "danger",
				Title: "Bad VMs",
				Text:  strings.Join(names, "\n"),
			})
	}

	postMessage(l, client, channel, slack.MsgOptionAttachments(attachments...))
}

func postError(l *logger.Logger, client *slack.Client, channel string, err error) {
	l.Printf("Posting error to Slack: %v", err)
	if client == nil || channel == "" {
		return
	}

	postMessage(
		l, client, channel, slack.MsgOptionText(fmt.Sprintf("```\n%s\n```", err), false),
	)
}

func postMessage(l *logger.Logger, client *slack.Client, channel string, opts ...slack.MsgOption) {
	if client == nil || channel == "" {
		return
	}

	defaultOpts := []slack.MsgOption{
		slack.MsgOptionUsername("roachprod"),
	}

	msgOpts := append(defaultOpts, opts...)
	_, _, err := client.PostMessage(channel, msgOpts...)
	if err != nil {
		l.Printf("Error posting to Slack: %v", err)
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
	fileBytes, err := os.ReadFile(hashPath)
	if err != nil && !oserror.IsNotExist(err) {
		return true, err
	}
	oldHash := string(fileBytes)
	newHash := status.notificationHash()

	if newHash == oldHash {
		return false, nil
	}

	return true, os.WriteFile(hashPath, []byte(newHash), 0644)
}

// resourceDescription groups together resource descriptions to be
// used when a resource is deleted by the GC process. It allows custom
// formatting to be applied in the description used in the Slack
// message sent by roachprod, while keeping a plain text
// representation for our logs.
type resourceDescription struct {
	Description      string
	SlackDescription string
}

// reportDeletedResources will log the resources being deleted and
// send a message on the roachprod-status Slack channel about it.
func reportDeletedResources(
	l *logger.Logger,
	client *slack.Client,
	channel, resourceName string,
	resources []resourceDescription,
) {
	if len(resources) > 0 {
		countMsg := fmt.Sprintf("Destroyed %d %s:", len(resources), resourceName)
		slackMsg := []string{countMsg}
		l.Printf("%s", countMsg)

		for _, r := range resources {
			// Note that we use the unicode "bullet" character here because
			// the Slack API does not render lists in API messages, despite
			// supporting a subset of Markdown in the content.
			//
			// See: https://api.slack.com/reference/surfaces/formatting#lists
			slackMsg = append(slackMsg, fmt.Sprintf("• %s", r.SlackDescription))
			l.Printf("- %s", r.Description)
		}

		postMessage(l, client, channel, slack.MsgOptionText(strings.Join(slackMsg, "\n"), false))
	}
}

// destroyResource is a thin wrapper around a function that actually
// performs a resource deletion, making it as no-op if `dryrun` is
// true.
func destroyResource(dryrun bool, doDestroy func() error) error {
	if dryrun {
		return nil
	}

	return doDestroy()
}

// GCClusters checks all cluster to see if they should be deleted. It only
// fails on failure to perform cloud actions. All other actions (load/save
// file, email) do not abort.
func GCClusters(l *logger.Logger, cloud *Cloud, dryrun bool) error {
	now := timeutil.Now()

	var names []string
	for name := range cloud.Clusters {
		if !config.IsLocalClusterName(name) {
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
		// We skip fake VMs and only delete "bad vms" if they were created more than 1h ago.
		if now.Sub(vm.CreatedAt) >= time.Hour && !vm.EmptyCluster {
			badVMs = append(badVMs, vm)
		}
	}

	client := makeSlackClient()
	// Send out user notifications if any of the user's clusters are expired or
	// will be destroyed.
	for user, status := range users {
		if len(status.warn) > 0 || len(status.destroy) > 0 {
			userChannel, err := findUserChannel(client, user+config.EmailDomain)
			if err == nil {
				postStatus(l, client, userChannel, dryrun, status, nil)
			} else if !errors.Is(err, errNoSlackClient) {
				l.Printf("could not deliver Slack DM to %s: %v", user+config.EmailDomain, err)
			}
		}
	}

	channel, _ := findChannel(client, "roachprod-status", "")
	if len(badVMs) > 0 {
		// Destroy bad VMs.
		var deletedVMs []resourceDescription
		if err := vm.FanOut(badVMs, func(p vm.Provider, vms vm.List) error {
			err := destroyResource(dryrun, func() error {
				return p.Delete(l, vms)
			})

			if err == nil {
				for _, vm := range vms {
					deletedVMs = append(deletedVMs, resourceDescription{
						Description:      vm.Name,
						SlackDescription: fmt.Sprintf("`%s`", vm.Name),
					})
				}
			}

			return err
		}); err != nil {
			postError(l, client, channel, err)
		}

		reportDeletedResources(l, client, channel, "bad VMs", deletedVMs)
	}

	var destroyedClusters []resourceDescription
	for _, c := range s.destroy {
		if err := destroyResource(dryrun, func() error {
			return DestroyCluster(l, c)
		}); err == nil {
			clouds := c.Clouds()
			formatPreamble := func(s string, isSlack bool) string {
				if isSlack {
					return fmt.Sprintf("*%s*", s)
				}
				return s
			}
			formatClouds := func(isSlack bool) string {
				var b strings.Builder
				// preamble
				if len(clouds) > 1 {
					b.WriteString(formatPreamble("clouds", isSlack))
				} else {
					b.WriteString(formatPreamble("cloud", isSlack))
				}
				b.WriteString(": ")
				// join clouds
				var sep string
				if isSlack {
					sep = "`,`"
					// header
					b.WriteString("`")
				} else {
					sep = ","
				}
				b.WriteString(clouds[0])
				for _, s := range clouds[1:] {
					b.WriteString(sep)
					b.WriteString(s)
				}
				if isSlack {
					// trailer
					b.WriteString("`")
				}
				return b.String()
			}

			if !c.IsEmptyCluster() {
				destroyedClusters = append(destroyedClusters, resourceDescription{
					Description:      fmt.Sprintf("%s (%s, expiration: %s)", c.Name, formatClouds(false), c.GCAt().String()),
					SlackDescription: fmt.Sprintf("`%s` (%s, *expiration*: %s)", c.Name, formatClouds(true), slackClusterExpirationDate(c)),
				})
			} else {
				// N.B. elide expiration for dangling resources since it's irrelevant.
				destroyedClusters = append(destroyedClusters, resourceDescription{
					Description:      fmt.Sprintf("%s (%s [empty cluster/dangling resource])", c.Name, formatClouds(false)),
					SlackDescription: fmt.Sprintf("`%s` (%s [empty cluster/dangling resource])", c.Name, formatClouds(true)),
				})
			}
		} else {
			postError(l, client, channel, err)
		}
	}

	reportDeletedResources(l, client, channel, "clusters", destroyedClusters)
	return nil
}

// GCDNS deletes dangling DNS records for clusters that have been destroyed.
// This is inferred when a DNS record name contains a cluster name that is no
// longer present. The cluster list is traversed and the DNS records for each
// provider are listed. If a DNS record is found that does not have a
// corresponding cluster, it is deleted.
func GCDNS(l *logger.Logger, cloud *Cloud, dryrun bool) error {
	// Gather cluster names.
	clusterNames := make(map[string]struct{})
	for _, cluster := range cloud.Clusters {
		clusterNames[cluster.Name] = struct{}{}
	}
	// Ensure all DNS providers do not have records for clusters that are no
	// longer present.
	ctx := context.Background()
	for _, provider := range vm.Providers {
		p, ok := provider.(vm.DNSProvider)
		if !ok {
			continue
		}
		records, err := p.ListRecords(ctx)
		if err != nil {
			return err
		}
		danglingRecordNames := make(map[string]struct{})
		for _, record := range records {
			nameParts := strings.Split(record.Name, ".")
			// Only consider DNS records that contain a cluster name.
			if len(nameParts) < 3 {
				continue
			}
			dnsClusterName := nameParts[2]
			if _, exists := clusterNames[dnsClusterName]; !exists {
				danglingRecordNames[record.Name] = struct{}{}
			}
		}

		client := makeSlackClient()
		channel, _ := findChannel(client, "roachprod-status", "")
		recordNames := maps.Keys(danglingRecordNames)
		sort.Strings(recordNames)

		if err := destroyResource(dryrun, func() error {
			return p.DeleteRecordsByName(ctx, recordNames...)
		}); err != nil {
			return err
		}

		deletedRecords := make([]resourceDescription, 0, len(recordNames))
		for _, name := range recordNames {
			deletedRecords = append(deletedRecords, resourceDescription{
				Description: name,
				// Display record names in backticks so that special characters in
				// the domain name (such as underscores) are not interpreted as markup.
				SlackDescription: fmt.Sprintf("`%s`", name),
			})
		}

		reportDeletedResources(l, client, channel, "dangling DNS records", deletedRecords)
	}
	return nil
}

// GCAzure iterates through subscription IDs passed in --azure-subscription-names
// and performs GC on them.
// N.B. this function does not preserve the existing subscription ID set in the
// provider.
func GCAzure(l *logger.Logger, dryrun bool) error {
	provider := vm.Providers[azure.ProviderName]
	var azureSubscriptions []string
	p, ok := provider.(*azure.Provider)
	if ok {
		azureSubscriptions = p.SubscriptionNames
	}

	if len(azureSubscriptions) == 0 {
		// If no subscription names were specified, then fall back to cleaning up
		// the subscription ID specified in the env or the default subscription.
		cld, _ := ListCloud(l, vm.ListOptions{IncludeEmptyClusters: true, IncludeProviders: []string{azure.ProviderName}})
		return GCClusters(l, cld, dryrun)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.OperationTimeout)
	defer cancel()
	var combinedErrors error
	for _, subscription := range azureSubscriptions {
		if err := p.SetSubscription(ctx, subscription); err != nil {
			combinedErrors = errors.CombineErrors(combinedErrors, err)
			continue
		}

		cld, _ := ListCloud(l, vm.ListOptions{IncludeEmptyClusters: true, IncludeProviders: []string{azure.ProviderName}})
		if err := GCClusters(l, cld, dryrun); err != nil {
			combinedErrors = errors.CombineErrors(combinedErrors, err)
		}
	}
	return combinedErrors
}
