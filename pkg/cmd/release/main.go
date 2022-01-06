package main

import (
	"context"
	"fmt"
	"html/template"
	"os"

	"github.com/spf13/cobra"
)

var (
	qualifyBucket       string
	qualifyObjectPrefix string
	releaseBucket       string
	releaseObjectPrefix string
	releaseSeries       string
	dryRun              bool
	smtpUser            string
	smtpPassword        string
	smtpHost            string
	smtpPort            int
	emailAddresses      []string
	jiraUsername        string
	jiraToken           string
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	smtpPassword = os.Getenv("SMTP_PASSWORD")
	if smtpPassword == "" {
		return fmt.Errorf("SMTP_PASSWORD environment variable should be set")
	}
	jiraUsername = os.Getenv("JIRA_USERNAME")
	if jiraUsername == "" {
		return fmt.Errorf("JIRA_USERNAME environment variable should be set")
	}
	jiraToken = os.Getenv("JIRA_TOKEN")
	if jiraToken == "" {
		return fmt.Errorf("JIRA_TOKEN environment variable should be set")
	}

	cmdPickSHA := &cobra.Command{
		Use:   "pick-sha",
		Short: "Pick release git sha for a particular version and communicate the choice via Jira and email",
		// TODO: improve Long description
		Long: "Pick release git sha for a particular version and communicate the choice via Jira and email",
		RunE: pickSHA,
	}
	// TODO: improve flag usage comments
	cmdPickSHA.Flags().StringVar(&qualifyBucket, "qualify-bucket", "", "qualify bucket")
	cmdPickSHA.Flags().StringVar(&qualifyObjectPrefix, "qualify-object-prefix", "", "qualify object prefix")
	cmdPickSHA.Flags().StringVar(&releaseBucket, "release-bucket", "", "release bucket")
	cmdPickSHA.Flags().StringVar(&releaseObjectPrefix, "release-object-prefix", "", "release object prefix")
	cmdPickSHA.Flags().StringVar(&releaseSeries, "release-series", "", "release series")
	cmdPickSHA.Flags().BoolVar(&dryRun, "dry-run", false, "dry ryn")
	cmdPickSHA.Flags().StringVar(&smtpUser, "smtp-user", os.Getenv("SMTP_USER"), "smtp user")
	cmdPickSHA.Flags().StringVar(&smtpHost, "smtp-host", "", "smtp host")
	cmdPickSHA.Flags().IntVar(&smtpPort, "smtp-port", 0, "smtp port")
	cmdPickSHA.Flags().StringArrayVar(&emailAddresses, "to", []string{}, "to")
	requiredFlags := []string{
		"qualify-bucket",
		"qualify-object-prefix",
		"release-bucket",
		"release-object-prefix",
		"release-series",
		"smtp-user",
		"smtp-host",
		"smtp-port",
		"to",
	}
	for _, flag := range requiredFlags {
		if err := cmdPickSHA.MarkFlagRequired(flag); err != nil {
			return err
		}
	}
	// TODO: improve rootCmd description
	rootCmd := &cobra.Command{Use: "release"}
	rootCmd.AddCommand(cmdPickSHA)
	if err := rootCmd.Execute(); err != nil {
		return err
	}
	return nil
}

func pickSHA(_ *cobra.Command, _ []string) error {
	next, err := getNextRelease(releaseSeries)
	if err != nil {
		return err
	}
	// TODO: improve stdout message
	fmt.Println("Previous release:", next.prevReleaseVersion)
	fmt.Println("Target release:", next.nextReleaseVersion)
	fmt.Println("Release SHA", next.nextReleaseMetadata.SHA)

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseObj := fmt.Sprintf("%s/%s.json", releaseObjectPrefix, next.nextReleaseVersion)
	if err := publishJSON(context.Background(), next, releaseBucket, releaseObj); err != nil {
		return fmt.Errorf("cannot publish release metadata: %w", err)
	}

	trackingIssue, err := createTrackingIssue(jiraUsername, jiraToken, next)
	if err != nil {
		return fmt.Errorf("cannot create tracking issue: %w", err)
	}
	// TODO: add another Jira post for SRE
	// sreIssue, err := createSRETicket(jiraUsername, jiraToken, next)
	// if err != nil {
	// 	return err
	// }
	emailArgs := emailArgs{
		To:         emailAddresses,
		Version:    next.nextReleaseVersion,
		SHA:        next.nextReleaseMetadata.SHA,
		JiraTicket: trackingIssue.Key,
		JiraUrl:    fmt.Sprintf("%sbrowse/%s", jiraUrl, trackingIssue.Key),
		DiffUrl: template.URL(fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s",
			next.prevReleaseVersion,
			next.nextReleaseMetadata.SHA)),
	}
	smtpOpts := smtpOpts{
		from:     fmt.Sprintf("Release Bot <%s>", smtpUser),
		host:     smtpHost,
		port:     smtpPort,
		user:     smtpUser,
		password: smtpPassword,
	}
	if err := sendEmailSMTP(emailArgs, smtpOpts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
