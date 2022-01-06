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
		Short: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
		// TODO: improve Long description
		Long: "Pick release git SHA for a particular version and communicate the choice via Jira and email",
		RunE: pickSHA,
	}
	// TODO: improve flag usage comments
	cmdPickSHA.Flags().StringVar(&qualifyBucket, "qualify-bucket", "", "release qualification metadata GCS bucket")
	cmdPickSHA.Flags().StringVar(&qualifyObjectPrefix, "qualify-object-prefix", "",
		"release qualification object prefix")
	cmdPickSHA.Flags().StringVar(&releaseBucket, "release-bucket", "", "release candidates metadata GCS bucket")
	cmdPickSHA.Flags().StringVar(&releaseObjectPrefix, "release-object-prefix", "", "release candidate object prefix")
	cmdPickSHA.Flags().StringVar(&releaseSeries, "release-series", "", "major release series")
	cmdPickSHA.Flags().StringVar(&smtpUser, "smtp-user", os.Getenv("SMTP_USER"), "SMTP user name")
	cmdPickSHA.Flags().StringVar(&smtpHost, "smtp-host", "", "SMTP host")
	cmdPickSHA.Flags().IntVar(&smtpPort, "smtp-port", 0, "SMTP port")
	cmdPickSHA.Flags().StringArrayVar(&emailAddresses, "to", []string{}, "email addresses")
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
	nextRelease, err := findNextRelease(releaseSeries)
	if err != nil {
		return fmt.Errorf("cannot find next release: %w", err)
	}
	// TODO: improve stdout message
	fmt.Println("Previous version:", nextRelease.prevReleaseVersion)
	fmt.Println("Next version:", nextRelease.nextReleaseVersion)
	fmt.Println("Release SHA:", nextRelease.buildInfo.SHA)

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseInfoPath := fmt.Sprintf("%s/%s.json", releaseObjectPrefix, nextRelease.nextReleaseVersion)
	fmt.Println("Publishing release candidate metadata")
	if err := publishBuildInfo(context.Background(), nextRelease, releaseBucket, releaseInfoPath); err != nil {
		return fmt.Errorf("cannot publish release metadata: %w", err)
	}

	fmt.Println("Creating SRE issue")
	sreIssue, err := createSRETicket(jiraUsername, jiraToken, nextRelease)
	if err != nil {
		return err
	}

	fmt.Println("Creating tracking issue")
	trackingIssue, err := createTrackingIssue(jiraUsername, jiraToken, nextRelease, sreIssue)
	if err != nil {
		return fmt.Errorf("cannot create tracking issue: %w", err)
	}
	diffUrl := template.URL(
		fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s",
			nextRelease.prevReleaseVersion,
			nextRelease.buildInfo.SHA))
	emailArgs := emailArgs{
		Version:          nextRelease.nextReleaseVersion,
		SHA:              nextRelease.buildInfo.SHA,
		TrackingIssue:    trackingIssue.Key,
		TrackingIssueURL: template.URL(jiraUrl(trackingIssue)),
		DiffUrl:          diffUrl,
	}
	smtpOpts := smtpOpts{
		from:     fmt.Sprintf("Justin Beaver <%s>", smtpUser),
		host:     smtpHost,
		port:     smtpPort,
		user:     smtpUser,
		password: smtpPassword,
		to:       emailAddresses,
	}
	fmt.Println("Sending email")
	if err := sendmail(emailArgs, smtpOpts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
