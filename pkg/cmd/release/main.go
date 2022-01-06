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
)

func main() {
	smtpPassword = os.Getenv("SMTP_PASSWORD")
	if smtpPassword == "" {
		panic("SMTP_PASSWORD environment variable should be set")
	}

	cmdPickSHA := &cobra.Command{
		Use:   "pick-sha qualify-url publish-url",
		Short: "Pick release git sha for a particular version and communicate the decision",
		Long:  `Pick release sha, send email, create Jira ticket, etc.`,
		RunE:  pickSHA,
	}
	cmdPickSHA.Flags().StringVar(&qualifyBucket, "qualify-bucket", "", "qualify bucket")
	cmdPickSHA.Flags().StringVar(&qualifyObjectPrefix, "qualify-object-prefix", "", "qualify object prefix")
	cmdPickSHA.Flags().StringVar(&releaseBucket, "release-bucket", "", "release bucket")
	cmdPickSHA.Flags().StringVar(&releaseObjectPrefix, "release-object-prefix", "", "release object prefix")
	cmdPickSHA.Flags().StringVar(&releaseSeries, "release-series", "", "release series")
	cmdPickSHA.Flags().BoolVar(&dryRun, "dry-run", false, "dry ryn")
	cmdPickSHA.Flags().StringVar(&smtpUser, "smtp-user", os.Getenv("SMTP_USER"), "smtp user")
	cmdPickSHA.Flags().StringVar(&smtpHost, "smtp-host", "", "smtp host")
	cmdPickSHA.Flags().IntVar(&smtpPort, "smtp-port", 0, "smtp port")
	cmdPickSHA.Flags().StringArrayVar(&emailAddresses, "to", []string{"rail@cockroachlabs.com"}, "to")
	requiredFlags := []string{
		"qualify-bucket",
		"qualify-object-prefix",
		"release-bucket",
		"release-object-prefix",
		"release-series",
		"smtp-user",
		"smtp-host",
		"smtp-port",
	}
	for _, flag := range requiredFlags {
		if err := cmdPickSHA.MarkFlagRequired(flag); err != nil {
			panic(err)
		}
	}
	rootCmd := &cobra.Command{Use: "release"}
	rootCmd.AddCommand(cmdPickSHA)
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func pickSHA(cmd *cobra.Command, args []string) error {
	next, err := getNextRelease(releaseSeries)
	if err != nil {
		return err
	}
	fmt.Println("Previous release:", next.prevReleaseVersion)
	fmt.Println("Target release:", next.nextReleaseVersion)
	fmt.Println("Release SHA", next.nextReleaseMetadata.SHA)

	// TODO: before copying check if it's already there and bail if exists, can be forced by -f
	releaseObj := fmt.Sprintf("%s/%s.json", releaseObjectPrefix, next.nextReleaseVersion)
	if err := publishJSON(context.Background(), next, releaseBucket, releaseObj); err != nil {
		return err
	}

	// ticket, err := postToJira(ctx, meta)
	// if err != nil {
	// 	// TODO: add flag to skip this step
	// 	return err
	// }
	// // TODO: use ticket and metadata in the template passed to email
	// emailArgs := emailArgs{
	// 	Version:    meta.Version,
	// 	SHA:        meta.SHA,
	// 	JiraTicket: ticket.ticket,
	// 	JiraUrl:    ticket.url,
	// }

	emailArgs := emailArgs{
		To:         emailAddresses,
		Version:    next.nextReleaseVersion,
		SHA:        next.nextReleaseMetadata.SHA,
		JiraTicket: "RE-NNN",
		JiraUrl:    "https://path/url",
		DiffUrl: template.URL(fmt.Sprintf("https://github.com/cockroachdb/cockroach/compare/%s...%s",
			next.prevReleaseVersion,
			next.nextReleaseMetadata.SHA)),
	}
	opts := smtpOpts{
		host:     smtpHost,
		port:     smtpPort,
		user:     smtpUser,
		password: smtpPassword,
	}
	if err := sendEmailSMTP(emailArgs, opts); err != nil {
		return err
	}
	return nil
}
