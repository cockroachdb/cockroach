// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/kr/pretty"
	"github.com/spf13/cobra"
)

const (
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

var (
	gcsBucket string
	platforms release.Platforms
)

func main() {
	if err := makeRootCmd().Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}

func makeRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "publish-artifacts",
		Short: "Publish cockroach build artifacts",
		Long: `Publish cockroach build artifacts to cloud storage.

This tool supports two modes:
  edge        Publish edge build artifacts (for bleeding-edge builds)
  release     Publish release artifacts (for releases and nightly builds)`,
	}

	// Add persistent flags that are available to all subcommands
	rootCmd.PersistentFlags().StringVar(&gcsBucket, "gcs-bucket", "", "GCS bucket (required)")
	rootCmd.PersistentFlags().Var(&platforms, "platform", "platforms to build (if not specified, builds all default platforms)")
	if err := rootCmd.MarkPersistentFlagRequired("gcs-bucket"); err != nil {
		panic(fmt.Sprintf("Failed to mark gcs-bucket as required: %v", err))
	}

	rootCmd.AddCommand(
		makeEdgeCmd(),
		makeReleaseCmd(),
	)

	return rootCmd
}

func makeEdgeCmd() *cobra.Command {
	edgeCmd := &cobra.Command{
		Use:   "edge",
		Short: "Publish edge build artifacts",
		Long: `Publish edge build artifacts for bleeding-edge builds.

This command builds and publishes edge artifacts for the configured platforms.
It's typically used for continuous integration builds on development branches.`,
		Example: `  publish-artifacts edge --gcs-bucket=my-bucket --platform=linux-amd64
  publish-artifacts edge --gcs-bucket=my-bucket --platform=linux-amd64 --platform=darwin-amd64`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEdgeCmd()
		},
	}

	return edgeCmd
}

func makeReleaseCmd() *cobra.Command {
	var outputDirectory string
	var buildTagOverride string
	var thirdPartyNoticesFileOverride string
	var telemetryDisabled bool
	var cockroachArchivePrefix string

	releaseCmd := &cobra.Command{
		Use:     "release",
		Short:   "Publish release artifacts",
		Long:    "Publish release artifacts for releases and nightly builds.",
		Example: `  publish-artifacts release --gcs-bucket=my-bucket`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReleaseCmd(
				outputDirectory,
				buildTagOverride,
				thirdPartyNoticesFileOverride,
				telemetryDisabled,
				cockroachArchivePrefix,
			)
		},
	}

	releaseCmd.Flags().StringVar(&outputDirectory, "output-directory", "", "Save local copies of uploaded release archives in this directory")
	releaseCmd.Flags().StringVar(&buildTagOverride, "build-tag-override", "", "override the version from version.txt")
	releaseCmd.Flags().StringVar(&thirdPartyNoticesFileOverride, "third-party-notices-file", "", "override the file with third party notices")
	releaseCmd.Flags().BoolVar(&telemetryDisabled, "telemetry-disabled", false, "disable telemetry")
	releaseCmd.Flags().StringVar(&cockroachArchivePrefix, "cockroach-archive-prefix", "cockroach", "prefix for the cockroach archive")

	return releaseCmd
}

func makeGCSProvider() (*release.GCSProvider, error) {
	if _, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !ok {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")
	}
	gcs, err := release.NewGCS(gcsBucket)
	if err != nil {
		return nil, fmt.Errorf("creating GCS session: %w", err)
	}
	return gcs, nil
}

func getSHA() (string, string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", "", fmt.Errorf("unable to locate CRDB directory: %w", err)
	}
	// Make sure the WORKSPACE file is in the current working directory.
	_, err = os.Stat(filepath.Join(wd, "WORKSPACE"))
	if err != nil {
		return "", "", fmt.Errorf("unable to find WORKSPACE file in CRDB directory: %w", err)
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = wd
	log.Printf("%s %s", cmd.Env, cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		return "", "", fmt.Errorf("%s: out=%q err=%w", cmd.Args, out, err)
	}
	return string(bytes.TrimSpace(out)), wd, nil
}

func runEdgeCmd() error {
	platformsList := platforms
	if len(platformsList) == 0 {
		platformsList = release.DefaultPlatforms()
	}
	gcs, err := makeGCSProvider()
	if err != nil {
		return fmt.Errorf("creating GCS provider: %w", err)
	}
	branch := os.Getenv(teamcityBuildBranchKey)
	if branch == "" {
		return fmt.Errorf("VCS branch environment variable %s is not set or empty", teamcityBuildBranchKey)
	}
	sha, pkg, err := getSHA()
	if err != nil {
		return fmt.Errorf("getting SHA: %w", err)
	}

	runEdgeImpl(gcs, platformsList, edgeRunFlags{
		pkgDir: pkg,
		branch: branch,
		sha:    sha,
	}, release.ExecFn{})
	return nil
}

func runReleaseCmd(
	outputDirectory string,
	buildTagOverride string,
	thirdPartyNoticesFileOverride string,
	telemetryDisabled bool,
	cockroachArchivePrefix string,
) error {
	platformsList := platforms
	if len(platformsList) == 0 {
		platformsList = release.DefaultPlatforms()
	}
	gcs, err := makeGCSProvider()
	if err != nil {
		return fmt.Errorf("creating GCS provider: %w", err)
	}
	branch := os.Getenv(teamcityBuildBranchKey)
	if branch == "" {
		return fmt.Errorf("VCS branch environment variable %s is not set or empty", teamcityBuildBranchKey)
	}
	sha, pkg, err := getSHA()
	if err != nil {
		return fmt.Errorf("getting SHA: %w", err)
	}

	return runReleaseImpl(gcs, platformsList, releaseRunFlags{
		buildTagOverride:              buildTagOverride,
		branch:                        branch,
		pkgDir:                        pkg,
		sha:                           sha,
		outputDirectory:               outputDirectory,
		thirdPartyNoticesFileOverride: thirdPartyNoticesFileOverride,
		telemetryDisabled:             telemetryDisabled,
		cockroachArchivePrefix:        cockroachArchivePrefix,
	}, release.ExecFn{})
}

type edgeRunFlags struct {
	branch string
	sha    string
	pkgDir string
}

type releaseRunFlags struct {
	buildTagOverride              string
	branch                        string
	sha                           string
	pkgDir                        string
	outputDirectory               string
	thirdPartyNoticesFileOverride string
	telemetryDisabled             bool
	cockroachArchivePrefix        string
}

func runEdgeImpl(
	gcs release.ObjectPutGetter,
	platforms release.Platforms,
	flags edgeRunFlags,
	execFn release.ExecFn,
) {
	for _, platform := range platforms {
		var o edgeOpts
		o.Platform = platform
		o.ReleaseVersions = []string{flags.sha}
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = flags.sha
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))
		o.Channel = release.ChannelFromPlatform(platform)
		buildEdgeCockroach(gcs, o, execFn)

		if slices.Contains(release.WorkloadPlatforms(), platform) {
			var o edgeOpts
			o.Platform = platform
			o.PkgDir = flags.pkgDir
			o.Branch = flags.branch
			o.VersionStr = flags.sha
			buildAndPublishWorkload(gcs, o, execFn)
		}
	}
}

func runReleaseImpl(
	gcs release.ObjectPutGetter,
	platforms release.Platforms,
	flags releaseRunFlags,
	execFn release.ExecFn,
) error {
	if flags.cockroachArchivePrefix == "" {
		return fmt.Errorf("cockroach archive prefix is not set")
	}
	if flags.telemetryDisabled && flags.cockroachArchivePrefix == "cockroach" {
		return fmt.Errorf("telemetry is disabled, but cockroach archive prefix is set to 'cockroach'")
	}

	versionStr := flags.branch
	var cockroachBuildOpts []releaseOpts
	for _, platform := range platforms {
		var o releaseOpts
		o.Platform = platform
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = versionStr
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))
		o.Channel = release.ChannelFromPlatform(platform)
		o.TelemetryDisabled = flags.telemetryDisabled
		cockroachBuildOpts = append(cockroachBuildOpts, o)
	}

	for _, o := range cockroachBuildOpts {
		buildReleaseCockroach(flags, o, execFn)

		thirdPartyNoticesFile := filepath.Join(o.PkgDir, "licenses", "THIRD-PARTY-NOTICES.txt")
		if flags.thirdPartyNoticesFileOverride != "" {
			thirdPartyNoticesFile = flags.thirdPartyNoticesFileOverride
		}
		licenseFiles := []release.ArchiveFile{
			{
				LocalAbsolutePath: filepath.Join(o.PkgDir, "LICENSE"),
				ArchiveFilePath:   "LICENSE",
			},
			{
				LocalAbsolutePath: thirdPartyNoticesFile,
				ArchiveFilePath:   "THIRD-PARTY-NOTICES.txt",
			},
		}
		crdbFiles := append(
			[]release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.AbsolutePath, "cockroach")},
			release.MakeCRDBLibraryArchiveFiles(o.PkgDir, o.Platform)...,
		)
		crdbFiles = append(crdbFiles, licenseFiles...)
		crdbBody, err := release.CreateArchive(o.Platform, o.VersionStr, flags.cockroachArchivePrefix, crdbFiles)
		if err != nil {
			log.Fatalf("cannot create crdb release archive %s", err)
		}
		release.PutRelease(gcs, release.PutReleaseOptions{
			NoCache:         false,
			Platform:        o.Platform,
			VersionStr:      o.VersionStr,
			ArchivePrefix:   flags.cockroachArchivePrefix,
			OutputDirectory: flags.outputDirectory,
		}, crdbBody)

		// telemetry disabled does not apply to cockroach-sql binaries
		if !flags.telemetryDisabled {
			sqlFiles := []release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.CockroachSQLAbsolutePath, "cockroach-sql")}
			sqlFiles = append(sqlFiles, licenseFiles...)
			sqlBody, err := release.CreateArchive(o.Platform, o.VersionStr, "cockroach-sql", sqlFiles)
			if err != nil {
				log.Fatalf("cannot create sql release archive %s", err)
			}
			release.PutRelease(gcs, release.PutReleaseOptions{
				NoCache:         false,
				Platform:        o.Platform,
				VersionStr:      o.VersionStr,
				ArchivePrefix:   "cockroach-sql",
				OutputDirectory: flags.outputDirectory,
			}, sqlBody)
		}
	}
	return nil
}

func buildEdgeCockroach(gcs release.ObjectPutGetter, o edgeOpts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	buildOpts := release.BuildOptions{
		ExecFn:  execFn,
		Channel: o.Channel,
		Release: false,
	}
	if err := release.MakeRelease(o.Platform, buildOpts, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	release.PutNonRelease(
		gcs,
		release.PutNonReleaseOptions{
			Branch: o.Branch,
			Files: append(
				[]release.NonReleaseFile{
					release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
					release.MakeCRDBBinaryNonReleaseFile(o.CockroachSQLAbsolutePath, o.VersionStr),
				},
				release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.Platform, o.VersionStr)...,
			),
		},
	)
	log.Printf("done building cockroach: %s", pretty.Sprint(o))
}

func buildAndPublishWorkload(gcs release.ObjectPutGetter, o edgeOpts, execFn release.ExecFn) {
	log.Printf("building workload %s", pretty.Sprint(o))
	if err := release.MakeWorkload(o.Platform, release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", "workload")
	if o.Platform == release.PlatformLinuxArm || o.Platform == release.PlatformLinuxS390x {
		o.AbsolutePath += release.SuffixFromPlatform(o.Platform)
	}
	release.PutNonRelease(
		gcs,
		release.PutNonReleaseOptions{
			Branch: o.Branch,
			Files: []release.NonReleaseFile{
				release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
			},
		},
	)
	log.Printf("done building workload: %s", pretty.Sprint(o))
}

func buildReleaseCockroach(flags releaseRunFlags, o releaseOpts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	buildOpts := release.BuildOptions{
		ExecFn:            execFn,
		Channel:           o.Channel,
		Release:           true,
		TelemetryDisabled: o.TelemetryDisabled,
	}
	if flags.buildTagOverride != "" {
		buildOpts.BuildTag = flags.buildTagOverride
	}

	if err := release.MakeRelease(o.Platform, buildOpts, o.PkgDir); err != nil {
		log.Fatal(err)
	}
}

type edgeOpts struct {
	VersionStr               string
	Branch                   string
	ReleaseVersions          []string
	Platform                 release.Platform
	AbsolutePath             string
	CockroachSQLAbsolutePath string
	PkgDir                   string
	Channel                  string
}

type releaseOpts struct {
	VersionStr               string
	Branch                   string
	Platform                 release.Platform
	AbsolutePath             string
	CockroachSQLAbsolutePath string
	PkgDir                   string
	Channel                  string
	TelemetryDisabled        bool
}
