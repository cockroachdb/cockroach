// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/kr/pretty"
)

const (
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

var provisionalReleasePrefixRE = regexp.MustCompile(`^provisional_[0-9]{12}_`)

func main() {
	var platforms release.Platforms
	var gcsBucket string
	var outputDirectory string
	var buildTagOverride string
	var thirdPartyNoticesFileOverride string
	var doProvisional bool
	var isRelease bool
	var doBless bool
	flag.Var(&platforms, "platform", "platforms to build")
	flag.BoolVar(&isRelease, "release", false, "build in release mode instead of bleeding-edge mode")
	flag.StringVar(&gcsBucket, "gcs-bucket", "", "GCS bucket")
	flag.StringVar(&outputDirectory, "output-directory", "",
		"Save local copies of uploaded release archives in this directory")
	flag.StringVar(&buildTagOverride, "build-tag-override", "", "override the version from version.txt")
	flag.StringVar(&thirdPartyNoticesFileOverride, "third-party-notices-file", "", "override the file with third party notices")
	flag.BoolVar(&doProvisional, "provisional", false, "publish provisional binaries")
	flag.BoolVar(&doBless, "bless", false, "bless provisional binaries")

	flag.Parse()
	if len(platforms) == 0 {
		platforms = release.DefaultPlatforms()
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// GCS bucket is required now
	if gcsBucket == "" {
		log.Fatal("GCS bucket not specified")
	}
	if _, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !ok {
		log.Fatal("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")
	}
	gcs, err := release.NewGCS(gcsBucket)
	if err != nil {
		log.Fatalf("Creating GCS session: %s", err)
	}
	providers := []release.ObjectPutGetter{gcs}

	branch, ok := os.LookupEnv(teamcityBuildBranchKey)
	if !ok {
		log.Fatalf("VCS branch environment variable %s is not set", teamcityBuildBranchKey)
	}
	pkg, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}
	// Make sure the WORKSPACE file is in the current working directory.
	_, err = os.Stat(filepath.Join(pkg, "WORKSPACE"))
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}

	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = pkg
	log.Printf("%s %s", cmd.Env, cmd.Args)
	shaOut, err := cmd.Output()
	if err != nil {
		log.Fatalf("%s: out=%q err=%s", cmd.Args, shaOut, err)
	}

	run(providers, platforms, runFlags{
		doProvisional:                 doProvisional,
		doBless:                       doBless,
		isRelease:                     isRelease,
		buildTagOverride:              buildTagOverride,
		branch:                        branch,
		pkgDir:                        pkg,
		sha:                           string(bytes.TrimSpace(shaOut)),
		outputDirectory:               outputDirectory,
		thirdPartyNoticesFileOverride: thirdPartyNoticesFileOverride,
	}, release.ExecFn{})
}

type runFlags struct {
	doProvisional                 bool
	doBless                       bool
	isRelease                     bool
	buildTagOverride              string
	branch                        string
	sha                           string
	pkgDir                        string
	outputDirectory               string
	thirdPartyNoticesFileOverride string
}

func run(
	providers []release.ObjectPutGetter,
	platforms release.Platforms,
	flags runFlags,
	execFn release.ExecFn,
) {
	// TODO(dan): non-release builds currently aren't broken into the two
	// phases. Instead, the provisional phase does them both.
	if !flags.isRelease {
		flags.doProvisional = true
		flags.doBless = false
	}

	var versionStr string
	var updateLatest bool
	if flags.isRelease {
		// If the tag starts with "provisional_", then we're building a binary
		// that we hope will be some final release and the tag will be of the
		// form `provisional_<yyyymmddhhss>_<semver>`. If all goes well with the
		// long-running tests, these bits will be released exactly as-is, so the
		// version is set to <semver> by stripping the prefix.
		versionStr = provisionalReleasePrefixRE.ReplaceAllLiteralString(flags.branch, "")

		ver, err := version.Parse(versionStr)
		if err != nil {
			log.Fatalf("refusing to build release with invalid version name '%s' (err: %s)",
				versionStr, err)
		}

		// Prerelease returns anything after the `-` and before metadata. eg:
		// `beta` for `1.0.1-beta+metadata`
		if ver.PreRelease() == "" {
			// TODO(dan): This is what it did before, but isn't this wrong? It
			// seems like it would mark a patch release of the previous minor
			// version as latest. Instead, move to something like "latest-2.0",
			// "latest-2.1".
			updateLatest = true
		}
	} else {
		versionStr = flags.sha
		updateLatest = true
	}
	var cockroachBuildOpts []opts
	for _, platform := range platforms {
		var o opts
		o.Platform = platform
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = versionStr
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))
		o.Channel = release.ChannelFromPlatform(platform)
		cockroachBuildOpts = append(cockroachBuildOpts, o)
	}

	if flags.doProvisional {
		for _, o := range cockroachBuildOpts {
			buildCockroach(flags, o, execFn)

			if !flags.isRelease {
				for _, provider := range providers {
					release.PutNonRelease(
						provider,
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
				}
			} else {
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
				for _, provider := range providers {
					crdbFiles := append(
						[]release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.AbsolutePath, "cockroach")},
						release.MakeCRDBLibraryArchiveFiles(o.PkgDir, o.Platform)...,
					)
					crdbFiles = append(crdbFiles, licenseFiles...)
					crdbBody, err := release.CreateArchive(o.Platform, o.VersionStr, "cockroach", crdbFiles)
					if err != nil {
						log.Fatalf("cannot create crdb release archive %s", err)
					}

					sqlFiles := []release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.CockroachSQLAbsolutePath, "cockroach-sql")}
					sqlFiles = append(sqlFiles, licenseFiles...)
					sqlBody, err := release.CreateArchive(o.Platform, o.VersionStr, "cockroach-sql", sqlFiles)
					if err != nil {
						log.Fatalf("cannot create sql release archive %s", err)
					}
					release.PutRelease(provider, release.PutReleaseOptions{
						NoCache:         false,
						Platform:        o.Platform,
						VersionStr:      o.VersionStr,
						ArchivePrefix:   "cockroach",
						OutputDirectory: flags.outputDirectory,
					}, crdbBody)
					release.PutRelease(provider, release.PutReleaseOptions{
						NoCache:         false,
						Platform:        o.Platform,
						VersionStr:      o.VersionStr,
						ArchivePrefix:   "cockroach-sql",
						OutputDirectory: flags.outputDirectory,
					}, sqlBody)
				}
			}
		}
	}
	if flags.doBless {
		if !flags.isRelease {
			log.Fatal("cannot bless non-release versions")
		}
		if updateLatest {
			for _, o := range cockroachBuildOpts {
				for _, provider := range providers {
					markLatestRelease(provider, o)
				}
			}
		}
	}
}

func buildCockroach(flags runFlags, o opts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	buildOpts := release.BuildOptions{
		ExecFn:  execFn,
		Channel: release.ChannelFromPlatform(o.Platform),
	}
	if flags.isRelease {
		buildOpts.Release = true
	}
	if flags.buildTagOverride != "" {
		buildOpts.BuildTag = flags.buildTagOverride
	}

	if err := release.MakeRelease(o.Platform, buildOpts, o.PkgDir); err != nil {
		log.Fatal(err)
	}
}

type opts struct {
	VersionStr               string
	Branch                   string
	Platform                 release.Platform
	AbsolutePath             string
	CockroachSQLAbsolutePath string
	PkgDir                   string
	Channel                  string
}

func markLatestRelease(svc release.ObjectPutGetter, o opts) {
	latestOpts := release.LatestOpts{
		Platform:   o.Platform,
		VersionStr: o.VersionStr,
	}
	release.MarkLatestReleaseWithSuffix(svc, latestOpts, "")
	release.MarkLatestReleaseWithSuffix(svc, latestOpts, release.ChecksumSuffix)
}
