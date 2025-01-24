// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package build

import (
	"bytes"
	_ "embed"
	"fmt"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/redact"
)

// TimeFormat is the reference format for build.Time. Make sure it stays in sync
// with the string passed to the linker in the root Makefile.
const TimeFormat = "2006/01/02 15:04:05"

// These variables are initialized by Bazel via the linker -X flag
// when compiling release binaries.
var (
	utcTime          string // Build time in UTC (year/month/day hour:min:sec)
	rev              string // SHA-1 of this build (git rev-parse)
	buildTagOverride string
	cgoTargetTriple  string
	typ              string // Type of this build: <empty>, "development", or "release"
	channel          string
)

// Distribution is changed by the CCL init-time hook in non-APL builds.
var Distribution = "OSS"

var (
	cgoCompiler       = cgoVersion()
	platform          = fmt.Sprintf("%s %s", runtime.GOOS, runtime.GOARCH)
	envChannel        = envutil.EnvOrDefaultString("COCKROACH_CHANNEL", "unknown")
	enabledAssertions = buildutil.CrdbTestBuild
)

var (
	//go:embed version.txt
	versionTxt       string
	parsedVersionTxt *version.Version = parseCockroachVersion(versionTxt)
	binaryVersion    string           = computeBinaryVersion(buildTagOverride, parsedVersionTxt, rev)
	// binaryVersionTestingOverride is modified by TestingOverrideVersion.
	binaryVersionTestingOverride string
)

const (
	DefaultTelemetryChannel = "official-binary"
	FIPSTelemetryChannel    = "official-fips-binary"
)

// IsRelease returns true if the binary was produced by a "release" build.
func IsRelease() bool {
	return typ == "release"
}

// SeemsOfficial reports whether this binary is likely to have come from an
// official release channel.
func SeemsOfficial() bool {
	return channel == DefaultTelemetryChannel || channel == FIPSTelemetryChannel
}

func parseCockroachVersion(versionTxt string) *version.Version {
	txt := strings.TrimSuffix(versionTxt, "\n")
	v, err := version.Parse(txt)
	if err != nil {
		panic(fmt.Errorf("could not parse version.txt: %w", err))
	}
	return v
}

func computeBinaryVersion(
	buildTagOverride string, parsedVersionTxt *version.Version, revision string,
) string {
	if buildTagOverride != "" {
		return buildTagOverride
	}
	if IsRelease() {
		return parsedVersionTxt.String()
	}
	if revision != "" {
		return fmt.Sprintf("%s-dev-%s", parsedVersionTxt.String(), revision)
	}
	return fmt.Sprintf("%s-dev", parsedVersionTxt.String())
}

// BinaryVersion returns the version prefix, patch number and metadata of the
// current build.
func BinaryVersion() string {
	if binaryVersionTestingOverride != "" {
		return binaryVersionTestingOverride
	}
	return binaryVersion
}

// VersionForURLs is used to determine the version to use in public-facing doc URLs.
// It returns "vX.Y" for all release versions, and all prerelease versions >= "alpha.1".
// X and Y are the major and minor, respectively, of the version specified in version.txt.
// For all other prerelease versions, it returns "dev".
// N.B. new public-facing doc URLs are expected to be up beginning with the "alpha.1" prerelease. Otherwise, "dev" will
// cause the url mapper to redirect to the latest stable release.
func VersionForURLs() string {
	// Prerelease versions >= "alpha.1"
	if parsedVersionTxt.PreRelease() >= "alpha.1" {
		return fmt.Sprintf("v%d.%d", parsedVersionTxt.Major(), parsedVersionTxt.Minor())
	}
	// Production release versions
	if parsedVersionTxt.PreRelease() == "" {
		return fmt.Sprintf("v%d.%d", parsedVersionTxt.Major(), parsedVersionTxt.Minor())
	}
	return "dev"
}

// BranchReleaseSeries returns tha major and minor in version.txt, without
// allowing for any overrides.
func BranchReleaseSeries() (major, minor int) {
	return parsedVersionTxt.Major(), parsedVersionTxt.Minor()
}

func init() {
	// Allow tests to override the version.txt contents.
	if versionOverride := envutil.EnvOrDefaultString(
		"COCKROACH_TESTING_VERSION_OVERRIDE", ""); versionOverride != "" {
		TestingOverrideVersion(versionOverride)
	}
}

// Short returns a pretty printed build and version summary.
func (b Info) Short() redact.RedactableString {
	plat := b.Platform
	if b.CgoTargetTriple != "" {
		plat = b.CgoTargetTriple
	}
	return redact.Sprintf("CockroachDB %s %s (%s, built %s, %s)",
		redact.SafeString(b.Distribution),
		redact.SafeString(b.Tag),
		redact.SafeString(plat),
		redact.SafeString(b.Time),
		redact.SafeString(b.GoVersion),
	)
}

// Long returns a pretty printed build summary
func (b Info) Long() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	fmt.Fprintf(tw, "Build Tag:        %s\n", b.Tag)
	fmt.Fprintf(tw, "Build Time:       %s\n", b.Time)
	fmt.Fprintf(tw, "Distribution:     %s\n", b.Distribution)
	fmt.Fprintf(tw, "Platform:         %s", b.Platform)
	if b.CgoTargetTriple != "" {
		fmt.Fprintf(tw, " (%s)", b.CgoTargetTriple)
	}
	fmt.Fprintln(tw)
	fmt.Fprintf(tw, "Go Version:       %s\n", b.GoVersion)
	fmt.Fprintf(tw, "C Compiler:       %s\n", b.CgoCompiler)
	fmt.Fprintf(tw, "Build Commit ID:  %s\n", b.Revision)
	fmt.Fprintf(tw, "Build Type:       %s\n", b.Type)
	fmt.Fprintf(tw, "Enabled Assertions: %t", b.EnabledAssertions) // No final newline: cobra prints one for us.
	_ = tw.Flush()
	return buf.String()
}

// GoTime parses the utcTime string and returns a time.Time.
func (b Info) GoTime() time.Time {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return time.Time{}
	}
	return val
}

// Timestamp parses the utcTime string and returns the number of seconds since epoch.
func (b Info) Timestamp() (int64, error) {
	val, err := time.Parse(TimeFormat, b.Time)
	if err != nil {
		return 0, err
	}
	return val.Unix(), nil
}

// GetInfo returns an Info struct populated with the build information.
func GetInfo() Info {
	ch := channel
	if ch == "" {
		ch = "unknown"
	}
	return Info{
		GoVersion:         runtime.Version(),
		Tag:               BinaryVersion(),
		Time:              utcTime,
		Revision:          rev,
		CgoCompiler:       cgoCompiler,
		CgoTargetTriple:   cgoTargetTriple,
		Platform:          platform,
		Distribution:      Distribution,
		Type:              typ,
		Channel:           ch,
		EnvChannel:        envChannel,
		EnabledAssertions: enabledAssertions,
	}
}

// TestingOverrideVersion allows tests to override the binary version
// reported by cockroach.
func TestingOverrideVersion(v string) func() {
	prevOverride := binaryVersionTestingOverride
	binaryVersionTestingOverride = v
	return func() { binaryVersionTestingOverride = prevOverride }
}

// MakeIssueURL produces a URL to a CockroachDB issue.
func MakeIssueURL(issue int) string {
	return fmt.Sprintf("https://go.crdb.dev/issue-v/%d/%s", issue, VersionForURLs())
}
