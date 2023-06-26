// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

const (
	edgeBinaryServer    = "https://storage.googleapis.com/cockroach-edge-artifacts-prod/"
	releaseBinaryServer = "https://storage.googleapis.com/cockroach-release-artifacts-prod/"
)

type archInfo struct {
	// DebugArchitecture is the "target triple" string for debug
	// builds on the given architecture.
	DebugArchitecture string
	// ReleaseArchitecture is the "target triple" string for debug
	// builds on the given architecture.
	ReleaseArchitecture string
	// LibraryExtension is the extensions that dynamic libraries
	// have on the given architecture.
	LibraryExtension string
	// ExecutableExtension is the extensions that executable files
	// have on the given architecture.
	ExecutableExtension string
	// ReleaseArchiveExtension is the extensions that release archive files
	// have on the given architecture.
	ReleaseArchiveExtension string
}

// N.B. DebugArchitecture must correspond to 'SuffixFromPlatform' followed by stripping the literal (os) version,
// in 'MakeCRDBBinaryNonReleaseFile' (see pkg/release/build.go and pkg/release/release.go).
//
//	ReleaseArchitecture must correspond to 'SuffixFromPlatform' followed by stripping "gnu-" in 'makeArchiveKeys'
//
// (see pkg/release/upload.go).
// TODO(srosenberg): refactor to use the above, directly from pkg/release/ which is the source of truth.
var (
	linux_x86_64_ArchInfo = archInfo{
		DebugArchitecture:       "linux-gnu-amd64",
		ReleaseArchitecture:     "linux-amd64",
		LibraryExtension:        ".so",
		ExecutableExtension:     "",
		ReleaseArchiveExtension: "tgz",
	}
	linux_x86_64_fips_ArchInfo = archInfo{
		DebugArchitecture:       "linux-gnu-amd64-fips",
		ReleaseArchitecture:     "linux-amd64-fips",
		LibraryExtension:        ".so",
		ExecutableExtension:     "",
		ReleaseArchiveExtension: "tgz",
	}
	linux_arm64_ArchInfo = archInfo{
		DebugArchitecture:       "linux-gnu-arm64",
		ReleaseArchitecture:     "linux-arm64",
		LibraryExtension:        ".so",
		ExecutableExtension:     "",
		ReleaseArchiveExtension: "tgz",
	}
	darwin_x86_64_ArchInfo = archInfo{
		DebugArchitecture:       "darwin-amd64",
		ReleaseArchitecture:     "darwin-10.9-amd64",
		LibraryExtension:        ".dylib",
		ExecutableExtension:     "",
		ReleaseArchiveExtension: "tgz",
	}
	darwin_arm64_ArchInfo = archInfo{
		DebugArchitecture:       "darwin-arm64.unsigned",
		ReleaseArchitecture:     "darwin-11.0-arm64",
		LibraryExtension:        ".dylib",
		ExecutableExtension:     "",
		ReleaseArchiveExtension: "tgz",
	}
	windowsArchInfo = archInfo{
		DebugArchitecture:       "windows-amd64",
		ReleaseArchitecture:     "windows-6.2-amd64",
		LibraryExtension:        ".dll",
		ExecutableExtension:     ".exe",
		ReleaseArchiveExtension: "zip",
	}

	crdbLibraries = []string{"libgeos", "libgeos_c"}
)

// ArchInfoForOS returns an ArchInfo for the given OS and Architecture if currently supported.
func ArchInfoForOS(os string, arch vm.CPUArch) (archInfo, error) {
	if arch != "" && arch != vm.ArchAMD64 && arch != vm.ArchARM64 && arch != vm.ArchFIPS {
		return archInfo{}, errors.Errorf("unsupported architecture %q", arch)
	}

	switch os {
	case "linux":
		if arch == vm.ArchARM64 {
			return linux_arm64_ArchInfo, nil
		}
		if arch == vm.ArchFIPS {
			return linux_x86_64_fips_ArchInfo, nil
		}
		return linux_x86_64_ArchInfo, nil
	case "darwin":
		if arch == vm.ArchARM64 {
			return darwin_arm64_ArchInfo, nil
		}
		if arch == vm.ArchFIPS {
			return archInfo{}, errors.Errorf("%q is not supported on %q", arch, os)
		}
		return darwin_x86_64_ArchInfo, nil
	case "windows":
		if arch == vm.ArchFIPS || arch == vm.ArchARM64 {
			return archInfo{}, errors.Errorf("%q is not supported on %q", arch, os)
		}
		return windowsArchInfo, nil
	default:
		return archInfo{}, errors.Errorf("unsupported OS %q", os)
	}
}

// GetEdgeURL returns a URL from the edge binary archive. If no SHA is
// given, the latest version is returned.
func getEdgeURL(urlPathBase, SHA, arch string, ext string) (*url.URL, error) {
	edgeBinaryLocation, err := url.Parse(edgeBinaryServer)
	if err != nil {
		return nil, err
	}
	edgeBinaryLocation.Path += urlPathBase
	// If a target architecture is provided, attach that.
	if len(arch) > 0 {
		edgeBinaryLocation.Path += "." + arch
	}
	// If a specific SHA is provided, just attach that.
	if len(SHA) > 0 {
		edgeBinaryLocation.Path += "." + SHA + ext
	} else {
		// Special case for windows latest executable. For some reason it has no extension. ¯\_(ツ)_/¯
		if ext == ".exe" {
			edgeBinaryLocation.Path += ".LATEST"
		} else {
			edgeBinaryLocation.Path += ext + ".LATEST"
		}
	}
	return edgeBinaryLocation, nil
}

func cockroachReleaseURL(version string, arch string, archiveExtension string) (*url.URL, error) {
	binURL, err := url.Parse(releaseBinaryServer)
	if err != nil {
		return nil, err
	}
	binURL.Path += fmt.Sprintf("cockroach-%s.%s.%s", version, arch, archiveExtension)
	return binURL, nil
}

// StageApplication downloads the appropriate artifact for the given
// application name into the specified directory on each node in the
// cluster. If no version is specified, the latest artifact is used if
// available.
func StageApplication(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	applicationName string,
	version string,
	os string,
	arch vm.CPUArch,
	destDir string,
) error {
	archInfo, err := ArchInfoForOS(os, arch)
	if err != nil {
		return err
	}

	switch applicationName {
	case "cockroach":
		err := stageRemoteBinary(
			ctx, l, c, applicationName, "cockroach/cockroach", version, archInfo.DebugArchitecture, destDir,
		)
		if err != nil {
			return err
		}
		// NOTE: libraries may not be present in older versions.
		// Use the sha for the binary to download the same remote library.
		for _, library := range crdbLibraries {
			if err := StageOptionalRemoteLibrary(
				ctx,
				l,
				c,
				library,
				fmt.Sprintf("cockroach/lib/%s", library),
				version,
				archInfo.DebugArchitecture,
				archInfo.LibraryExtension,
				destDir,
			); err != nil {
				return err
			}
		}
		return nil
	case "workload":
		// N.B. workload binary is only available for linux amd64: https://github.com/cockroachdb/cockroach/issues/103563
		err := stageRemoteBinary(
			ctx, l, c, applicationName, "cockroach/workload", version, "" /* arch */, destDir,
		)
		return err
	case "release":
		return StageCockroachRelease(ctx, l, c, version, archInfo.ReleaseArchitecture, archInfo.ReleaseArchiveExtension, destDir)
	default:
		return fmt.Errorf("unknown application %s", applicationName)
	}
}

// URLsForApplication returns a slice of URLs that should be
// downloaded for the given application.
func URLsForApplication(
	application string, version string, os string, arch vm.CPUArch,
) ([]*url.URL, error) {
	archInfo, err := ArchInfoForOS(os, arch)
	if err != nil {
		return nil, err
	}

	switch application {
	case "cockroach":
		u, err := getEdgeURL("cockroach/cockroach", version, archInfo.DebugArchitecture, archInfo.ExecutableExtension)
		if err != nil {
			return nil, err
		}

		urls := []*url.URL{u}
		for _, library := range crdbLibraries {
			u, err := getEdgeURL(
				fmt.Sprintf("cockroach/lib/%s", library),
				version,
				archInfo.DebugArchitecture,
				archInfo.LibraryExtension,
			)
			if err != nil {
				return nil, err
			}
			urls = append(urls, u)
		}
		return urls, nil
	case "workload":
		// N.B. workload binary is only available for linux amd64: https://github.com/cockroachdb/cockroach/issues/103563
		u, err := getEdgeURL("cockroach/workload", version, "" /* arch */, "" /* extension */)
		if err != nil {
			return nil, err
		}
		return []*url.URL{u}, nil
	case "release":
		u, err := cockroachReleaseURL(version, archInfo.ReleaseArchitecture, archInfo.ReleaseArchiveExtension)
		if err != nil {
			return nil, err
		}
		return []*url.URL{u}, nil
	default:
		return nil, fmt.Errorf("unknown application %s", application)
	}
}

// StageRemoteBinary downloads a cockroach edge binary with the provided
// application path to each specified by the cluster to the specified directory.
// If no SHA is specified, the latest build of the binary is used instead.
// Returns the SHA of the resolved binary.
func stageRemoteBinary(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	applicationName, urlPathBase, SHA, arch, dir string,
) error {
	binURL, err := getEdgeURL(urlPathBase, SHA, arch, "")
	if err != nil {
		return err
	}
	l.Printf("Resolved binary url for %s: %s", applicationName, binURL)
	target := filepath.Join(dir, applicationName)
	cmdStr := fmt.Sprintf(
		`curl -sfSL -o "%s" "%s" && chmod 755 %s`, target, binURL, target,
	)
	return c.Run(
		ctx, l, l.Stdout, l.Stderr, c.Nodes, fmt.Sprintf("staging binary (%s)", applicationName), cmdStr,
	)
}

// StageOptionalRemoteLibrary downloads a library from the cockroach edge with
// the provided application path to each specified by the cluster to <dir>/lib.
// If no SHA is specified, the latest build of the library is used instead.
// It will not error if the library does not exist on the edge.
func StageOptionalRemoteLibrary(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	libraryName, urlPathBase, SHA, arch, ext, dir string,
) error {
	url, err := getEdgeURL(urlPathBase, SHA, arch, ext)
	if err != nil {
		return err
	}
	libDir := filepath.Join(dir, "lib")
	target := filepath.Join(libDir, libraryName+ext)
	l.Printf("Resolved library url for %s: %s", libraryName, url)
	cmdStr := fmt.Sprintf(
		`mkdir -p "%s" && \
curl -sfSL -o "%s" "%s" 2>/dev/null || echo 'optional library %s not found; continuing...'`,
		libDir,
		target,
		url,
		libraryName+ext,
	)
	return c.Run(
		ctx, l, l.Stdout, l.Stderr, c.Nodes, fmt.Sprintf("staging library (%s)", libraryName), cmdStr,
	)
}

// StageCockroachRelease downloads an official CockroachDB release binary with
// the specified version.
func StageCockroachRelease(
	ctx context.Context,
	l *logger.Logger,
	c *SyncedCluster,
	version, arch, archiveExtension, dir string,
) error {
	if len(version) == 0 {
		return fmt.Errorf(
			"release application cannot be staged without specifying a specific version",
		)
	}

	binURL, err := cockroachReleaseURL(version, arch, archiveExtension)
	if err != nil {
		return err
	}
	l.Printf("Resolved release url for cockroach version %s: %s", version, binURL)

	// This command incantation:
	// - Creates a temporary directory on the remote machine
	// - Downloads and unpacks the cockroach release into the temp directory
	// - Moves the cockroach executable from the binary to the provided directory
	//   and gives it the correct permissions.
	cmdStr := fmt.Sprintf(`
tmpdir="$(mktemp -d /tmp/cockroach-release.XXX)" && \
dir=%s && \
curl -f -s -S -o- %s | tar xfz - -C "${tmpdir}" --strip-components 1 && \
mv ${tmpdir}/cockroach ${dir}/cockroach && \
mkdir -p ${dir}/lib && \
if [ -d ${tmpdir}/lib ]; then mv ${tmpdir}/lib/* ${dir}/lib; fi && \
chmod 755 ${dir}/cockroach
`, dir, binURL)
	return c.Run(
		ctx, l, l.Stdout, l.Stderr, c.Nodes, "staging cockroach release binary", cmdStr,
	)
}
