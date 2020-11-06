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
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

const (
	edgeBinaryServer    = "https://edge-binaries.cockroachdb.com"
	releaseBinaryServer = "https://s3.amazonaws.com/binaries.cockroachdb.com/"
)

func getEdgeURL(urlPathBase, SHA, arch string, ext string) (*url.URL, error) {
	edgeBinaryLocation, err := url.Parse(edgeBinaryServer)
	if err != nil {
		return nil, err
	}
	edgeBinaryLocation.Path = urlPathBase
	// If a target architecture is provided, attach that.
	if len(arch) > 0 {
		edgeBinaryLocation.Path += "." + arch
	}
	// If a specific SHA is provided, just attach that.
	if len(SHA) > 0 {
		edgeBinaryLocation.Path += "." + SHA + ext
	} else {
		edgeBinaryLocation.Path += ext + ".LATEST"
		// Otherwise, find the latest SHA binary available. This works because
		// "[executable].LATEST" redirects to the latest SHA.
		resp, err := httputil.Head(context.TODO(), edgeBinaryLocation.String())
		if err != nil {
			return nil, err
		}
		edgeBinaryLocation = resp.Request.URL
	}

	return edgeBinaryLocation, nil
}

// StageRemoteBinary downloads a cockroach edge binary with the provided
// application path to each specified by the cluster to the specified directory.
// If no SHA is specified, the latest build of the binary is used instead.
// Returns the SHA of the resolved binary.
func StageRemoteBinary(
	c *SyncedCluster, applicationName, urlPathBase, SHA, arch, dir string,
) (string, error) {
	binURL, err := getEdgeURL(urlPathBase, SHA, arch, "")
	if err != nil {
		return "", err
	}
	fmt.Printf("Resolved binary url for %s: %s\n", applicationName, binURL)
	urlSplit := strings.Split(binURL.Path, ".")
	target := filepath.Join(dir, applicationName)
	cmdStr := fmt.Sprintf(
		`curl -sfSL -o "%s" "%s" && chmod 755 %s`, target, binURL, target,
	)
	return urlSplit[len(urlSplit)-1], c.Run(
		os.Stdout, os.Stderr, c.Nodes, fmt.Sprintf("staging binary (%s)", applicationName), cmdStr,
	)
}

// StageOptionalRemoteLibrary downloads a library from the cockroach edge with
// the provided application path to each specified by the cluster to <dir>/lib.
// If no SHA is specified, the latest build of the library is used instead.
// It will not error if the library does not exist on the edge.
func StageOptionalRemoteLibrary(
	c *SyncedCluster, libraryName, urlPathBase, SHA, arch, ext, dir string,
) error {
	url, err := getEdgeURL(urlPathBase, SHA, arch, ext)
	if err != nil {
		return err
	}
	libDir := filepath.Join(dir, "lib")
	target := filepath.Join(libDir, libraryName+ext)
	fmt.Printf("Resolved library url for %s: %s\n", libraryName, url)
	cmdStr := fmt.Sprintf(
		`mkdir -p "%s" && \
curl -sfSL -o "%s" "%s" 2>/dev/null || echo 'optional library %s not found; continuing...'`,
		libDir,
		target,
		url,
		libraryName+ext,
	)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, fmt.Sprintf("staging library (%s)", libraryName), cmdStr,
	)
}

// StageCockroachRelease downloads an official CockroachDB release binary with
// the specified version.
func StageCockroachRelease(c *SyncedCluster, version, arch, dir string) error {
	if len(version) == 0 {
		return fmt.Errorf(
			"release application cannot be staged without specifying a specific version",
		)
	}
	binURL, err := url.Parse(releaseBinaryServer)
	if err != nil {
		return err
	}
	binURL.Path += fmt.Sprintf("cockroach-%s.%s.tgz", version, arch)
	fmt.Printf("Resolved release url for cockroach version %s: %s\n", version, binURL)

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
		os.Stdout, os.Stderr, c.Nodes, "staging cockroach release binary", cmdStr,
	)
}
