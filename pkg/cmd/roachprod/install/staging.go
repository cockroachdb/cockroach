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

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

const (
	edgeBinaryServer    = "https://edge-binaries.cockroachdb.com"
	releaseBinaryServer = "https://s3.amazonaws.com/binaries.cockroachdb.com/"
)

func getEdgeBinaryURL(binaryName, SHA, arch string) (*url.URL, error) {
	edgeBinaryLocation, err := url.Parse(edgeBinaryServer)
	if err != nil {
		return nil, err
	}
	edgeBinaryLocation.Path = binaryName
	// If a target architecture is provided, attach that.
	if len(arch) > 0 {
		edgeBinaryLocation.Path += "." + arch
	}
	// If a specific SHA is provided, just attach that.
	if len(SHA) > 0 {
		edgeBinaryLocation.Path += "." + SHA
	} else {
		edgeBinaryLocation.Path += ".LATEST"
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
// application path to each specified by the cluster. If no SHA is specified,
// the latest build of the binary is used instead.
func StageRemoteBinary(c *SyncedCluster, applicationName, binaryPath, SHA, arch string) error {
	binURL, err := getEdgeBinaryURL(binaryPath, SHA, arch)
	if err != nil {
		return err
	}
	fmt.Printf("Resolved binary url for %s: %s\n", applicationName, binURL)
	cmdStr := fmt.Sprintf(
		`curl -sfSL -o %s "%s" && chmod 755 ./%s`, applicationName, binURL, applicationName,
	)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, OtherCmd,
		fmt.Sprintf("staging binary (%s)", applicationName), cmdStr,
	)
}

// StageCockroachRelease downloads an official CockroachDB release binary with
// the specified version.
func StageCockroachRelease(c *SyncedCluster, version, arch string) error {
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
	// - Moves the cockroach executable from the binary to '/.' and gives it
	// the correct permissions.
	cmdStr := fmt.Sprintf(`
tmpdir="$(mktemp -d /tmp/cockroach-release.XXX)" && \
curl -f -s -S -o- %s | tar xfz - -C "${tmpdir}" --strip-components 1 && \
mv ${tmpdir}/cockroach ./cockroach && \
chmod 755 ./cockroach
`, binURL)
	return c.Run(
		os.Stdout, os.Stderr, c.Nodes, OtherCmd, "staging cockroach release binary", cmdStr,
	)
}
