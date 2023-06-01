// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrade

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

var (
	buildIDs = map[string]string{
		"linux-amd64":   "Cockroach_Ci_Builds_BuildLinuxX8664",
		"linux-arm64":   "Cockroach_UnitTests_BazelBuildLinuxArmCross",
		"darwin-amd64":  "Cockroach_UnitTests_BazelBuildMacOSCross",
		"darwin-arm64":  "Cockroach_Ci_Builds_BuildMacOSArm64",
		"windows-amd64": "Cockroach_UnitTests_BazelBuildWindowsCross",
	}
	apiBase = "https://teamcity.cockroachdb.com/guestAuth/app/rest"
)

// DownloadLatestRoadprod attempts to download the latest binary to the
// current binary's directory. It returns the path to the downloaded binary.
// toFile is the path to the file to download to.
func DownloadLatestRoadprod(toFile string) error {
	buildType, ok := buildIDs[runtime.GOOS+"-"+runtime.GOARCH]
	if !ok {
		fmt.Println("Supported platforms:")
		for k := range buildIDs {
			fmt.Printf("\t%s\n", k)
		}
		return fmt.Errorf("unable to find build type for this platform")
	}

	// Build are sorted by build date desc, so limiting to 1 will get the latest.
	builds, err := getBuilds("count:1,status:SUCCESS,branch:master,buildType:" + buildType)
	if err != nil {
		return err
	}

	if len(builds.Build) == 0 {
		return fmt.Errorf("no builds found")
	}

	out, err := os.Create(toFile)
	if err != nil {
		return err
	}

	defer out.Close()
	err = downloadRoachprod(builds.Build[0].Id, out)
	if err != nil {
		return err
	}
	fmt.Printf("Downloaded latest roachprod to:\t%s\n", toFile)
	return nil
}

// getBuilds returns a list of builds matching the locator
// See https://www.jetbrains.com/help/teamcity/rest/buildlocator.html
func getBuilds(locator string) (TCBuildResponse, error) {
	urlWithLocator := fmt.Sprintf("%s/builds?locator=%s", apiBase, locator)
	buildResp := &TCBuildResponse{}
	err := httputil.GetJSONWithOptions(*httputil.DefaultClient.Client, urlWithLocator, buildResp,
		httputil.IgnoreUnknownFields())
	return *buildResp, err
}

// downloadRoachprod downloads the roachprod binary from the build
// using the specified writer. It is the caller's responsibility to close the writer.
func downloadRoachprod(buildID int32, destWriter io.Writer) error {
	if buildID <= 0 {
		return fmt.Errorf("invalid build id: %v", buildID)
	}

	url := roachprodDownloadURL(buildID)
	fmt.Printf("Downloading roachprod from:\t%s\n", url)

	// Set a long timeout here because the download can take a while.
	httpClient := httputil.NewClientWithTimeouts(httputil.StandardHTTPTimeout, 10*time.Minute)
	resp, err := httpClient.Get(context.Background(), url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status downloading roachprod: %s", resp.Status)
	}

	_, err = io.Copy(destWriter, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func roachprodDownloadURL(buildID int32) string {
	url := fmt.Sprintf("%s%s",
		apiBase,
		fmt.Sprintf("/builds/id:%v/artifacts/content/bazel-bin/pkg/cmd/roachprod/roachprod_/roachprod", buildID))
	return url
}
