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

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
)

var (
	buildIDs = map[string]string{
		"linux-amd64":   "Cockroach_ScratchProjectPutTcExperimentsInHere_BazelBuild",
		"linux-arm64":   "Cockroach_UnitTests_BazelBuildLinuxArmCross",
		"darwin-amd64":  "Cockroach_UnitTests_BazelBuildMacOSCross",
		"darwin-arm64":  "Cockroach_Ci_Builds_BuildMacOSArm64",
		"windows-amd64": "Cockroach_UnitTests_BazelBuildWindowsCross",
	}
	buildType = buildIDs[runtime.GOOS+"-"+runtime.GOARCH]
	apiBase   = "https://teamcity.cockroachdb.com/guestAuth/app/rest"
)

func downloadURL(buildId int32) string {
	url := fmt.Sprintf("%s%s",
		apiBase,
		fmt.Sprintf("/builds/id:%v/artifacts/content/bazel-bin/pkg/cmd/roachprod/roachprod_/roachprod", buildId),
	)
	fmt.Println(url)
	return url
}

// DownloadLatestRoadprod attempts to download the latest binary to the
// current binary's directory. It returns the path to the downloaded binary.
func DownloadLatestRoadprod() (string, error) {
	if buildType == "" {
		panic("unable to find build type for this platform")
	}
	builds, err := GetBuilds("count:1,status:SUCCESS,branch:master,buildType:" + buildType)
	if err != nil {
		return "", err
	}

	if len(builds.Build) == 0 {
		return "", fmt.Errorf("no builds found")
	}

	currentRoachprod, err := os.Executable()
	if err != nil {
		return "", err
	}

	newRoachprod := currentRoachprod + ".new"
	err = DownloadRoachprod(builds.Build[0], newRoachprod)
	if err != nil {
		return "", err
	}
	fmt.Printf("Latest roachprod downloaded to %s\n", newRoachprod)
	return newRoachprod, nil
}

func GetBuilds(locator string) (TCBuildResponse, error) {
	// Get the latest successful build
	urlWithLocator := fmt.Sprintf("%s/builds?locator=%s", apiBase, locator)
	fmt.Println("URL: ", urlWithLocator)

	buildResp := &TCBuildResponse{}
	err := httputil.GetJSONWithOptions(*httputil.DefaultClient.Client, urlWithLocator, buildResp, httputil.IgnoreUnknownFields())

	return *buildResp, err
}

// DownloadRoachprod downloads the roachprod binary from the build
// to the specified destination file.
func DownloadRoachprod(build *TCBuild, destFile string) error {
	out, err := os.Create(destFile)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := httputil.Get(context.Background(), downloadURL(build.Id))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad status downloading roachprod: %s", resp.Status)
	}
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
