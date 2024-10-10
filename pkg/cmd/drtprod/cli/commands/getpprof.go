// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commands

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/spf13/cobra"
)

var (
	// singleClusterNodeRegex is the regex to identify if the cluster name input provided is for a single node or not.
	// this extracts the node number. e.g.
	// drt-chaos, drt-chaos:1-3, drt-chaos:1,2,4 - does not match
	// drt-chaos:3 matches and extracts 3
	singleClusterNodeRegex = regexp.MustCompile(`^.+:(\d+)$`)
	// pprofFileListRegex extracts the ppprof file name of the "ls -ltr" console output. It also extracts the date
	// and time from the file name. e.g.
	// 	-rw-r----- 1 ubuntu ubuntu 221508 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_19.067.67.pprof
	// extracts cpuprof.2024-09-27T10_05_19.067.67.pprof and 2024-09-27T10_05_19.067.67
	pprofFileListRegex = regexp.MustCompile(`.+ logs/pprof_dump/(cpuprof\.(.*)\.pprof)$`)
	// clusterNodeNumberRegex extracts the node number of the cluster from the "roachprod get" command. e.g.
	//    6: 	<ok>
	// extracts 6
	clusterNodeNumberRegex = regexp.MustCompile(`(\d+):[ \t]+<ok>$`)
	// pprofCommandExecutor is responsible for executing the shell commands
	pprofCommandExecutor = helpers.ExecuteCmdWithReturn
)

// GetPProfCommand creates a new Cobra command for getting the pprof file from a cluster in the mentioned time
// window. If no time window is provided the latest file across all cluster is downloaded.
func GetPProfCommand(ctx context.Context) *cobra.Command {
	var t *string
	cobraCmd := &cobra.Command{
		Use:   "getpprof <cluster> [flags]",
		Short: "downloads the latest pprof file before the provided time.",
		Long: `Downloads the latest pprof file before the provided time.
The time should be of the format 2022-08-31T15:23:22Z for UTC or 2022-08-31T15:23:22+05:30 for time zone.
If the time is not provided, it downloads the latest pprof file across all clusters.
`,
		Args: cobra.ExactArgs(1),
		// Wraps the command execution with additional error handling
		Run: helpers.Wrap(func(cmd *cobra.Command, args []string) (retErr error) {
			return downloadFile(ctx, args[0], *t)
		}),
	}
	t = cobraCmd.Flags().StringP("time_before", "t", "",
		"the time before which the pprof file should be considered")
	return cobraCmd
}

// pprofInfo maintains the information of a single pprof file in a cluster
type pprofInfo struct {
	clusterNodeNumber int64
	filename          string
	createdTime       time.Time
}

// downloadFile downloads the latest pprof file before beforeTime across all cluster nodes
// if the beforeTime is not set, it defaults to the current time
func downloadFile(ctx context.Context, clusterName string, beforeTimeArg string) (err error) {
	beforeTime := time.Now()
	if beforeTimeArg != "" {
		beforeTime, err = time.Parse(time.RFC3339, beforeTimeArg)
		if err != nil {
			return err
		}
	}
	clusterToPprofInfo, err := getClusterToPprofInfoMap(ctx, clusterName)
	if err != nil {
		return err
	}

	latestPprof := getLatestPprofInfo(clusterToPprofInfo, beforeTime)

	if latestPprof != nil {
		// eliminate any suffixed node number
		cn := strings.Split(clusterName, ":")[0]
		// download the file if found
		args := []string{
			"get", fmt.Sprintf("%s:%d", cn, latestPprof.clusterNodeNumber),
			fmt.Sprintf("logs/pprof_dump/%s", latestPprof.filename),
			// the downloaded file name has the cluster name and node number prefixed
			fmt.Sprintf("%s_%d_%s", cn, latestPprof.clusterNodeNumber, latestPprof.filename),
		}
		_, _, err = pprofCommandExecutor(ctx, "roachprod", args...)
		if err != nil {
			return err
		}
	} else {
		fmt.Println("No file found to download.")
	}
	return nil
}

// getLatestPprofInfo gets the latest pprofInfo across all clusters before beforeTime
// it returns nil if no pprof were found.
func getLatestPprofInfo(clusterToPprofInfo map[int][]*pprofInfo, beforeTime time.Time) *pprofInfo {
	var latestPprof *pprofInfo
	allEligiblePprof := make([]*pprofInfo, 0)
	for _, pprofInfos := range clusterToPprofInfo {
		// data is sorted. so, binary search works
		info := getLatestPprofInfoBefore(pprofInfos, beforeTime)
		if info != nil {
			allEligiblePprof = append(allEligiblePprof, info)
		}
	}
	// check for the length as we may not find any file with the given criteria
	if len(allEligiblePprof) > 0 {
		latestPprof = allEligiblePprof[0]
		for i := 1; i < len(allEligiblePprof); i++ {
			if latestPprof.createdTime.Before(allEligiblePprof[i].createdTime) {
				latestPprof = allEligiblePprof[i]
			}
		}
	}
	return latestPprof
}

// getClusterToPprofInfoMap run "ls -ltr logs/pprof_dump" on the cluster and creates a map where
// the key is the node number and value is a list of pprofInfo. e.g.
// Consider the following console output:
// drt-chaos:[1 2 3 4 5 6]: ls -ltr logs/pprof_dump....
//
//	  1: 	<ok>
//		total 1100
//		-rw-r----- 1 ubuntu ubuntu 207410 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_42.752.79.pprof
//		-rw-r----- 1 ubuntu ubuntu 248365 Sep 27 10:06 logs/pprof_dump/cpuprof.2024-09-27T10_06_33.042.82.pprof
//		-rw-r----- 1 ubuntu ubuntu 231194 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof
//
//	  2: 	<ok>
//		total 992
//		-rw-r----- 1 ubuntu ubuntu 255149 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_39.811.84.pprof
//		-rw-r----- 1 ubuntu ubuntu 237958 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_06_50.128.87.pprof
//
// The above forms the following map:
//
//	{
//			1: []*pprofInfo{
//							{clusterNodeNumber: 1, filename: cpuprof.2024-09-27T10_07_33.427.85.pprof, createdTime: 2024-09-27T10:07:33Z},
//							{clusterNodeNumber: 1, filename: cpuprof.2024-09-27T10_06_33.042.82.pprof, createdTime: 2024-09-27T10:06:33Z},
//				},
//			2: []*pprofInfo{
//							{clusterNodeNumber: 2, filename: cpuprof.2024-09-27T10_06_50.128.87.pprof, createdTime: 2024-09-27T10:06:50Z},
//							{clusterNodeNumber: 2, filename: cpuprof.2024-09-27T10_05_39.811.84.pprof, createdTime: 2024-09-27T10:05:39Z},
//				},
//	}
//
// Here 1 amd 2 as key represents the node number and the values represent the files in each node
// Note that the lists per node are reverse sorted.
// It also detects if a single cluster node number is provided in input, in which case the output does not have the
// cluster node number.
func getClusterToPprofInfoMap(
	ctx context.Context, clusterName string,
) (map[int][]*pprofInfo, error) {
	args := []string{
		"ssh", clusterName, "--", "ls", "-ltr", "logs/pprof_dump/cpuprof.*.pprof",
	}
	output, _, err := pprofCommandExecutor(ctx, "roachprod", args...)
	if err != nil {
		return nil, err
	}

	singleClusterMatch := singleClusterNodeRegex.FindStringSubmatch(clusterName)
	// singleClusterNodeNumber represents the node number if provided in the argument. If the value is not provided,
	// it is defaulted to -1
	singleClusterNodeNumber := int64(-1)
	if len(singleClusterMatch) > 1 {
		singleClusterNodeNumber, _ = strconv.ParseInt(singleClusterMatch[1], 10, 64)
	}
	return buildClusterPprofInfo(strings.Split(output, "\n"), singleClusterNodeNumber), nil
}

// getLatestPprofInfoBefore gets the last pprofInfo record before a certain time using binary search
func getLatestPprofInfoBefore(pprofList []*pprofInfo, beforeTime time.Time) *pprofInfo {
	// Binary search to find the first element where createdTime is before the given time
	index := sort.Search(len(pprofList), func(i int) bool {
		// Since the list is reverse sorted, we check the opposite condition
		// 1 is added to select the file with exact matching timestamp
		return pprofList[i].createdTime.Before(beforeTime.Add(1))
	})

	// no files are found
	if index == len(pprofList) {
		return nil
	}
	// Return the pprofInfo at the index
	return pprofList[index]
}

// buildClusterPprofInfo scans through each line and builds the map of cluster node number to the list of files
func buildClusterPprofInfo(
	lines []string, singleClusterNodeNumber int64,
) (clusterToPprofInfo map[int][]*pprofInfo) {
	clusterToPprofInfo = make(map[int][]*pprofInfo)
	// allPprof contains the list of pprofInfo for a node. This list gets reset when the node number changes
	// A "node number changes" means that we found a line with "x: 	<ok>" where x is the new node number.
	// As the lines are read in sequence, the files for a single node always come in one flow.
	allPprof := make([]*pprofInfo, 0)
	var clusterNodeNumber int64
	if singleClusterNodeNumber > -1 {
		// set the cluster number if it is for a single node. The cluster number will not be part of the output
		clusterNodeNumber = singleClusterNodeNumber
	}
	for index := 0; index < len(lines); index++ {
		// extract the line. The lines that are of our interest are:
		// cluster number line -> x: 	<ok>" where x is the new node number
		// pprof file line -> "-rw-r----- 1 ubuntu ubuntu 237958 Sep 27 10:07 logs/pprof_dump/<pprof file name"
		// any other line is ignored
		line := lines[index]
		// check for the cluster number match e.g. 1: 	<ok>
		// this will never match for a single node query.
		clusterNameMatch := clusterNodeNumberRegex.FindStringSubmatch(line)
		if len(clusterNameMatch) > 1 {
			// if there is a match, the pprof file list will be for the next cluster node.
			// so, if there is any data present in allPprof, this is for the previously extracted cluster node.
			if len(allPprof) > 0 {
				// store allPprof against the previous cluster node
				clusterToPprofInfo[int(clusterNodeNumber)] = allPprof
				// clear the allPprof for the new cluster node
				allPprof = make([]*pprofInfo, 0)
			}
			// replace the clusterNodeNumber. From now on allPprof will start storing for this cluster node number
			clusterNodeNumber, _ = strconv.ParseInt(clusterNameMatch[1], 10, 64)
			continue
		}
		// check if the line matches any ls -ltr output of ppprof file
		pprofFileMatch := pprofFileListRegex.FindStringSubmatch(line)
		if len(pprofFileMatch) > 2 {
			// this is a line with a matching pprof file name
			// extract the date from the file name. To make it parsable replace _ with :
			ct, err := time.Parse("2006-01-02T15:04:05",
				strings.ReplaceAll(strings.Split(pprofFileMatch[2], ".")[0], "_", ":"))
			if err != nil {
				fmt.Println("Unsupported file name format:", pprofFileMatch[1], err)
				continue
			}
			// append the new pprofInfo in  the beginning
			allPprof = append([]*pprofInfo{{
				clusterNodeNumber: clusterNodeNumber,
				filename:          pprofFileMatch[1],
				createdTime:       ct,
			}}, allPprof...)
		}
		// if the line does not match any of the above, continue to the next line
	}
	// once all the lines are read, we reach here. if allPprof has any data, this is for the last node number
	if len(allPprof) > 0 {
		clusterToPprofInfo[int(clusterNodeNumber)] = allPprof
	}
	return clusterToPprofInfo
}
