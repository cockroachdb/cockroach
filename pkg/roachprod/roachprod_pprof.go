// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var (
	// pprofFileListRegex extracts the ppprof file name of the "ls -ltr" console output. It also extracts the date
	// and time from the file name. e.g.
	// 	-rw-r----- 1 ubuntu ubuntu 221508 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_19.067.67.pprof
	// extracts cpuprof.2024-09-27T10_05_19.067.67.pprof and 2024-09-27T10_05_19.067.67
	pprofFileListRegex = regexp.MustCompile(`.+ logs/pprof_dump/(cpuprof\.(.*)\.pprof)$`)
)

// pprofInfo maintains the information of a single pprof file in a cluster
type pprofInfo struct {
	node        install.Node
	filename    string
	createdTime time.Time
}

// DownloadLatestPProfFile downloads the latest pprof file before beforeTime across all cluster nodes
// if the beforeTime is not set, it defaults to the current time
func DownloadLatestPProfFile(
	ctx context.Context, l *logger.Logger, clusterName string, beforeTimeArg string,
) (err error) {
	beforeTime := timeutil.Now()
	if beforeTimeArg != "" {
		beforeTime, err = time.Parse(time.RFC3339, beforeTimeArg)
		if err != nil {
			return err
		}
	}
	c, err := GetClusterFromCache(l, clusterName)
	if err != nil {
		return err
	}

	nodePProfInfo, err := getClusterToPProfInfoMap(ctx, l, c)
	if err != nil {
		return err
	}

	latestPProf := getLatestPProfInfo(nodePProfInfo, beforeTime)

	if latestPProf != nil {
		// download the file if found
		err = c.Get(ctx, l, install.Nodes{latestPProf.node},
			fmt.Sprintf("logs/pprof_dump/%s", latestPProf.filename),
			// the downloaded file name has the cluster name and node number prefixed
			fmt.Sprintf("%s_%d_%s", c.Name, latestPProf.node, latestPProf.filename),
		)
		if err != nil {
			return err
		}
	} else {
		fmt.Println("No file found to download.")
	}
	return nil
}

// getLatestPProfInfo gets the latest pprofInfo across all clusters before beforeTime
// it returns nil if no pprof were found.
func getLatestPProfInfo(
	clusterToPProfInfo map[install.Node][]*pprofInfo, beforeTime time.Time,
) *pprofInfo {
	var latestPProf *pprofInfo
	allEligiblePProf := make([]*pprofInfo, 0)
	for _, pprofInfos := range clusterToPProfInfo {
		// data is sorted. so, binary search works
		info := getLatestPProfInfoBefore(pprofInfos, beforeTime)
		if info != nil {
			allEligiblePProf = append(allEligiblePProf, info)
		}
	}
	// check for the length as we may not find any file with the given criteria
	if len(allEligiblePProf) > 0 {
		latestPProf = allEligiblePProf[0]
		for i := 1; i < len(allEligiblePProf); i++ {
			if latestPProf.createdTime.Before(allEligiblePProf[i].createdTime) {
				latestPProf = allEligiblePProf[i]
			}
		}
	}
	return latestPProf
}

// getClusterToPProfInfoMap run "ls -ltr logs/pprof_dump" on the cluster and creates a map where
// the key is the node number and value is a list of pprofInfo. e.g.
// Consider the following console output for different nodes:
// Node 1 output:
//
//	-rw-r----- 1 ubuntu ubuntu 207410 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_42.752.79.pprof
//	-rw-r----- 1 ubuntu ubuntu 248365 Sep 27 10:06 logs/pprof_dump/cpuprof.2024-09-27T10_06_33.042.82.pprof
//	-rw-r----- 1 ubuntu ubuntu 231194 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_07_33.427.85.pprof
//
// Node 2 output:
//
//	-rw-r----- 1 ubuntu ubuntu 255149 Sep 27 10:05 logs/pprof_dump/cpuprof.2024-09-27T10_05_39.811.84.pprof
//	-rw-r----- 1 ubuntu ubuntu 237958 Sep 27 10:07 logs/pprof_dump/cpuprof.2024-09-27T10_06_50.128.87.pprof
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
func getClusterToPProfInfoMap(
	ctx context.Context, l *logger.Logger, c *install.SyncedCluster,
) (map[install.Node][]*pprofInfo, error) {
	results, err := c.RunWithDetails(ctx, l, install.WithNodes(c.Nodes), "",
		"ls -ltr logs/pprof_dump/cpuprof.*.pprof")
	if err != nil {
		return nil, err
	}
	clusterToPProfInfo := make(map[install.Node][]*pprofInfo)
	for _, r := range results {
		node := r.Node
		output := r.Stdout
		buildClusterPProfInfo(strings.Split(output, "\n"), node, clusterToPProfInfo)
	}

	return clusterToPProfInfo, nil
}

// getLatestPProfInfoBefore gets the last pprofInfo record before a certain time using binary search
func getLatestPProfInfoBefore(pprofList []*pprofInfo, beforeTime time.Time) *pprofInfo {
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
	// the pprofInfo at the index is returned
	return pprofList[index]
}

// buildClusterPProfInfo scans through each line and builds the map of cluster node to the list of files
func buildClusterPProfInfo(
	lines []string, node install.Node, clusterToPProfInfo map[install.Node][]*pprofInfo,
) {
	// allPProf contains the list of pprofInfo for a node.
	// As the lines are read in sequence, the files for a single node always come in one flow.
	allPProf := make([]*pprofInfo, 0)
	for index := 0; index < len(lines); index++ {
		// One line at a time is read. The lines that are of our interest looks like the following:
		// "-rw-r----- 1 ubuntu ubuntu 237958 Sep 27 10:07 logs/pprof_dump/<pprof file name"
		// any other line is ignored
		line := lines[index]
		// it checks if the line matches any ls -ltr output of ppprof file
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
			allPProf = append([]*pprofInfo{{
				node:        node,
				filename:    pprofFileMatch[1],
				createdTime: ct,
			}}, allPProf...)
		}
		// if the line does not match any of the above, continue to the next line
	}
	// we reach here once all the lines are read. if allPProf has any data, this is for the node
	if len(allPProf) > 0 {
		clusterToPProfInfo[node] = allPProf
	}
}
