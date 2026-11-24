// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func Test_buildClusterPProfInfo(t *testing.T) {
	clusterToPProfInfo := make(map[install.Node][]*pprofInfo)
	buildClusterPProfInfo(strings.Split(getListClusterMockResponse(1), "\n"), install.Node(1), clusterToPProfInfo)
	buildClusterPProfInfo(strings.Split(getListClusterMockResponse(2), "\n"), install.Node(2), clusterToPProfInfo)
	require.Equal(t, 2, len(clusterToPProfInfo))
	pprofRegExp := regexp.MustCompile(`cpuprof\.(.*)\.pprof`)
	for node, pprofInfos := range clusterToPProfInfo {
		nodeNumber := int(node)
		// fileCount represents the file index as returned by getListClusterMockResponse
		fileCount := 0
		for _, pi := range pprofInfos {
			// fileDate is using the same logic as used in getListClusterMockResponse for calculating the date
			fileDate := nodeNumber + 20 - fileCount*2
			require.Equal(t, fmt.Sprintf("cpuprof.2024-09-%dT10_05_22.234.69.pprof", fileDate),
				pi.filename)
			pprofFileMatch := pprofRegExp.FindStringSubmatch(pi.filename)
			require.Equal(t, 2, len(pprofFileMatch))
			ct, err := time.Parse("2006-01-02T15:04:05",
				strings.ReplaceAll(strings.Split(pprofFileMatch[1], ".")[0], "_", ":"))
			require.Nil(t, err)
			require.Equal(t, ct, pi.createdTime)
			fileCount++
			if fileCount == 1 {
				// the second last file in the list returned is invalid. Since the files get reverse sorted by time, the invalid
				// file entry becomes the second entry
				fileCount++
			}
		}
	}
}

func Test_getLatestPProfInfoBefore(t *testing.T) {
	timeNow := time.Now()
	fileInfoMap := map[install.Node][]*pprofInfo{
		1: {
			{createdTime: timeNow.Add(-10)},
			{createdTime: timeNow.Add(-20)},
			{createdTime: timeNow.Add(-30)},
		},
		2: {
			{createdTime: timeNow.Add(-15)},
			{createdTime: timeNow.Add(-25)},
			{createdTime: timeNow.Add(-28)},
		},
		3: {
			{createdTime: timeNow.Add(-5)},
			{createdTime: timeNow.Add(-11)},
			{createdTime: timeNow.Add(-15)},
		},
	}
	require.Nil(t, getLatestPProfInfo(fileInfoMap, timeNow.Add(-31)))
	require.Equal(t, fileInfoMap[3][1], getLatestPProfInfo(fileInfoMap, timeNow.Add(-11)))
	require.Equal(t, fileInfoMap[3][0], getLatestPProfInfo(fileInfoMap, time.Now()))
	require.Equal(t, fileInfoMap[1][0], getLatestPProfInfo(fileInfoMap, timeNow.Add(-6)))
	require.Equal(t, fileInfoMap[2][2], getLatestPProfInfo(fileInfoMap, timeNow.Add(-26)))
}

// getListClusterMockResponse returns the mocked data for a node. The node number decides the dates.
// e.g. if n=1, it returns 6 records with dates:
// Sept 11, 13, 15, 17, 19, 21
// Similarly for n=2, the 6 records are:
// Sept 12, 14, 16, 18, 20, 22
// Records on day n+18 is invalid.
func getListClusterMockResponse(n int) string {
	return fmt.Sprintf(`-rw-r----- 1 ubuntu ubuntu 219534 Sep %d 10:05 logs/pprof_dump/cpuprof.2024-09-%dT10_05_22.234.69.pprof
-rw-r----- 1 ubuntu ubuntu 211461 Sep %d 10:05 logs/pprof_dump/cpuprof.2024-09-%dT10_05_22.234.69.pprof
-rw-r----- 1 ubuntu ubuntu 207410 Sep %d 10:05 logs/pprof_dump/cpuprof.2024-09-%dT10_05_22.234.69.pprof
-rw-r----- 1 ubuntu ubuntu 248365 Sep %d 10:06 logs/pprof_dump/cpuprof.2024-09-%dT10_05_22.234.69.pprof
-rw-r----- 1 ubuntu ubuntu 248365 Sep %d 10:06 logs/pprof_dump/cpuprof.2024-09-Invalid_06_%d.042.82.pprof
-rw-r----- 1 ubuntu ubuntu 231194 Sep %d 10:07 logs/pprof_dump/cpuprof.2024-09-%dT10_05_22.234.69.pprof`,
		n+10, n+10, n+12, n+12, n+14, n+14, n+16, n+16, n+18, n+18, n+20, n+20)
}
