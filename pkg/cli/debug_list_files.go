// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/cobra"
)

var debugListFilesCmd = &cobra.Command{
	Use:   "list-files",
	Short: "list files available for retrieval via 'debug zip'",
	RunE:  MaybeDecorateGRPCError(runDebugListFiles),
}

func runDebugListFiles(cmd *cobra.Command, _ []string) error {
	if err := zipCtx.files.validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the node pointed to in the command line.
	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	status := serverpb.NewStatusClient(conn)

	// Retrieve the details for the head node.
	firstNodeDetails, err := status.Details(ctx, &serverpb.DetailsRequest{NodeId: "local"})
	if err != nil {
		return err
	}

	// Retrieve the list of all nodes.
	nodes, err := status.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		log.Warningf(ctx, "cannot retrieve node list: %v", err)
	}
	// In case nodes came up back empty (the Nodes() RPC failed), we
	// still want to inspect the per-node endpoints on the head
	// node. As per the above, we were able to connect at least to
	// that.
	inputNodeList := []statuspb.NodeStatus{{Desc: roachpb.NodeDescriptor{
		NodeID:     firstNodeDetails.NodeID,
		Address:    firstNodeDetails.Address,
		SQLAddress: firstNodeDetails.SQLAddress,
	}}}
	if nodes != nil {
		// If the nodes were found, use that instead.
		inputNodeList = nodes.Nodes
	}

	// Filter the list of nodes according to configuration.
	var nodeList []roachpb.NodeID
	for _, n := range inputNodeList {
		if zipCtx.nodes.isIncluded(n.Desc.NodeID) {
			nodeList = append(nodeList, n.Desc.NodeID)
		}
	}

	// Determine the list of non-log file types.
	fileTypes := make([]int, 0, len(serverpb.FileType_value))
	for _, v := range serverpb.FileType_value {
		fileTypes = append(fileTypes, int(v))
	}
	sort.Ints(fileTypes)

	// The log files for each node.
	logFiles := make(map[roachpb.NodeID][]logpb.FileInfo)
	// The non-log files on each node. The int32 is the file type.
	otherFiles := make(map[roachpb.NodeID]map[int32][]*serverpb.File)

	// Retrieve the files.
	for _, nodeID := range nodeList {
		nodeIDs := fmt.Sprintf("%d", nodeID)
		nodeLogs, err := status.LogFilesList(ctx, &serverpb.LogFilesListRequest{NodeId: nodeIDs})
		if err != nil {
			log.Warningf(ctx, "cannot retrieve log file list from node %d: %v", nodeID, err)
		} else {
			logFiles[nodeID] = nodeLogs.Files
		}

		otherFiles[nodeID] = make(map[int32][]*serverpb.File)
		for _, fileTypeI := range fileTypes {
			fileType := int32(fileTypeI)
			nodeFiles, err := status.GetFiles(ctx, &serverpb.GetFilesRequest{
				NodeId:   nodeIDs,
				ListOnly: true,
				Type:     serverpb.FileType(fileType),
				Patterns: zipCtx.files.retrievalPatterns(),
			})
			if err != nil {
				log.Warningf(ctx, "cannot retrieve %s file list from node %d: %v", serverpb.FileType_name[fileType], nodeID, err)
			} else {
				otherFiles[nodeID][fileType] = nodeFiles.Files
			}
		}
	}

	// Format the entries and compute the total size.
	fileTypeNames := map[int32]string{}
	for t, n := range serverpb.FileType_name {
		fileTypeNames[t] = strings.ToLower(n)
	}
	var totalSize int64
	fileTableHeaders := []string{"node_id", "type", "file_name", "ctime_utc", "mtime_utc", "size"}
	alignment := "lllr"
	var rows [][]string
	for _, nodeID := range nodeList {
		nodeIDs := fmt.Sprintf("%d", nodeID)
		for _, logFile := range logFiles[nodeID] {
			ctime := extractTimeFromFileName(logFile.Name)
			mtime := timeutil.Unix(0, logFile.ModTimeNanos)
			if !zipCtx.files.isIncluded(logFile.Name, ctime, mtime) {
				continue
			}
			totalSize += logFile.SizeBytes
			ctimes := formatTimeSimple(ctime)
			mtimes := formatTimeSimple(mtime)
			rows = append(rows, []string{nodeIDs, "log", logFile.Name, ctimes, mtimes, fmt.Sprintf("%d", logFile.SizeBytes)})
		}
		for _, ft := range fileTypes {
			fileType := int32(ft)
			for _, other := range otherFiles[nodeID][fileType] {
				ctime := extractTimeFromFileName(other.Name)
				if !zipCtx.files.isIncluded(other.Name, ctime, ctime) {
					continue
				}
				totalSize += other.FileSize
				ctimes := formatTimeSimple(ctime)
				rows = append(rows, []string{nodeIDs, fileTypeNames[fileType], other.Name, ctimes, ctimes, fmt.Sprintf("%d", other.FileSize)})
			}
		}
	}
	// Append the total size.
	rows = append(rows, []string{"", "total", fmt.Sprintf("(%s)", humanizeutil.IBytes(totalSize)), "", "", fmt.Sprintf("%d", totalSize)})

	// Display the file listing.
	return PrintQueryOutput(os.Stdout, fileTableHeaders, NewRowSliceIter(rows, alignment))
}

var tzRe = regexp.MustCompile(`\d\d\d\d-\d\d-\d\dT\d\d_\d\d_\d\d`)

// formatTimeSimple formats a timestamp for use in file lists.
// It simplifies the display to just a date and time.
func formatTimeSimple(t time.Time) string {
	return t.Format("2006-01-02 15:04")
}

// extractTimeFromFileName extracts a timestamp from the name of one of the
// artifacts produced server-side. We use the knowledge that the server
// always embeds the creation timestamp with format YYYY-MM-DDTHH_MM_SS.
func extractTimeFromFileName(f string) time.Time {
	ts := tzRe.FindString(f)
	if ts == "" {
		// No match.
		return time.Time{}
	}
	tm, err := time.ParseInLocation("2006-01-02T15_04_05", ts, time.UTC)
	if err != nil {
		// No match.
		return time.Time{}
	}
	return tm
}
