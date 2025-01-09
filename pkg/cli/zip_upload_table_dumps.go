// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type tsvColumnParserFn func(string) (any, error)

type columnParserMap map[string]tsvColumnParserFn

// clusterWideTableDumps is a map of table dumps and their column parsers.
// Column parsers are required for columns that require special interpretation.
// For example, columns that are protobufs. If the parser is not present for a
// column, it is assumed to be plain text.
var clusterWideTableDumps = map[string]columnParserMap{
	// table dumps with only plain text columns
	"system.namespace.txt":                          {},
	"crdb_internal.kv_node_liveness.txt":            {},
	"crdb_internal.cluster_database_privileges.txt": {},
	"system.rangelog.txt":                           {},
	"crdb_internal.table_indexes.txt":               {},
	"crdb_internal.index_usage_statistics.txt":      {},
	"crdb_internal.create_statements.txt":           {},
	"system.job_info.txt":                           {},
	"crdb_internal.create_schema_statements.txt":    {},
	"crdb_internal.default_privileges.txt":          {},
	"system.role_members.txt":                       {},
	"crdb_internal.cluster_settings.txt":            {},
	"system.role_id_seq.txt":                        {},
	"crdb_internal.cluster_sessions.txt":            {},
	"system.migrations.txt":                         {},
	"crdb_internal.kv_store_status.txt":             {},
	"system.locations.txt":                          {},
	"crdb_internal.cluster_transactions.txt":        {},
	"crdb_internal.kv_node_status.txt":              {},
	"crdb_internal.cluster_contention_events.txt":   {},
	"crdb_internal.cluster_queries.txt":             {},
	"crdb_internal.jobs.txt":                        {},
	"crdb_internal.regions.txt":                     {},
	"system.table_statistics.txt":                   {},

	// table dumps with columns that need to be interpreted as protos
	"crdb_internal.system_jobs.txt": {
		"progress": makeProtoColumnParser[*jobspb.Progress](),
	},
	"system.tenants.txt": {
		"info": makeProtoColumnParser[*mtinfopb.ProtoInfo](),
	},
}

var nodeSpecificTableDumps = map[string]columnParserMap{
	"crdb_internal.node_metrics.txt":                   {},
	"crdb_internal.node_txn_stats.txt":                 {},
	"crdb_internal.node_contention_events.txt":         {},
	"crdb_internal.gossip_liveness.txt":                {},
	"crdb_internal.gossip_nodes.txt":                   {},
	"crdb_internal.node_runtime_info.txt":              {},
	"crdb_internal.node_transaction_statistics.txt":    {},
	"crdb_internal.node_tenant_capabilities_cache.txt": {},
	"crdb_internal.node_sessions.txt":                  {},
	"crdb_internal.node_statement_statistics.txt":      {},
	"crdb_internal.leases.txt":                         {},
	"crdb_internal.node_build_info.txt":                {},
	"crdb_internal.node_memory_monitors.txt":           {},
	"crdb_internal.active_range_feeds.txt":             {},
	"crdb_internal.gossip_alerts.txt":                  {},
	"crdb_internal.node_transactions.txt":              {},
	"crdb_internal.feature_usage.txt":                  {},
	"crdb_internal.node_queries.txt":                   {},
}

// makeProtoColumnParser returns a generic function that can parse a column
// using the given proto type. This function is implemented this way because it
// allows us to effortlessly extend support to new tables without having to
// write a lot of boilerplate code for unmarshalling each column.
func makeProtoColumnParser[T protoutil.Message]() tsvColumnParserFn {
	return func(s string) (any, error) {
		interpretedBytes, ok := interpretString(s)
		if !ok {
			return nil, fmt.Errorf("failed to interpret progress column: %s", s)
		}

		var zeroValue T // dummy var to infer the type of T
		obj := reflect.New(reflect.TypeOf(zeroValue).Elem()).Interface().(T)
		if err := protoutil.Unmarshal(interpretedBytes, obj); err != nil {
			return nil, err
		}

		return obj, nil
	}
}

func processTableDump(
	ctx context.Context, dir, fileName, uploadID string, parsers columnParserMap,
) error {
	f, err := os.Open(path.Join(dir, fileName))
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		lines     = [][]byte{}
		tableName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
		tags      = append(
			[]string{"env:debug", "source:debug-zip"}, makeDDTag(uploadIDTag, uploadID),
			makeDDTag(clusterTag, debugZipUploadOpts.clusterName), makeDDTag(tableTag, tableName),
		)
	)

	if strings.HasPrefix(fileName, "nodes/") {
		tags = append(tags, makeDDTag(nodeIDTag, strings.Split(fileName, "/")[1]))
	}

	header, iter := makeTableIterator(f)
	if err := iter(func(row string) error {
		cols := strings.Split(row, "\t")
		if len(header) != len(cols) {
			return errors.Newf("the number of headers is not matching the number of columns in the row")
		}

		headerColumnMapping := map[string]any{
			ddTagsTag: strings.Join(tags, ","),
		}
		for i, h := range header {
			if parser, ok := parsers[h]; ok {
				colBytes, err := parser(cols[i])
				if err != nil {
					return err
				}

				headerColumnMapping[h] = colBytes
				continue
			}

			headerColumnMapping[h] = cols[i]
		}

		jsonRow, err := json.Marshal(headerColumnMapping)
		if err != nil {
			return err
		}

		lines = append(lines, jsonRow)
		return nil
	}); err != nil {
		return err
	}

	if len(lines) == 0 {
		return nil
	}

	// datadog's logs API only allows 1000 lines of logs per request. So, split
	// the lines into batches of 1000.
	for i := 0; i < len(lines); i += datadogMaxLogLinesPerReq {
		end := min(i+datadogMaxLogLinesPerReq, len(lines))
		if _, err := uploadLogsToDatadog(
			makeDDMultiLineLogPayload(lines[i:end]), debugZipUploadOpts.ddAPIKey, debugZipUploadOpts.ddSite,
		); err != nil {
			return err
		}
	}

	fmt.Printf("uploaded %s\n", fileName)
	return nil
}

// makeTableIterator returns the headers slice and an iterator
func makeTableIterator(f io.Reader) ([]string, func(func(string) error) error) {
	scanner := bufio.NewScanner(f)
	scanner.Scan() // scan the first line to get the headers

	return strings.Split(scanner.Text(), "\t"), func(fn func(string) error) error {
		for scanner.Scan() {
			if err := fn(scanner.Text()); err != nil {
				return err
			}
		}

		return scanner.Err()
	}
}

func getNodeSpecificTableDumps(debugDirPath string) ([]string, error) {
	allTxtFiles, err := expandPatterns([]string{path.Join(debugDirPath, zippedNodeTableDumpsPattern)})
	if err != nil {
		return nil, err
	}

	filteredTxtFiles := []string{}
	for _, txtFile := range allTxtFiles {
		if _, ok := nodeSpecificTableDumps[filepath.Base(txtFile)]; ok {
			filteredTxtFiles = append(filteredTxtFiles, strings.TrimPrefix(txtFile, debugDirPath+"/"))
		}
	}

	return filteredTxtFiles, nil
}
