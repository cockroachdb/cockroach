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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

type tsvColumnParserFn func(string) (any, error)

type columnParserMap map[string]tsvColumnParserFn

// skipColumn is an empty struct that is used to indicate that a column should
// be skipped. This indicator has to be a type and not a value like bool
// because, it could conflict with the actual column value. Parsing functions
// can return this type conditionally or unconditionally to indicate that the
// column should be skipped.
type skipColumn struct{}

type tableDumpChunk struct {
	payload   []byte
	tableName string
}

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
	"system.statement_diagnostics_requests.txt":     {},
	"system.statement_diagnostics.txt":              {},
	"system.sql_stats_cardinality.txt":              {},
	"system.settings.txt":                           {},
	"system.reports_meta.txt":                       {},
	"system.replication_stats.txt":                  {},
	"system.replication_critical_localities.txt":    {},
	"system.replication_constraint_stats.txt":       {},
	"crdb_internal.cluster_distsql_flows.txt":       {},
	"system.tenant_tasks.txt":                       {},
	"system.tenant_settings.txt":                    {},
	"system.task_payloads.txt":                      {},
	"system.role_options.txt":                       {},
	"system.protected_ts_meta.txt":                  {},
	"system.privileges.txt":                         {},
	"system.external_connections.txt":               {},
	"system.database_role_settings.txt":             {},
	"crdb_internal.super_regions.txt":               {},
	"crdb_internal.schema_changes.txt":              {},
	"crdb_internal.partitions.txt":                  {},
	"crdb_internal.kv_system_privileges.txt":        {},
	"crdb_internal.kv_protected_ts_records.txt":     {},
	"crdb_internal.invalid_objects.txt":             {},
	"crdb_internal.create_type_statements.txt":      {},
	"crdb_internal.create_procedure_statements.txt": {},
	"crdb_internal.create_function_statements.txt":  {},
	"crdb_internal.logical_replication_spans.txt":   {},
	"crdb_internal.cluster_replication_spans.txt":   {},
	"system.protected_ts_records.txt":               {},

	// table dumps with columns that need to be decoded
	"crdb_internal.system_jobs.txt": {
		"progress": makeProtoColumnParser[*jobspb.Progress](),
	},
	"system.tenants.txt": {
		"info": makeProtoColumnParser[*mtinfopb.ProtoInfo](),
	},
	"system.statement_statistics_limit_5000.txt": {
		"fingerprint_id":             decodeUUID,
		"transaction_fingerprint_id": decodeUUID,
		"plan_hash":                  decodeUUID,
	},
	"system.sql_instances.txt": {
		"session_id": decodeUUID,
	},
	"system.sqlliveness.txt": {
		"session_id":  decodeUUID,
		"crdb_region": decodeRegion,
	},
	"system.lease.txt": {
		"session_id":  decodeUUID,
		"crdb_region": decodeRegion,
	},
	"system.eventlog.txt": {
		"uniqueID": decodeUUID,
	},
	"system.descriptor.txt": {
		"descriptor": makeProtoColumnParser[*descpb.Descriptor](),
	},
	"system.scheduled_jobs.txt": {
		"schedule_state":   makeProtoColumnParser[*jobspb.ScheduleState](),
		"schedule_details": makeProtoColumnParser[*jobspb.ScheduleDetails](),
		"execution_args":   makeProtoColumnParser[*jobspb.ExecutionArguments](),
	},
	"system.tenant_usage.txt": {
		"total_consumption": makeProtoColumnParser[*kvpb.TenantConsumption](),
		"current_rates":     makeProtoColumnParser[*kvpb.TenantConsumptionRates](),
		"next_rates":        makeProtoColumnParser[*kvpb.TenantConsumptionRates](),
	},
	"system.span_configurations.txt": {
		"start_key": decodeKey,
		"end_key":   decodeKey,
		"config":    makeProtoColumnParser[*roachpb.SpanConfig](),
	},
	"crdb_internal.cluster_locks.txt": {
		"lock_key": skipColumnFn, // lock_key can be skipped because there is another column called lock_key_pretty
	},
	"crdb_internal.transaction_contention_events.fallback.txt": {
		"blocking_txn_fingerprint_id": decodeUUID,
		"waiting_stmt_fingerprint_id": decodeUUID,
		"waiting_txn_fingerprint_id":  decodeUUID,
	},
	"crdb_internal.transaction_contention_events.txt": {
		"blocking_txn_fingerprint_id": decodeUUID,
		"waiting_stmt_fingerprint_id": decodeUUID,
		"waiting_txn_fingerprint_id":  decodeUUID,
	},
}

var nodeSpecificTableDumps = map[string]columnParserMap{
	"crdb_internal.node_metrics.txt":                                {},
	"crdb_internal.node_txn_stats.txt":                              {},
	"crdb_internal.node_contention_events.txt":                      {},
	"crdb_internal.gossip_liveness.txt":                             {},
	"crdb_internal.gossip_nodes.txt":                                {},
	"crdb_internal.node_runtime_info.txt":                           {},
	"crdb_internal.node_transaction_statistics.txt":                 {},
	"crdb_internal.node_tenant_capabilities_cache.txt":              {},
	"crdb_internal.node_sessions.txt":                               {},
	"crdb_internal.node_statement_statistics.txt":                   {},
	"crdb_internal.leases.txt":                                      {},
	"crdb_internal.node_build_info.txt":                             {},
	"crdb_internal.node_memory_monitors.txt":                        {},
	"crdb_internal.active_range_feeds.txt":                          {},
	"crdb_internal.gossip_alerts.txt":                               {},
	"crdb_internal.node_transactions.txt":                           {},
	"crdb_internal.feature_usage.txt":                               {},
	"crdb_internal.node_queries.txt":                                {},
	"crdb_internal.cluster_replication_node_stream_checkpoints.txt": {},
	"crdb_internal.logical_replication_node_processors.txt":         {},
	"crdb_internal.cluster_replication_node_streams.txt":            {},
	"crdb_internal.node_inflight_trace_spans.txt":                   {},
	"crdb_internal.node_distsql_flows.txt":                          {},
	"crdb_internal.cluster_replication_node_stream_spans.txt":       {},

	// table dumps with columns that need to be decoded
	"crdb_internal.node_txn_execution_insights.txt": {
		"txn_fingerprint_id": decodeUUID,
	},
	"crdb_internal.node_execution_insights.txt": {
		"txn_fingerprint_id":  decodeUUID,
		"stmt_fingerprint_id": decodeUUID,
	},
	"crdb_internal.kv_session_based_leases.txt": {
		"session_id":  decodeUUID,
		"crdb_region": decodeRegion,
	},
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
	ctx context.Context,
	dir, fileName, uploadID string,
	parsers columnParserMap,
	uploadFn func(*tableDumpChunk),
) error {
	f, err := os.Open(path.Join(dir, fileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	defer f.Close()

	var (
		lines     = [][]byte{}
		tableName = strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))
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
			if cols[i] == "NULL" {
				headerColumnMapping[h] = nil
				continue
			}

			if parser, ok := parsers[h]; ok {
				colBytes, err := parser(cols[i])
				if err != nil {
					return err
				}

				if colBytes == (skipColumn{}) {
					continue
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

		if len(lines) == datadogMaxLogLinesPerReq {
			uploadFn(&tableDumpChunk{
				payload:   makeDDMultiLineLogPayload(lines),
				tableName: tableName,
			})
			lines = lines[:0]
			return nil
		}

		lines = append(lines, jsonRow)
		return nil
	}); err != nil {
		return err
	}

	// flush the remaining lines if any
	if len(lines) > 0 {
		uploadFn(&tableDumpChunk{
			payload:   makeDDMultiLineLogPayload(lines),
			tableName: tableName,
		})
	}
	return nil
}

// makeTableIterator returns the headers slice and an iterator
func makeTableIterator(f io.Reader) ([]string, func(func(string) error) error) {
	scanner := bufio.NewScanner(f)

	// some of the rows can be very large, bigger than the bufio.MaxTokenSize
	// (65kb). So, we need to increase the buffer size and split by lines while
	// scanning.
	scanner.Buffer(nil, 5<<20) // 5 MB
	scanner.Split(bufio.ScanLines)

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

func decodeUUID(inp string) (any, error) {
	_, u, err := encoding.DecodeUUIDValue([]byte(inp))
	return u, err
}

func decodeRegion(s string) (any, error) {
	if s == `\x80` {
		return nil, nil
	}
	return s, nil
}

func decodeKey(s string) (any, error) {
	b, ok := interpretString(s)
	if !ok {
		return nil, fmt.Errorf("failed to interpret column value: %s", s)
	}

	return roachpb.Key(b).String(), nil
}

func skipColumnFn(s string) (any, error) {
	return skipColumn{}, nil
}
