// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { AlignedData } from "uplot";

import { longToInt, TimestampToNumber } from "../util";

type StatementStatisticsPerAggregatedTs =
  cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByAggregatedTs;

export function generateExecuteAndPlanningTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const execution: Array<number> = [];
  const planning: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    execution.push(stat.stats.run_lat.mean * 1e9);
    planning.push(stat.stats.plan_lat.mean * 1e9);
  });

  return [ts, execution, planning];
}

export function generateClientWaitTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const clientWait: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    clientWait.push(stat.stats.idle_lat.mean * 1e9);
  });

  return [ts, clientWait];
}

export function generateRowsProcessedTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const read: Array<number> = [];
  const written: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    read.push(stat.stats.rows_read?.mean);
    written.push(stat.stats.rows_written?.mean);
  });

  return [ts, read, written];
}

export function generateExecRetriesTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const retries: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);

    const totalCountBarChart = longToInt(stat.stats.count);
    const firstAttemptsBarChart = longToInt(stat.stats.first_attempt_count);
    retries.push(totalCountBarChart - firstAttemptsBarChart);
  });

  return [ts, retries];
}

export function generateExecCountTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    count.push(longToInt(stat.stats.count));
  });

  return [ts, count];
}

export function generateContentionTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
    count.push(stat.stats.exec_stats.contention_time.mean * 1e9);
  });

  return [ts, count];
}

export function generateCPUTimeseries(
  stats: StatementStatisticsPerAggregatedTs[],
): AlignedData {
  const ts: Array<number> = [];
  const count: Array<number> = [];

  stats.forEach(function (stat: StatementStatisticsPerAggregatedTs) {
    if (stat.stats.exec_stats.cpu_sql_nanos) {
      ts.push(TimestampToNumber(stat.aggregated_ts) * 1e3);
      count.push(stat.stats.exec_stats.cpu_sql_nanos.mean);
    }
  });

  return [ts, count];
}

type StatementStatisticsPerAggregatedTsAndPlanHash =
  cockroach.server.serverpb.StatementDetailsResponse.ICollectedStatementGroupedByAggregatedTsAndPlanHash;

export function generatePlanDistributionTimeseries(
  stats: StatementStatisticsPerAggregatedTsAndPlanHash[],
): { alignedData: AlignedData; planHashes: string[] } {
  // Group data by timestamp and plan hash
  const timeMap = new Map<number, Map<string, number>>();
  const planHashSet = new Set<string>();

  stats.forEach((stat) => {
    const ts = TimestampToNumber(stat.aggregated_ts) * 1e3;
    const planHashHex = stat.plan_hash?.toString(16) || "unknown";
    planHashSet.add(planHashHex);

    if (!timeMap.has(ts)) {
      timeMap.set(ts, new Map());
    }
    timeMap.get(ts).set(planHashHex, Number(stat.execution_count || 0));
  });

  // Sort timestamps and plan hashes for consistent ordering
  const timestamps = Array.from(timeMap.keys()).sort((a, b) => a - b);
  const planHashes = Array.from(planHashSet).sort();

  // Build aligned data structure: [timestamps, plan1_counts, plan2_counts, ...]
  const alignedData: AlignedData = [timestamps];

  planHashes.forEach((planHash) => {
    const counts = timestamps.map((ts) => {
      return timeMap.get(ts)?.get(planHash) || 0;
    });
    alignedData.push(counts);
  });

  return { alignedData, planHashes };
}

type TableStatsCollectionEvents = { [key: string]: {
  events?: Array<{
    timestamp?: { seconds?: number; nanos?: number };
    event_type?: string;
    reporting_id?: number;
    info?: string;
    unique_id?: Uint8Array;
    stats_name?: string;
    column_ids?: number[];
    info_timestamp?: { seconds?: number; nanos?: number };
    stats_id?: number;
  }>;
} };

interface TableStatsEvent {
  tableId: number;
  tableName: string;
  events: Array<{
    timestamp: Date;
    statsId: number;
    statsName: string;
    columnIds: number[];
    eventType: string;
    reportingId: number;
    info: string;
    uniqueId: Uint8Array;
    infoTimestamp: Date;
  }>;
}

interface TableStatsTimelineData {
  alignedData: AlignedData;
  tableNames: string[];
  eventsByTable: Map<string, Array<{
    startTime: number;
    endTime: number;
    statsId: number;
    statsName: string;
    columnIds: number[];
    eventInfo: string;
  }>>;
}

export function generateTableStatsCollectionTimeline(
  tableStatsCollectionEvents: TableStatsCollectionEvents,
): TableStatsTimelineData {
  const tableEvents: TableStatsEvent[] = [];
  
  // Convert protobuf map to array of table events
  Object.entries(tableStatsCollectionEvents || {}).forEach(([tableIdStr, collection]) => {
    const tableId = parseInt(tableIdStr, 10);
    const events = collection.events?.map(event => {
      // Handle protobuf timestamp format
      const timestampMs = event.timestamp ? 
        (event.timestamp.seconds || 0) * 1000 + (event.timestamp.nanos || 0) / 1000000 :
        0;
      const infoTimestampMs = event.info_timestamp ?
        (event.info_timestamp.seconds || 0) * 1000 + (event.info_timestamp.nanos || 0) / 1000000 :
        0;
        
      return {
        timestamp: new Date(timestampMs),
        statsId: Number(event.stats_id || 0),
        statsName: event.stats_name || '',
        columnIds: event.column_ids || [],
        eventType: event.event_type || '',
        reportingId: Number(event.reporting_id || 0),
        info: event.info || '',
        uniqueId: event.unique_id || new Uint8Array(),
        infoTimestamp: new Date(infoTimestampMs),
      };
    }) || [];

    // Sort events by timestamp
    events.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());

    tableEvents.push({
      tableId,
      tableName: `table_${tableId}`, // We could enhance this by looking up actual table names
      events,
    });
  });

  // Sort tables by ID for consistent ordering
  tableEvents.sort((a, b) => a.tableId - b.tableId);
  
  const tableNames = tableEvents.map(t => t.tableName);

  // Find the overall time range
  let minTime = Number.MAX_SAFE_INTEGER;
  let maxTime = 0;
  tableEvents.forEach(table => {
    table.events.forEach(event => {
      const ts = event.timestamp.getTime();
      minTime = Math.min(minTime, ts);
      maxTime = Math.max(maxTime, ts);
    });
  });

  // If no events, return empty data
  if (minTime === Number.MAX_SAFE_INTEGER) {
    return {
      alignedData: [[]],
      tableNames: [],
      eventsByTable: new Map(),
    };
  }

  // Create timeline segments for each table
  const eventsByTable = new Map<string, Array<{
    startTime: number;
    endTime: number;
    statsId: number;
    statsName: string;
    columnIds: number[];
    eventInfo: string;
  }>>();

  tableEvents.forEach((table, tableIndex) => {
    const segments: Array<{
      startTime: number;
      endTime: number;
      statsId: number;
      statsName: string;
      columnIds: number[];
      eventInfo: string;
    }> = [];

    for (let i = 0; i < table.events.length; i++) {
      const event = table.events[i];
      const startTime = event.timestamp.getTime();
      // End time is either the next event or extend beyond the current max time
      const endTime = i < table.events.length - 1 
        ? table.events[i + 1].timestamp.getTime()
        : maxTime + (maxTime - minTime) * 0.1; // Extend 10% beyond last event

      segments.push({
        startTime,
        endTime,
        statsId: event.statsId,
        statsName: event.statsName,
        columnIds: event.columnIds,
        eventInfo: event.info,
      });
    }

    eventsByTable.set(table.tableName, segments);
  });

  // For the timeline chart, we'll create data points at event boundaries
  const allEventTimes = new Set<number>();
  tableEvents.forEach(table => {
    table.events.forEach(event => {
      allEventTimes.add(event.timestamp.getTime());
    });
  });

  const sortedTimes = Array.from(allEventTimes).sort((a, b) => a - b);

  // Create chart data where each table has a constant value of its index (for y-axis positioning)
  const alignedData: AlignedData = [sortedTimes];
  
  tableNames.forEach((tableName, index) => {
    // Each table gets a horizontal line at its index position
    const values = sortedTimes.map(() => index);
    alignedData.push(values);
  });

  return {
    alignedData,
    tableNames,
    eventsByTable,
  };
}
