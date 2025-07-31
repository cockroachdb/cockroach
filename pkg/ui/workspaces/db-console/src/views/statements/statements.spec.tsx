// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale, toRoundedDateRange, util } from "@cockroachlabs/cluster-ui";
import * as H from "history";
import merge from "lodash/merge";
import Long from "long";
import moment from "moment-timezone";
import { RouteComponentProps } from "react-router-dom";

import "src/protobufInit";
import * as protos from "src/js/protos";
import { AdminUIState, createAdminUIStore } from "src/redux/state";
import {
  appAttr,
  appNamesAttr,
  statementAttr,
  unset,
} from "src/util/constants";

import { selectStatementDetails } from "./statementDetails";
import { selectLastReset } from "./statementsPage";

import ISensitiveInfo = protos.cockroach.sql.ISensitiveInfo;

const { generateStmtDetailsToID, longToInt } = util;

type CollectedStatementStatistics = util.CollectedStatementStatistics;
type ExecStats = util.ExecStats;
type StatementStatistics = util.StatementStatistics;
type StatementDetails =
  protos.cockroach.server.serverpb.StatementDetailsResponse;

interface StatementDetailsWithID {
  details: StatementDetails;
  id: Long;
}

const INTERNAL_STATEMENT_PREFIX = "$ internal";

const timeScale: TimeScale = {
  key: "Custom",
  windowSize: moment.duration(1, "hour"),
  windowValid: moment.duration(1, "minute"),
  sampleSize: moment.duration(30, "seconds"),
  fixedWindowEnd: moment(1646334815),
};

describe("selectLastReset", () => {
  it('returns "unknown" if the statements data is invalid', () => {
    const state = makeInvalidState();

    const result = selectLastReset(state);

    expect(result).toEqual("unknown");
  });

  it("returns the formatted timestamp if valid", () => {
    const timestamp = 92951700;
    const state = makeStateWithLastReset(timestamp, timeScale);

    const result = selectLastReset(state);

    expect(moment.utc(result).unix()).toEqual(timestamp);
  });
});

describe("selectStatement", () => {
  it("returns null if the statements data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();
    const { statementDetails } = selectStatementDetails(state, props);

    expect(statementDetails).toBeNull();
  });

  it("returns the statement currently loaded", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, "foobar");
    const stmtC = makeFingerprint(3, "another");
    const detailsA = makeDetails(stmtA);
    const detailsB = makeDetails(stmtB);
    const detailsC = makeDetails(stmtC);
    const state = makeStateWithStatements([stmtA, stmtB, stmtC], timeScale, [
      detailsA,
      detailsB,
      detailsC,
    ]);

    const stmtAFingerprintID = stmtA.id.toString();
    const props = makeRoutePropsWithStatement(stmtAFingerprintID);
    const { statementDetails } = selectStatementDetails(state, props);
    const result = statementDetails.statement;

    expect(result.metadata.query).toEqual(stmtA.key.key_data.query);
    expect(result.stats.count.toNumber()).toEqual(stmtA.stats.count.toNumber());
    expect(longToInt(result.stats.failure_count)).toBe(0);
    expect(result.metadata.app_names).toEqual([stmtA.key.key_data.app]);
    expect(longToInt(result.metadata.dist_sql_count)).toBe(0);
    expect(longToInt(result.metadata.full_scan_count)).toBe(0);
    expect(longToInt(result.metadata.vec_count)).toBe(0);
    expect(longToInt(result.metadata.total_count)).toBe(1);
  });

  it("filters out statements when app param is set", () => {
    const stmtA = makeFingerprint(1, "foo");
    const stmtB = makeFingerprint(2, "bar");
    const stmtC = makeFingerprint(3, "baz");
    const detailsA = makeDetails(stmtA);
    const detailsB = makeDetails(stmtB);
    const detailsC = makeDetails(stmtC);
    const appFilter = "foo";

    const state = makeStateWithStatements(
      [stmtA, stmtB, stmtC],
      timeScale,
      [detailsA, detailsB, detailsC],
      appFilter,
    );
    const stmtAFingerprintID = stmtA.id.toString();
    const props = makeRoutePropsWithStatementAndApp(
      stmtAFingerprintID,
      appFilter,
    );

    const { statementDetails } = selectStatementDetails(state, props);
    const result = statementDetails.statement;

    expect(result.metadata.query).toEqual(stmtA.key.key_data.query);
    expect(result.stats.count.toNumber()).toEqual(stmtA.stats.count.toNumber());
    expect(longToInt(result.stats.failure_count)).toBe(0);
    expect(result.metadata.app_names).toEqual([stmtA.key.key_data.app]);
    expect(longToInt(result.metadata.dist_sql_count)).toBe(0);
    expect(longToInt(result.metadata.full_scan_count)).toBe(0);
    expect(longToInt(result.metadata.vec_count)).toBe(0);
    expect(longToInt(result.metadata.total_count)).toBe(1);
  });

  it('filters out statements with app set when app param is "(unset)"', () => {
    const stmtA = makeFingerprint(1, "");
    const stmtB = makeFingerprint(2, "bar");
    const stmtC = makeFingerprint(3, "baz");
    const detailsA = makeDetails(stmtA);
    const detailsB = makeDetails(stmtB);
    const detailsC = makeDetails(stmtC);

    const state = makeStateWithStatements([stmtA, stmtB, stmtC], timeScale, [
      detailsA,
      detailsB,
      detailsC,
    ]);
    const stmtAFingerprintID = stmtA.id.toString();
    const props = makeRoutePropsWithStatementAndApp(stmtAFingerprintID, unset);

    const { statementDetails } = selectStatementDetails(state, props);
    const result = statementDetails.statement;

    expect(result.metadata.query).toEqual(stmtA.key.key_data.query);
    expect(result.stats.count.toNumber()).toEqual(stmtA.stats.count.toNumber());
    expect(longToInt(result.stats.failure_count)).toBe(0);
    expect(result.metadata.app_names).toEqual([stmtA.key.key_data.app]);
    expect(longToInt(result.metadata.dist_sql_count)).toBe(0);
    expect(longToInt(result.metadata.full_scan_count)).toBe(0);
    expect(longToInt(result.metadata.vec_count)).toBe(0);
    expect(longToInt(result.metadata.total_count)).toBe(1);
  });

  it('filters out statements with app set when app param is "$ internal"', () => {
    const stmtA = makeFingerprint(1, "$ internal_stmnt_app");
    const stmtB = makeFingerprint(2, "bar");
    const stmtC = makeFingerprint(3, "baz");
    const detailsA = makeDetails(stmtA);
    const detailsB = makeDetails(stmtB);
    const detailsC = makeDetails(stmtC);
    const appFilter = "$ internal";
    const state = makeStateWithStatements(
      [stmtA, stmtB, stmtC],
      timeScale,
      [detailsA, detailsB, detailsC],
      appFilter,
    );
    const stmtAFingerprintID = stmtA.id.toString();
    const props = makeRoutePropsWithStatementAndApp(
      stmtAFingerprintID,
      appFilter,
    );

    const { statementDetails } = selectStatementDetails(state, props);
    const result = statementDetails?.statement;

    expect(result.metadata.query).toEqual(stmtA.key.key_data.query);
    expect(result.stats.count.toNumber()).toEqual(stmtA.stats.count.toNumber());
    expect(longToInt(result.stats.failure_count)).toBe(0);
    // Statements with internal app prefix should have "$ internal" as app name
    expect(result.metadata.app_names).toEqual(["$ internal_stmnt_app"]);
    expect(longToInt(result.metadata.dist_sql_count)).toBe(0);
    expect(longToInt(result.metadata.full_scan_count)).toBe(0);
    expect(longToInt(result.metadata.vec_count)).toBe(0);
    expect(longToInt(result.metadata.total_count)).toBe(1);
  });
});

function makeFingerprint(
  id: number,
  app = "",
  nodeId = 1,
  distSQL = false,
  failed = false,
  vec = false,
) {
  return {
    key: {
      key_data: {
        query: "SELECT * FROM table_" + id + " WHERE true",
        app,
        distSQL,
        vec,
        failed,
      },
      node_id: nodeId,
    },
    id: Long.fromNumber(id),
    stats: makeStats(),
  };
}

function makeDetails(
  statement: CollectedStatementStatistics,
): StatementDetailsWithID {
  return {
    id: statement.id,
    details: {
      statement: {
        metadata: {
          query: statement.key.key_data.query,
          app_names: [statement.key.key_data.app],
          dist_sql_count: statement.key.key_data.distSQL
            ? new Long(1)
            : new Long(0),
          full_scan_count: statement.key.key_data.full_scan
            ? new Long(1)
            : new Long(0),
          vec_count: statement.key.key_data.vec ? new Long(1) : new Long(0),
          total_count: new Long(1),
        },
        stats: statement.stats,
      },
      statement_statistics_per_aggregated_ts: [],
      statement_statistics_per_plan_hash: [],
      internal_app_name_prefix: "$ internal",
    },
  };
}

let makeStatsIndex = 1;
function makeStats(): Required<StatementStatistics> {
  return {
    count: Long.fromNumber(makeStatsIndex++),
    first_attempt_count: Long.fromNumber(1),
    max_retries: Long.fromNumber(0),
    legacy_last_err: "",
    legacy_last_err_redacted: "",
    num_rows: makeStat(),
    idle_lat: makeStat(),
    parse_lat: makeStat(),
    plan_lat: makeStat(),
    run_lat: makeStat(),
    overhead_lat: makeStat(),
    service_lat: makeStat(),
    sensitive_info: makeEmptySensitiveInfo(),
    rows_read: makeStat(),
    bytes_read: makeStat(),
    rows_written: makeStat(),
    exec_stats: makeExecStats(),
    sql_type: "DDL",
    last_exec_timestamp: {
      seconds: Long.fromInt(1599670292),
      nanos: 111613000,
    },
    nodes: [Long.fromInt(1), Long.fromInt(2), Long.fromInt(3)],
    kv_node_ids: [1, 2, 3],
    regions: ["gcp-us-east1"],
    used_follower_read: false,
    plan_gists: ["Ais="],
    index_recommendations: [],
    indexes: ["123@456"],
    latency_info: {
      min: 0.01,
      max: 1.2,
      p50: 0.4,
      p90: 0.7,
      p99: 1.1,
    },
    last_error_code: "",
    failure_count: Long.fromNumber(0),
  };
}

function makeExecStats(): ExecStats {
  return {
    count: Long.fromNumber(10),
    network_bytes: makeStat(),
    max_mem_usage: makeStat(),
    contention_time: makeStat(),
    network_messages: makeStat(),
    max_disk_usage: makeStat(),
    cpu_sql_nanos: makeStat(),
  };
}

function makeStat() {
  return {
    mean: 10,
    squared_diffs: 1,
  };
}

function makeEmptySensitiveInfo(): ISensitiveInfo {
  return {
    last_err: null,
    most_recent_plan_description: null,
  };
}

function makeInvalidState(): AdminUIState {
  const store = createAdminUIStore(H.createMemoryHistory());
  return merge(store.getState(), {
    cachedData: {
      statements: {
        inFlight: true,
        valid: false,
      },
      statementDetails: {},
      statementDiagnosticsReports: {
        inFlight: true,
        valid: false,
      },
    },
  });
}

function makeStateWithStatementsAndLastReset(
  statements: CollectedStatementStatistics[],
  lastReset: number,
  timeScale: TimeScale,
  statementsDetails?: StatementDetailsWithID[],
  appFilter?: string,
) {
  const store = createAdminUIStore(H.createMemoryHistory());
  const state = merge(store.getState(), {
    cachedData: {
      statements: {
        data: new protos.cockroach.server.serverpb.StatementsResponse({
          statements,
          last_reset: new protos.google.protobuf.Timestamp({
            seconds: Long.fromNumber(lastReset),
            nanos: 0,
          }),
          internal_app_name_prefix: INTERNAL_STATEMENT_PREFIX,
        }),
        inFlight: false,
        valid: true,
      },
      statementDetails: {},
      statementDiagnosticsReports: {
        inFlight: true,
        valid: false,
      },
    },
    localSettings: {
      [localStorage.GlOBAL_TIME_SCALE]: timeScale,
    },
    timeScale: {
      scale: timeScale,
    },
  });

  const [start, end] = toRoundedDateRange(timeScale);
  const timeStart = Long.fromNumber(start.unix());
  const timeEnd = Long.fromNumber(end.unix());

  if (statementsDetails) {
    for (const stmt of statementsDetails) {
      state.cachedData.statementDetails[
        generateStmtDetailsToID(
          stmt.id.toString(),
          appFilter,
          timeStart,
          timeEnd,
        )
      ] = {
        data: new protos.cockroach.server.serverpb.StatementDetailsResponse({
          statement: stmt.details.statement,
          statement_statistics_per_aggregated_ts: [],
          statement_statistics_per_plan_hash: [],
          internal_app_name_prefix: "$ internal",
        }),
        inFlight: false,
        valid: true,
        unauthorized: false,
      };
    }
  }

  return state;
}

function makeStateWithStatements(
  statements: CollectedStatementStatistics[],
  timeScale: TimeScale,
  statementsDetails?: StatementDetailsWithID[],
  appFilter?: string,
) {
  return makeStateWithStatementsAndLastReset(
    statements,
    0,
    timeScale,
    statementsDetails,
    appFilter,
  );
}

function makeStateWithLastReset(lastReset: number, timeScale: TimeScale) {
  return makeStateWithStatementsAndLastReset([], lastReset, timeScale);
}

function makeRoutePropsWithParams(params: { [key: string]: string }) {
  const history = H.createHashHistory();
  return {
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params,
    },
  };
}

function makeEmptyRouteProps(): RouteComponentProps<any> {
  const history = H.createHashHistory();
  return {
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params: {},
    },
  };
}

function makeRoutePropsWithStatement(stmt: string) {
  return makeRoutePropsWithParams({
    [statementAttr]: stmt,
  });
}

function makeRoutePropsWithStatementAndApp(stmt: string, app: string) {
  const params = {
    [appAttr]: app,
    [statementAttr]: stmt,
  };
  const searchParams = {
    [appNamesAttr]: app,
  };
  const history = H.createHashHistory();
  history.location.search = new URLSearchParams(searchParams).toString();
  return {
    location: history.location,
    history,
    match: {
      url: "",
      path: history.location.pathname,
      isExact: false,
      params,
    },
  };
}
