// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import Long from "long";
import moment from "moment";
import { RouteComponentProps } from "react-router-dom";
import * as H from "history";
import { merge } from "lodash";

import "src/protobufInit";
import * as protos from "src/js/protos";
import {
  appAttr,
  appNamesAttr,
  statementAttr,
  unset,
} from "src/util/constants";
import {
  selectStatements,
  selectApps,
  selectTotalFingerprints,
  selectLastReset,
} from "./statementsPage";
import { selectStatementDetails } from "./statementDetails";
import ISensitiveInfo = protos.cockroach.sql.ISensitiveInfo;
import { AdminUIState, createAdminUIStore } from "src/redux/state";
import { TimeScale, toRoundedDateRange, util } from "@cockroachlabs/cluster-ui";

const { generateStmtDetailsToID, longToInt } = util;

type CollectedStatementStatistics = util.CollectedStatementStatistics;
type ExecStats = util.ExecStats;
type StatementStatistics = util.StatementStatistics;
type StatementDetails = protos.cockroach.server.serverpb.StatementDetailsResponse;

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

describe("selectStatements", () => {
  it("returns null if the statements data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();
    const result = selectStatements(state, props);

    assert.isNull(result);
  });

  it("returns the statements currently loaded", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, "foobar");
    const stmtC = makeFingerprint(3, "another");
    const state = makeStateWithStatements([stmtA, stmtB, stmtC], timeScale);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);
    assert.equal(result.length, 3);

    const expectedFingerprints = [stmtA, stmtB, stmtC].map(
      stmt => stmt.key.key_data.query,
    );
    expectedFingerprints.sort();
    const actualFingerprints = result.map((stmt: any) => stmt.label);
    actualFingerprints.sort();
    assert.deepEqual(actualFingerprints, expectedFingerprints);
  });

  it("returns the statements without Internal for default ALL filter", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, INTERNAL_STATEMENT_PREFIX);
    const stmtC = makeFingerprint(3, INTERNAL_STATEMENT_PREFIX);
    const stmtD = makeFingerprint(3, "another");
    const state = makeStateWithStatements(
      [stmtA, stmtB, stmtC, stmtD],
      timeScale,
    );
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 2);
  });

  it("coalesces statements from different apps", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(1, "foobar");
    const stmtC = makeFingerprint(1, "another");
    const sumCount = stmtA.stats.count
      .add(stmtB.stats.count.add(stmtC.stats.count))
      .toNumber();
    const state = makeStateWithStatements([stmtA, stmtB, stmtC], timeScale);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
    assert.equal(result[0].label, stmtA.key.key_data.query);
    assert.equal(result[0].stats.count.toNumber(), sumCount);
  });

  it("coalesces statements with differing node ids", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "", 1),
        makeFingerprint(1, "", 2),
        makeFingerprint(1, "", 3),
      ],
      timeScale,
    );
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("coalesces statements with differing distSQL and failed values", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "", 1, false, false),
        makeFingerprint(1, "", 1, false, true),
        makeFingerprint(1, "", 1, true, false),
        makeFingerprint(1, "", 1, true, true),
      ],
      timeScale,
    );
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("filters out statements when app param is set", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "foo"),
        makeFingerprint(2, "bar"),
        makeFingerprint(3, "baz"),
      ],
      timeScale,
    );
    const props = makeRoutePropsWithApp("foo");

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it('filters out statements with app set when app param is "(unset)"', () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, ""),
        makeFingerprint(2, "bar"),
        makeFingerprint(3, "baz"),
      ],
      timeScale,
    );
    const props = makeRoutePropsWithApp(unset);

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it('filters out statements with app set when app param is "$ internal"', () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "$ internal_stmnt_app"),
        makeFingerprint(2, "bar"),
        makeFingerprint(3, "baz"),
      ],
      timeScale,
    );
    const props = makeRoutePropsWithApp("$ internal");
    const result = selectStatements(state, props);
    assert.equal(result.length, 1);
  });
});

describe("selectApps", () => {
  it("returns an empty array if the statements data is invalid", () => {
    const state = makeInvalidState();

    const result = selectApps(state);

    assert.deepEqual(result, []);
  });

  it("returns all the apps that appear in the statements", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1),
        makeFingerprint(1, "foobar"),
        makeFingerprint(2, "foobar"),
        makeFingerprint(3, "cockroach sql"),
      ],
      timeScale,
    );

    const result = selectApps(state);

    assert.deepEqual(result, [unset, "cockroach sql", "foobar"]);
  });
});

describe("selectTotalFingerprints", () => {
  it("returns zero if the statements data is invalid", () => {
    const state = makeInvalidState();

    const result = selectTotalFingerprints(state);

    assert.equal(result, 0);
  });

  it("returns the number of statement fingerprints", () => {
    const state = makeStateWithStatements(
      [makeFingerprint(1), makeFingerprint(2), makeFingerprint(3)],
      timeScale,
    );

    const result = selectTotalFingerprints(state);

    assert.equal(result, 3);
  });

  it("coalesces statements from different apps", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1),
        makeFingerprint(1, "foobar"),
        makeFingerprint(1, "another"),
      ],
      timeScale,
    );

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });

  it("coalesces statements with differing node ids", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "", 1),
        makeFingerprint(1, "", 2),
        makeFingerprint(1, "", 3),
      ],
      timeScale,
    );

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });

  it("coalesces statements with differing distSQL and failed keys", () => {
    const state = makeStateWithStatements(
      [
        makeFingerprint(1, "", 1, false, false),
        makeFingerprint(1, "", 1, false, true),
        makeFingerprint(1, "", 1, true, false),
        makeFingerprint(1, "", 1, true, true),
      ],
      timeScale,
    );

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });
});

describe("selectLastReset", () => {
  it('returns "unknown" if the statements data is invalid', () => {
    const state = makeInvalidState();

    const result = selectLastReset(state);

    assert.equal(result, "unknown");
  });

  it("returns the formatted timestamp if valid", () => {
    const timestamp = 92951700;
    const state = makeStateWithLastReset(timestamp, timeScale);

    const result = selectLastReset(state);

    assert.equal(moment.utc(result).unix(), timestamp);
  });
});

describe("selectStatement", () => {
  it("returns null if the statements data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();
    const { statementDetails } = selectStatementDetails(state, props);
    const result = statementDetails;

    assert.isNull(result);
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

    assert.equal(result.metadata.query, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.metadata.app_names, [stmtA.key.key_data.app]);
    assert.equal(longToInt(result.metadata.dist_sql_count), 0);
    assert.equal(longToInt(result.metadata.failed_count), 0);
    assert.equal(longToInt(result.metadata.full_scan_count), 0);
    assert.equal(longToInt(result.metadata.vec_count), 0);
    assert.equal(longToInt(result.metadata.total_count), 1);
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

    assert.equal(result.metadata.query, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.metadata.app_names, [stmtA.key.key_data.app]);
    assert.equal(longToInt(result.metadata.dist_sql_count), 0);
    assert.equal(longToInt(result.metadata.failed_count), 0);
    assert.equal(longToInt(result.metadata.full_scan_count), 0);
    assert.equal(longToInt(result.metadata.vec_count), 0);
    assert.equal(longToInt(result.metadata.total_count), 1);
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

    assert.equal(result.metadata.query, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.metadata.app_names, [stmtA.key.key_data.app]);
    assert.equal(longToInt(result.metadata.dist_sql_count), 0);
    assert.equal(longToInt(result.metadata.failed_count), 0);
    assert.equal(longToInt(result.metadata.full_scan_count), 0);
    assert.equal(longToInt(result.metadata.vec_count), 0);
    assert.equal(longToInt(result.metadata.total_count), 1);
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

    assert.equal(result.metadata.query, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    // Statements with internal app prefix should have "$ internal" as app name
    assert.deepEqual(result.metadata.app_names, ["$ internal_stmnt_app"]);
    assert.equal(longToInt(result.metadata.dist_sql_count), 0);
    assert.equal(longToInt(result.metadata.failed_count), 0);
    assert.equal(longToInt(result.metadata.full_scan_count), 0);
    assert.equal(longToInt(result.metadata.vec_count), 0);
    assert.equal(longToInt(result.metadata.total_count), 1);
  });
});

function makeFingerprint(
  id: number,
  app: string = "",
  nodeId: number = 1,
  distSQL: boolean = false,
  failed: boolean = false,
  vec: boolean = false,
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
          failed_count: statement.key.key_data.failed
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
      toJSON: () => ({}),
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
    plan_gists: ["Ais="],
  };
}

function makeExecStats(): Required<ExecStats> {
  return {
    count: Long.fromNumber(10),
    network_bytes: makeStat(),
    max_mem_usage: makeStat(),
    contention_time: makeStat(),
    network_messages: makeStat(),
    max_disk_usage: makeStat(),
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
        data: protos.cockroach.server.serverpb.StatementsResponse.fromObject({
          statements,
          last_reset: {
            seconds: lastReset,
            nanos: 0,
          },
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
      "timeScale/SQLActivity": timeScale,
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
        data: protos.cockroach.server.serverpb.StatementDetailsResponse.fromObject(
          {
            statement: stmt.details.statement,
            statement_statistics_per_aggregated_ts: [],
            statement_statistics_per_plan_hash: [],
            internal_app_name_prefix: "$ internal",
          },
        ),
        inFlight: false,
        valid: true,
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

function makeRoutePropsWithSearchParams(params: { [key: string]: string }) {
  const history = H.createHashHistory();
  history.location.search = new URLSearchParams(params).toString();
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

function makeRoutePropsWithApp(app: string) {
  return makeRoutePropsWithSearchParams({
    [appAttr]: app,
  });
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
