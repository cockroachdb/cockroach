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

import "src/protobufInit";
import * as protos from "src/js/protos";
import { CollectedStatementStatistics } from "src/util/appStats";
import { appAttr, statementAttr } from "src/util/constants";
import { selectStatements, selectApps, selectTotalFingerprints, selectLastReset } from "./statementsPage";
import { selectStatement } from "./statementDetails";
import ISensitiveInfo = protos.cockroach.sql.ISensitiveInfo;

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
    const state = makeStateWithStatements([stmtA, stmtB, stmtC]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 3);

    const expectedFingerprints = [stmtA, stmtB, stmtC].map(stmt => stmt.key.key_data.query);
    expectedFingerprints.sort();
    const actualFingerprints = result.map(stmt => stmt.label);
    actualFingerprints.sort();
    assert.deepEqual(actualFingerprints, expectedFingerprints);
  });

  it("coalesces statements from different apps", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(1, "foobar");
    const stmtC = makeFingerprint(1, "another");
    const sumCount = stmtA.stats.count.add(stmtB.stats.count.add(stmtC.stats.count)).toNumber();
    const state = makeStateWithStatements([stmtA, stmtB, stmtC]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
    assert.equal(result[0].label, stmtA.key.key_data.query);
    assert.equal(result[0].stats.count.toNumber(), sumCount);
  });

  it("coalesces statements with differing node ids", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, "", 1),
      makeFingerprint(1, "", 2),
      makeFingerprint(1, "", 3),
    ]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("coalesces statements with differing distSQL and failed values", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, "", 1, false, false),
      makeFingerprint(1, "", 1, false, true),
      makeFingerprint(1, "", 1, true, false),
      makeFingerprint(1, "", 1, true, true),
    ]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("filters out statements when app param is set", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, "foo"),
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithApp("foo");

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("filters out statements with app set when app param is \"(unset)\"", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, ""),
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithApp("(unset)");

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
    const state = makeStateWithStatements([
      makeFingerprint(1),
      makeFingerprint(1, "foobar"),
      makeFingerprint(2, "foobar"),
      makeFingerprint(3, "cockroach sql"),
    ]);

    const result = selectApps(state);

    assert.deepEqual(result, ["(unset)", "foobar", "cockroach sql"]);
  });
});

describe("selectTotalFingerprints", () => {
  it("returns zero if the statements data is invalid", () => {
    const state = makeInvalidState();

    const result = selectTotalFingerprints(state);

    assert.equal(result, 0);
  });

  it("returns the number of statement fingerprints", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1),
      makeFingerprint(2),
      makeFingerprint(3),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 3);
  });

  it("coalesces statements from different apps", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1),
      makeFingerprint(1, "foobar"),
      makeFingerprint(1, "another"),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });

  it("coalesces statements with differing node ids", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, "", 1),
      makeFingerprint(1, "", 2),
      makeFingerprint(1, "", 3),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });

  it("coalesces statements with differing distSQL and failed keys", () => {
    const state = makeStateWithStatements([
      makeFingerprint(1, "", 1, false, false),
      makeFingerprint(1, "", 1, false, true),
      makeFingerprint(1, "", 1, true, false),
      makeFingerprint(1, "", 1, true, true),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });
});

describe("selectLastReset", () => {
  it("returns \"unknown\" if the statements data is invalid", () => {
    const state = makeInvalidState();

    const result = selectLastReset(state);

    assert.equal(result, "unknown");
  });

  it("returns the formatted timestamp if valid", () => {
    const timestamp = 92951700;
    const state = makeStateWithLastReset(timestamp);

    const result = selectLastReset(state);

    assert.equal(moment.utc(result).unix(), timestamp);
  });
});

describe("selectStatement", () => {
  it("returns null if the statements data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();

    const result = selectStatement(state, props);

    assert.isNull(result);
  });

  it("returns the statement currently loaded", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, "foobar");
    const stmtC = makeFingerprint(3, "another");
    const state = makeStateWithStatements([stmtA, stmtB, stmtC]);
    const props = makeRoutePropsWithStatement(stmtA.key.key_data.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.opt, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.failed, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.node_id, [stmtA.key.node_id]);
  });

  it("coalesces statements from different apps", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(1, "foobar");
    const stmtC = makeFingerprint(1, "another");
    const sumCount = stmtA.stats.count.add(stmtB.stats.count.add(stmtC.stats.count)).toNumber();
    const state = makeStateWithStatements([stmtA, stmtB, stmtC]);
    const props = makeRoutePropsWithStatement(stmtA.key.key_data.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), sumCount);
    assert.deepEqual(result.app, [stmtA.key.key_data.app, stmtB.key.key_data.app, stmtC.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.opt, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.failed, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.node_id, [stmtA.key.node_id]);
  });

  it("coalesces statements with differing node ids", () => {
    const stmtA = makeFingerprint(1, "", 1);
    const stmtB = makeFingerprint(1, "", 2);
    const stmtC = makeFingerprint(1, "", 3);
    const sumCount = stmtA.stats.count
      .add(stmtB.stats.count)
      .add(stmtC.stats.count)
      .toNumber();
    const state = makeStateWithStatements([stmtA, stmtB, stmtC]);
    const props = makeRoutePropsWithStatement(stmtA.key.key_data.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), sumCount);
    assert.deepEqual(result.app, [stmtA.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.opt, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.failed, { numerator: 0, denominator: 3 });
    assert.deepEqual(result.node_id, [1, 2, 3]);
  });

  it("coalesces statements with differing distSQL, opt and failed values", () => {
    const stmtA = makeFingerprint(1, "", 1, false, false, false);
    const stmtB = makeFingerprint(1, "", 1, false, false, true);
    const stmtC = makeFingerprint(1, "", 1, false, true, false);
    const stmtD = makeFingerprint(1, "", 1, false, true, true);
    const stmtE = makeFingerprint(1, "", 1, true, false, false);
    const stmtF = makeFingerprint(1, "", 1, true, false, true);
    const stmtG = makeFingerprint(1, "", 1, true, true, false);
    const stmtH = makeFingerprint(1, "", 1, true, true, true);
    const sumCount = stmtA.stats.count
      .add(stmtB.stats.count)
      .add(stmtC.stats.count)
      .add(stmtD.stats.count)
      .add(stmtE.stats.count)
      .add(stmtF.stats.count)
      .add(stmtG.stats.count)
      .add(stmtH.stats.count)
      .toNumber();
    const state = makeStateWithStatements([stmtA, stmtB, stmtC, stmtD, stmtE, stmtF, stmtG, stmtH]);
    const props = makeRoutePropsWithStatement(stmtA.key.key_data.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), sumCount);
    assert.deepEqual(result.app, [stmtA.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 4, denominator: 8 });
    assert.deepEqual(result.opt, { numerator: 4, denominator: 8 });
    assert.deepEqual(result.failed, { numerator: 4, denominator: 8 });
    assert.deepEqual(result.node_id, [stmtA.key.node_id]);
  });

  it("filters out statements when app param is set", () => {
    const stmtA = makeFingerprint(1, "foo");
    const state = makeStateWithStatements([
      stmtA,
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithStatementAndApp(stmtA.key.key_data.query, "foo");

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.opt, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.failed, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.node_id, [stmtA.key.node_id]);
  });

  it("filters out statements with app set when app param is \"(unset)\"", () => {
    const stmtA = makeFingerprint(1, "");
    const state = makeStateWithStatements([
      stmtA,
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithStatementAndApp(stmtA.key.key_data.query, "(unset)");

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.key_data.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.key_data.app]);
    assert.deepEqual(result.distSQL, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.opt, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.failed, { numerator: 0, denominator: 1 });
    assert.deepEqual(result.node_id, [stmtA.key.node_id]);
  });
});

function makeFingerprint(id: number, app: string = "", nodeId: number = 1, distSQL: boolean = false, failed: boolean = false, opt: boolean = false) {
  return {
    key: {
      key_data: {
        query: "SELECT * FROM table_" + id + " WHERE true",
        app,
        distSQL,
        opt,
        failed,
      },
      node_id: nodeId,
    },
    stats: makeStats(),
  };
}

let makeStatsIndex = 1;
function makeStats() {
  return {
    count: Long.fromNumber(makeStatsIndex++),
    first_attempt_count: Long.fromNumber(1),
    max_retries: Long.fromNumber(0),
    num_rows: makeStat(),
    parse_lat: makeStat(),
    plan_lat: makeStat(),
    run_lat: makeStat(),
    overhead_lat: makeStat(),
    service_lat: makeStat(),
    sensitive_info: makeEmptySensitiveInfo(),
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

function makeInvalidState() {
  return {
    cachedData: {
      statements: {
        inFlight: true,
        valid: false,
      },
    },
  };
}

function makeStateWithStatementsAndLastReset(statements: CollectedStatementStatistics[], lastReset: number) {
  return {
    cachedData: {
      statements: {
        data: protos.cockroach.server.serverpb.StatementsResponse.fromObject({
          statements,
          last_reset: {
            seconds: lastReset,
            nanos: 0,
          },
        }),
        inFlight: false,
        valid: true,
      },
    },
  };
}

function makeStateWithStatements(statements: CollectedStatementStatistics[]) {
  return makeStateWithStatementsAndLastReset(statements, 0);
}

function makeStateWithLastReset(lastReset: number) {
  return makeStateWithStatementsAndLastReset([], lastReset);
}

function makeRoutePropsWithParams(params: { [key: string]: string }) {
  return {
    params,
  };
}

function makeEmptyRouteProps() {
  return makeRoutePropsWithParams({});
}

function makeRoutePropsWithApp(app: string) {
  return makeRoutePropsWithParams({
    [appAttr]: app,
  });
}

function makeRoutePropsWithStatement(stmt: string) {
  return makeRoutePropsWithParams({
    [statementAttr]: stmt,
  });
}

function makeRoutePropsWithStatementAndApp(stmt: string, app: string) {
  return makeRoutePropsWithParams({
    [appAttr]: app,
    [statementAttr]: stmt,
  });
}
