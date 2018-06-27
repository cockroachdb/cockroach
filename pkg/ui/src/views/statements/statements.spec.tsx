import { assert } from "chai";
import Long from "long";
import moment from "moment";

import "src/protobufInit";
import * as protos from "src/js/protos";
import { CollectedStatementStatistics } from "src/util/appStats";
import { appAttr, statementAttr } from "src/util/constants";
import { selectStatements, selectApps, selectTotalFingerprints, selectLastReset } from "./statementsPage";
import { selectStatement } from "./statementDetails";

describe("selectStatements", () => {
  it("returns null if the queries data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.isNull(result);
  });

  it("returns the statements currently loaded", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, "foobar");
    const stmtC = makeFingerprint(3, "another");
    const state = makeStateWithQueries([stmtA, stmtB, stmtC]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 3);

    const expectedFingerprints = [stmtA, stmtB, stmtC].map(stmt => stmt.key.query);
    expectedFingerprints.sort();
    const actualFingerprints = result.map(stmt => stmt.statement);
    actualFingerprints.sort();
    assert.deepEqual(actualFingerprints, expectedFingerprints);
  });

  it("coalesces statements from different apps", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(1, "foobar");
    const stmtC = makeFingerprint(1, "another");
    const sumCount = stmtA.stats.count.add(stmtB.stats.count.add(stmtC.stats.count)).toNumber();
    const state = makeStateWithQueries([stmtA, stmtB, stmtC]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
    assert.equal(result[0].statement, stmtA.key.query);
    assert.equal(result[0].stats.count.toNumber(), sumCount);
  });

  it("coalesces statements with differing distSQL and failed values", () => {
    const state = makeStateWithQueries([
      makeFingerprint(1, "", false, false),
      makeFingerprint(1, "", false, true),
      makeFingerprint(1, "", true, false),
      makeFingerprint(1, "", true, true),
    ]);
    const props = makeEmptyRouteProps();

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("filters out statements when app param is set", () => {
    const state = makeStateWithQueries([
      makeFingerprint(1, "foo"),
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithApp("foo");

    const result = selectStatements(state, props);

    assert.equal(result.length, 1);
  });

  it("filters out statements with app set when app param is \"(unset)\"", () => {
    const state = makeStateWithQueries([
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
  it("returns an empty array if the queries data is invalid", () => {
    const state = makeInvalidState();

    const result = selectApps(state);

    assert.deepEqual(result, []);
  });

  it("returns all the apps that appear in the statements", () => {
    const state = makeStateWithQueries([
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
  it("returns zero if the queries data is invalid", () => {
    const state = makeInvalidState();

    const result = selectTotalFingerprints(state);

    assert.equal(result, 0);
  });

  it("returns the number of statement fingerprints", () => {
    const state = makeStateWithQueries([
      makeFingerprint(1),
      makeFingerprint(2),
      makeFingerprint(3),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 3);
  });

  it("coalesces statements from different apps", () => {
    const state = makeStateWithQueries([
      makeFingerprint(1),
      makeFingerprint(1, "foobar"),
      makeFingerprint(1, "another"),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });

  it("coalesces statements with differing distSQL and failed keys", () => {
    const state = makeStateWithQueries([
      makeFingerprint(1, "", false, false),
      makeFingerprint(1, "", false, true),
      makeFingerprint(1, "", true, false),
      makeFingerprint(1, "", true, true),
    ]);

    const result = selectTotalFingerprints(state);

    assert.equal(result, 1);
  });
});

describe("selectLastReset", () => {
  it("returns \"unknown\" if the queries data is invalid", () => {
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
  it("returns null if the queries data is invalid", () => {
    const state = makeInvalidState();
    const props = makeEmptyRouteProps();

    const result = selectStatement(state, props);

    assert.isNull(result);
  });

  it("returns the statement currently loaded", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(2, "foobar");
    const stmtC = makeFingerprint(3, "another");
    const state = makeStateWithQueries([stmtA, stmtB, stmtC]);
    const props = makeRoutePropsWithStatement(stmtA.key.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.app]);
    assert.deepEqual(result.distSQL, [stmtA.key.distSQL]);
    assert.deepEqual(result.failed, [stmtA.key.failed]);
  });

  it("coalesces statements from different apps", () => {
    const stmtA = makeFingerprint(1);
    const stmtB = makeFingerprint(1, "foobar");
    const stmtC = makeFingerprint(1, "another");
    const sumCount = stmtA.stats.count.add(stmtB.stats.count.add(stmtC.stats.count)).toNumber();
    const state = makeStateWithQueries([stmtA, stmtB, stmtC]);
    const props = makeRoutePropsWithStatement(stmtA.key.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.query);
    assert.equal(result.stats.count.toNumber(), sumCount);
    assert.deepEqual(result.app, [stmtA.key.app, stmtB.key.app, stmtC.key.app]);
    assert.deepEqual(result.distSQL, [stmtA.key.distSQL]);
    assert.deepEqual(result.failed, [stmtA.key.failed]);
  });

  it("coalesces statements with differing distSQL and failed values", () => {
    const stmtA = makeFingerprint(1, "", false, false);
    const stmtB = makeFingerprint(1, "", false, true);
    const stmtC = makeFingerprint(1, "", true, false);
    const stmtD = makeFingerprint(1, "", true, true);
    const sumCount = stmtA.stats.count
      .add(stmtB.stats.count)
      .add(stmtC.stats.count)
      .add(stmtD.stats.count)
      .toNumber();
    const state = makeStateWithQueries([stmtA, stmtB, stmtC, stmtD]);
    const props = makeRoutePropsWithStatement(stmtA.key.query);

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.query);
    assert.equal(result.stats.count.toNumber(), sumCount);
    assert.deepEqual(result.app, [stmtA.key.app]);
    assert.deepEqual(result.distSQL, [false, true]);
    assert.deepEqual(result.failed, [false, true]);
  });

  it("filters out statements when app param is set", () => {
    const stmtA = makeFingerprint(1, "foo");
    const state = makeStateWithQueries([
      stmtA,
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithStatementAndApp(stmtA.key.query, "foo");

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.app]);
    assert.deepEqual(result.distSQL, [stmtA.key.distSQL]);
    assert.deepEqual(result.failed, [stmtA.key.failed]);
  });

  it("filters out statements with app set when app param is \"(unset)\"", () => {
    const stmtA = makeFingerprint(1, "");
    const state = makeStateWithQueries([
      stmtA,
      makeFingerprint(2, "bar"),
      makeFingerprint(3, "baz"),
    ]);
    const props = makeRoutePropsWithStatementAndApp(stmtA.key.query, "(unset)");

    const result = selectStatement(state, props);

    assert.equal(result.statement, stmtA.key.query);
    assert.equal(result.stats.count.toNumber(), stmtA.stats.count.toNumber());
    assert.deepEqual(result.app, [stmtA.key.app]);
    assert.deepEqual(result.distSQL, [stmtA.key.distSQL]);
    assert.deepEqual(result.failed, [stmtA.key.failed]);
  });
});

function makeFingerprint(id: number, app: string = "", distSQL: boolean = false, failed: boolean = false) {
  return {
    key: {
      query: "SELECT * FROM table_" + id + " WHERE true",
      app,
      distSQL,
      failed,
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
  };
}

function makeStat() {
  return {
    mean: 10,
    squared_diffs: 1,
  };
}

function makeInvalidState() {
  return {
    cachedData: {
      queries: {
        inFlight: true,
        valid: false,
      },
    },
  };
}

function makeStateWithQueriesAndLastReset(queries: CollectedStatementStatistics[], lastReset: number) {
  return {
    cachedData: {
      queries: {
        data: protos.cockroach.server.serverpb.QueriesResponse.fromObject({
          queries,
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

function makeStateWithQueries(queries: CollectedStatementStatistics[]) {
  return makeStateWithQueriesAndLastReset(queries, 0);
}

function makeStateWithLastReset(lastReset: number) {
  return makeStateWithQueriesAndLastReset([], lastReset);
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
