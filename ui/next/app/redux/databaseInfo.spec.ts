import { assert } from "chai";

import reducer, * as databases from "./databases";

type TSRequest = cockroach.ts.TimeSeriesQueryRequest;
type TSRequestMessage = cockroach.ts.TimeSeriesQueryRequestMessage;
type TSResponse = cockroach.ts.TimeSeriesQueryResponse;

describe("databases reducer", function() {
  describe("actions", function() {
    it("requestMetrics() creates the correct action type.", function() {
      assert.equal(databases.requestDatabaseList().type, databases.REQUEST);
    });

    it("receiveMetrics() creates the correct action type.", function() {
      assert.equal(databases.receiveDatabaseList(null).type, databases.RECEIVE);
    });

    it("errorMetrics() creates the correct action type.", function() {
      assert.equal(databases.errorDatabaseList(null).type, databases.ERROR);
    });
  });

  describe("reducer", function () {
    let state: databases.DatabaseListState;

    beforeEach(() => {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function () {
      let expected = {
        inFlight: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestDatabaseList", function () {
      state = reducer(state, databases.requestDatabaseList());
      assert.isTrue(state.inFlight);
      assert.isUndefined(state.lastError);
      assert.isUndefined(state.databaseList);
    });

    it("should correctly dispatch receiveDatabaseList", function () {
      let dbList = { databases: [] as string[] };
      state = reducer(state, databases.receiveDatabaseList(dbList));
      assert.isFalse(state.inFlight);
      assert.isNull(state.lastError);
      assert.deepEqual(state.databaseList, dbList);
    });

    it("should correctly dispatch errorDatabaseList", function () {
      let dbErr = new Error();
      state = reducer(state, databases.errorDatabaseList(dbErr));
      assert.isFalse(state.inFlight);
      assert.isUndefined(state.databaseList);
      assert.deepEqual(state.lastError, dbErr);
    });
  });

  // TODO (maxlang): add tests for refreshDatabaseList

});
