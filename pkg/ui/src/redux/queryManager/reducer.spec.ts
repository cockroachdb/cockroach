import { assert } from "chai";
import moment from "moment";

import {
  managedQueryReducer,
  ManagedQueryState,
  queryBegin,
  queryComplete,
  queryError,
  queryManagerReducer,
  QueryManagerState,
} from "./reducer";

describe("Query Manager State", function () {
  describe("managed query reducer", function () {
    const testMoment = moment();
    const testError = new Error("err");
    let state: ManagedQueryState;

    beforeEach(function() {
      state = managedQueryReducer(undefined, undefined);
    });

    it("has the correct initial state", function () {
      assert.deepEqual(state, new ManagedQueryState());
    });

    it("dispatches queryBegin", function () {
      const expected = new ManagedQueryState();
      expected.isRunning = true;
      expected.lastError = null;
      expected.queriedAt = null;

      state = managedQueryReducer(state, queryBegin("ID"));
      assert.deepEqual(state, expected);
    });

    it("dispatches queryError", function () {
      const expected = new ManagedQueryState();
      expected.isRunning = false;
      expected.lastError = testError;
      expected.queriedAt = testMoment;

      state = managedQueryReducer(state, queryBegin("ID"));
      state = managedQueryReducer(state, queryError("ID", testError, testMoment));
      assert.deepEqual(state, expected);
    });

    it("dispatches queryComplete", function () {
      const expected = new ManagedQueryState();
      expected.isRunning = false;
      expected.lastError = null;
      expected.queriedAt = testMoment;

      state = managedQueryReducer(state, queryBegin("ID"));
      state = managedQueryReducer(state, queryComplete("ID", testMoment));
      assert.deepEqual(state, expected);
    });

    it("clears error on queryBegin", function () {
      const expected = new ManagedQueryState();
      expected.isRunning = true;
      expected.lastError = null;
      expected.queriedAt = null;

      state = managedQueryReducer(state, queryError("ID", testError, testMoment));
      state = managedQueryReducer(state, queryBegin("ID"));
      assert.deepEqual(state, expected);
    });

    it("ignores unrecognized actions", function () {
      const origState = state;
      state = managedQueryReducer(state, { type: "unsupported" } as any);
      assert.equal(state, origState);
    });
  });

  describe("query manager reducer", function () {
    const testMoment = moment();
    const testError = new Error("err");
    let state: QueryManagerState;

    beforeEach(function() {
      state = queryManagerReducer(undefined, undefined);
    });

    it("has the correct initial value", function () {
      assert.deepEqual(state, {});
    });

    it("correctly dispatches based on ID", function () {
      const expected = {
        "1": managedQueryReducer(undefined, queryBegin("1")),
        "2": managedQueryReducer(undefined, queryError("2", testError, testMoment)),
        "3": managedQueryReducer(undefined, queryComplete("3", testMoment)),
      };

      state = queryManagerReducer(state, queryBegin("1"));
      state = queryManagerReducer(state, queryBegin("2"));
      state = queryManagerReducer(state, queryBegin("3"));
      state = queryManagerReducer(state, queryError("2", testError, testMoment));
      state = queryManagerReducer(state, queryError("3", testError, testMoment));
      state = queryManagerReducer(state, queryBegin("3"));
      state = queryManagerReducer(state, queryComplete("3", testMoment));

      assert.deepEqual(state, expected);
    });
  });
});
