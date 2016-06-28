import { assert } from "chai";

import * as protos from "../js/protos";
import { healthReducerObj as health, HealthState } from "./apiReducers";

let reducer = health.reducer;

describe("health reducer", function() {
  describe("actions", function() {
    it("requestData() creates the correct action type.", function() {
      assert.equal(health.requestData().type, health.REQUEST);
    });

    it("receiveData() creates the correct action type.", function() {
      assert.equal(health.receiveData(null).type, health.RECEIVE);
    });

    it("errorData() creates the correct action type.", function() {
      assert.equal(health.errorData(null).type, health.ERROR);
    });

    it("invalidateData() creates the correct action type.", function() {
      assert.equal(health.invalidateData().type, health.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: HealthState;

    beforeEach(() => {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      assert.deepEqual(state, {
        inFlight: false,
        valid: false,
      });
    });

    it("should correctly dispatch requestData", function () {
      state = reducer(state, health.requestData());
      assert.deepEqual(state, {
        inFlight: true,
        valid: false,
      });
    });

    it("should correctly dispatch receiveData", function () {
      let h = new protos.cockroach.server.serverpb.HealthResponse();

      state = reducer(state, health.receiveData(h));
      assert.deepEqual(state, {
        inFlight: false,
        valid: true,
        data: h,
        lastError: null,
      });
    });

    it("should correctly dispatch errorData", function() {
      let e = new Error();

      state = reducer(state, health.errorData(e));
      assert.deepEqual(state, {
        inFlight: false,
        valid: false,
        lastError: e,
      });
    });

    it("should correctly dispatch invalidateData", function() {
      state = reducer(state, health.invalidateData());
      assert.deepEqual(state, {
        inFlight: false,
        valid: false,
      });
    });
  });
});
