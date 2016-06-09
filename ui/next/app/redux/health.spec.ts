import { assert } from "chai";

import * as protos from "../js/protos";
import reducer, * as health from "./health";

describe("health reducer", function() {
  describe("actions", function() {
    it("requestHealth() creates the correct action type.", function() {
      assert.equal(health.requestHealth().type, health.REQUEST);
    });

    it("receiveHealth() creates the correct action type.", function() {
      assert.equal(health.receiveHealth(null).type, health.RECEIVE);
    });

    it("errorHealth() creates the correct action type.", function() {
      assert.equal(health.errorHealth(null).type, health.ERROR);
    });

    it("invalidateHealth() creates the correct action type.", function() {
      assert.equal(health.invalidateHealth().type, health.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: health.HealthState;

    beforeEach(() => {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      let expected = {
        inFlight: false,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestHealth", function () {
      state = reducer(state, health.requestHealth());
      let expected = {
        inFlight: true,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch receiveHealth", function () {
      let e = new protos.cockroach.server.serverpb.HealthResponse();

      state = reducer(state, health.receiveHealth(e));
      let expected = {
        inFlight: false,
        valid: true,
        data: e,
        lastError: <any>null,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch errorHealth", function() {
      let e = new Error();

      state = reducer(state, health.errorHealth(e));
      let expected = {
        inFlight: false,
        valid: false,
        lastError: e,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch invalidateHealth", function() {
      state = reducer(state, health.invalidateHealth());
      let expected = {
        inFlight: false,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });
  });
});
