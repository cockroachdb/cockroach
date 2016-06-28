import { assert } from "chai";

import * as protos from "../js/protos";
import { eventsReducerObj as events, EventsState } from "./apiReducers";

let reducer = events.reducer;

describe("events reducer", function() {
  describe("actions", function() {
    it("requestData() creates the correct action type.", function() {
      assert.equal(events.requestData().type, events.REQUEST);
    });

    it("receiveData() creates the correct action type.", function() {
      assert.equal(events.receiveData(null).type, events.RECEIVE);
    });

    it("errorData() creates the correct action type.", function() {
      assert.equal(events.errorData(null).type, events.ERROR);
    });

    it("invalidateData() creates the correct action type.", function() {
      assert.equal(events.invalidateData().type, events.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: EventsState;

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

    it("should correctly dispatch requestData", function () {
      state = reducer(state, events.requestData());
      let expected = {
        inFlight: true,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch receiveData", function () {
      let e = new protos.cockroach.server.serverpb.EventsResponse();

      state = reducer(state, events.receiveData(e));
      let expected = {
        inFlight: false,
        valid: true,
        data: e,
        lastError: <any>null,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch errorData", function() {
      let e = new Error();

      state = reducer(state, events.errorData(e));
      let expected = {
        inFlight: false,
        valid: false,
        lastError: e,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch invalidateData", function() {
      state = reducer(state, events.invalidateData());
      let expected = {
        inFlight: false,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });
  });
});
