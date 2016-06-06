import { assert } from "chai";

import * as protos from "../js/protos";
import reducer, * as events from "./events";

describe("events reducer", function() {
  describe("actions", function() {
    it("requestEvents() creates the correct action type.", function() {
      assert.equal(events.requestEvents().type, events.REQUEST);
    });

    it("receiveEvents() creates the correct action type.", function() {
      assert.equal(events.receiveEvents(null).type, events.RECEIVE);
    });

    it("errorEvents() creates the correct action type.", function() {
      assert.equal(events.errorEvents(null).type, events.ERROR);
    });

    it("invalidateEvents() creates the correct action type.", function() {
      assert.equal(events.invalidateEvents().type, events.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: events.EventsState;

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

    it("should correctly dispatch requestEvents", function () {
      state = reducer(state, events.requestEvents());
      let expected = {
        inFlight: true,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch receiveEvents", function () {
      let e = new protos.cockroach.server.serverpb.EventsResponse();

      state = reducer(state, events.receiveEvents(e));
      let expected = {
        inFlight: false,
        valid: true,
        data: e,
        lastError: <any>null,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch errorEvents", function() {
      let e = new Error();

      state = reducer(state, events.errorEvents(e));
      let expected = {
        inFlight: false,
        valid: false,
        lastError: e,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch invalidateEvents", function() {
      state = reducer(state, events.invalidateEvents());
      let expected = {
        inFlight: false,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });
  });
});
