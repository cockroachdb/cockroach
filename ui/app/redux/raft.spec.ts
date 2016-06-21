import { assert } from "chai";
import * as fetchMock from "fetch-mock";

import * as protos from "../js/protos";
import reducer, * as raft from "./raft";
import { Action } from "../interfaces/action";

describe("raft reducer", function() {
  describe("actions", function() {
    it("requestMetrics() creates the correct action type.", function() {
      assert.equal(raft.requestRanges().type, raft.REQUEST);
    });

    it("receiveRanges() creates the correct action type.", function() {
      assert.equal(raft.receiveRanges(null).type, raft.RECEIVE);
    });

    it("errorRanges() creates the correct action type.", function() {
      assert.equal(raft.errorRanges(null).type, raft.ERROR);
    });

    it("invalidateRanges() creates the correct action type.", function() {
      assert.equal(raft.invalidateRanges().type, raft.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: raft.RaftDebugState;

    beforeEach(function() {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      let expected = {
        inFlight: false,
        isValid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestRanges", function() {
      state = reducer(state, raft.requestRanges());
      assert.isTrue(state.inFlight);
    });

    it("should correctly dispatch receiveRanges", function() {
      let response = new protos.cockroach.server.serverpb.RaftDebugResponse({});
      state = reducer(state, raft.receiveRanges(response));
      assert.isNotTrue(state.inFlight);
      assert.isTrue(state.isValid);
      assert.deepEqual(state.statuses, response);
      assert.isNull(state.lastError);
    });

    it("should correctly dispatch errorRanges", function() {
      let error: Error = new Error("An error occurred");
      state = reducer(state, raft.errorRanges(error));
      assert.isNotTrue(state.inFlight);
      assert.isNotTrue(state.isValid);
      assert.deepEqual(state.lastError, error);
    });
  });

  describe("refreshRaft asynchronous action", function() {
    // Mock of raft state.
    let mockDebugState: raft.RaftDebugState;
    let mockDispatch = (action: Action) => {
      mockDebugState = reducer(mockDebugState, action);
    };
    let refreshRaft = (): Promise<void> => {
      return raft.refreshRaft()(mockDispatch, () => {
        return {raft: mockDebugState};
      });
    };

    beforeEach(function() {
      mockDebugState = new raft.RaftDebugState();
    });

    afterEach(fetchMock.restore);

    it("correctly responds to errors.", function() {
      this.timeout(1000);

      // Mock out fetch server; send a positive reply to the first request, and
      // an error to the second request.
      let successSent = false;
      fetchMock.mock("/_status/raft", "get", (url: string) => {
          // Assert that metric store's "inFlight" is 1.
          assert.isTrue(mockDebugState.inFlight);

          if (successSent) {
            return { throws: new Error() };
          }
          successSent = true;
          return {
            sendAsJson: false,
            body: new protos.cockroach.server.serverpb.RaftDebugResponse().toArrayBuffer(),
          };
      });

      // Dispatch several requests.
      let p = refreshRaft();
      assert.isTrue(mockDebugState.inFlight);
      return p.then((): Promise<void> => {
        assert.isNull(mockDebugState.lastError);
        assert.isTrue(mockDebugState.isValid);
        assert.isNotTrue(mockDebugState.inFlight);

        mockDebugState.isValid = false;

        // second request should throw an error
        let p2 = refreshRaft();
        assert.isTrue(mockDebugState.inFlight);
        return p2;
      }).then(() => {
        // Assert that the server got the correct number of requests (2).
        assert.lengthOf(fetchMock.calls("/_status/raft"), 2);
        assert.isNotNull(mockDebugState.lastError);
        assert.isNotTrue(mockDebugState.inFlight);
      });
    });
  });
});
