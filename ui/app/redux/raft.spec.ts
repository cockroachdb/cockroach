import { assert } from "chai";
import * as fetchMock from "fetch-mock";

import * as protos from "../js/protos";
import { Action } from "../interfaces/action";
import { raftReducerObj as raft, RaftDebugState } from "./apiReducers";
import { CachedDataReducerState } from "./cachedDataReducer";

type RaftDebugResponseMessage = cockroach.server.serverpb.RaftDebugResponseMessage;

let reducer = raft.reducer;

describe("raft reducer", function() {
  describe("actions", function() {
    it("requestData() creates the correct action type.", function() {
      assert.equal(raft.requestData().type, raft.REQUEST);
    });

    it("receiveData() creates the correct action type.", function() {
      assert.equal(raft.receiveData(null).type, raft.RECEIVE);
    });

    it("errorData() creates the correct action type.", function() {
      assert.equal(raft.errorData(null).type, raft.ERROR);
    });

    it("invalidateData() creates the correct action type.", function() {
      assert.equal(raft.invalidateData().type, raft.INVALIDATE);
    });
  });

  describe("reducer", function() {
    let state: RaftDebugState;

    beforeEach(function() {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      let expected = {
        inFlight: false,
        valid: false,
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestData", function() {
      state = reducer(state, raft.requestData());
      assert.isTrue(state.inFlight);
    });

    it("should correctly dispatch receiveData", function() {
      let response = new protos.cockroach.server.serverpb.RaftDebugResponse({});
      state = reducer(state, raft.receiveData(response));
      assert.isNotTrue(state.inFlight);
      assert.isTrue(state.valid);
      assert.deepEqual(state.data, response);
      assert.isNull(state.lastError);
    });

    it("should correctly dispatch errorData", function() {
      let error: Error = new Error("An error occurred");
      state = reducer(state, raft.errorData(error));
      assert.isNotTrue(state.inFlight);
      assert.isNotTrue(state.valid);
      assert.deepEqual(state.lastError, error);
    });
  });

  describe("refresh asynchronous action", function() {
    // Mock of raft state.
    let mockDebugState: RaftDebugState;
    let mockDispatch = (action: Action) => {
      mockDebugState = reducer(mockDebugState, action);
    };
    let refresh = (): Promise<void> => {
      return raft.refresh()(mockDispatch, () => {
        return {raft: mockDebugState};
      });
    };

    beforeEach(function() {
      mockDebugState = new CachedDataReducerState<RaftDebugResponseMessage>();
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
      let p = refresh();
      assert.isTrue(mockDebugState.inFlight);
      return p.then((): Promise<void> => {
        assert.isNull(mockDebugState.lastError);
        assert.isTrue(mockDebugState.valid);
        assert.isNotTrue(mockDebugState.inFlight);

        mockDebugState.valid = false;

        // second request should throw an error
        let p2 = refresh();
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
