import { assert } from "chai";
import _ = require("lodash");
import { CachedDataReducer, CachedDataReducerState } from "./cachedDataReducer";
import { Action } from "../interfaces/action";

class Request {
  constructor(public request: string) { };
}

class Response {
  constructor(public response: string) { };
}

let apiEndpointMock = (req = new Request(null)) => new Promise((resolve, reject) => resolve(new Response(req.request)));

let expected: CachedDataReducerState<Response>;

describe("reducerObj", function () {
  let actionNamespace = "test";
  let testReducerObj = new CachedDataReducer<Request, Response>(apiEndpointMock, actionNamespace);

  describe("actions", function () {
    it("requestData() creates the correct action type.", function() {
      assert.equal(testReducerObj.requestData().type, testReducerObj.REQUEST);
    });

    it("receiveData() creates the correct action type.", function() {
      assert.equal(testReducerObj.receiveData(null).type, testReducerObj.RECEIVE);
    });

    it("errorData() creates the correct action type.", function() {
      assert.equal(testReducerObj.errorData(null).type, testReducerObj.ERROR);
    });

    it("invalidateData() creates the correct action type.", function() {
      assert.equal(testReducerObj.invalidateData().type, testReducerObj.INVALIDATE);
    });
  });

  let reducer = testReducerObj.reducer;

  describe("reducer", function() {
    let state: CachedDataReducerState<Response>;
    beforeEach(() => {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      expected = new CachedDataReducerState<Response>();
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestData", function () {
      state = reducer(state, testReducerObj.requestData());
      expected = new CachedDataReducerState<Response>();
      expected.inFlight = true;
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch receiveData", function () {
      let expectedResponse = new Response(null);

      state = reducer(state, testReducerObj.receiveData(expectedResponse));
      expected = new CachedDataReducerState<Response>();
      expected.valid = true;
      expected.data = expectedResponse;
      expected.lastError = null;
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch errorData", function() {
      let e = new Error();

      state = reducer(state, testReducerObj.errorData(e));
      expected = new CachedDataReducerState<Response>();
      expected.lastError = e;
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch invalidateData", function() {
      state = reducer(state, testReducerObj.invalidateData());
      expected = new CachedDataReducerState<Response>();
      assert.deepEqual(state, expected);
    });
  });

  describe("refresh", function () {
    let state: {
      cachedData: {
        test: CachedDataReducerState<Response>;
      };
    };

    let dispatch = (action: Action) => {
      state.cachedData.test = testReducerObj.reducer(state.cachedData.test, action);
    };

    it("correctly dispatches refresh", function () {
      state = {
        cachedData: {
          test: new CachedDataReducerState<Response>(),
        },
      };

      let testString = "refresh test string";

      return testReducerObj.refresh(new Request(testString))(dispatch, () => state).then(() => {
        expected = new CachedDataReducerState<Response>();
        expected.valid = true;
        expected.data = new Response(testString);
        expected.lastError = null;
        assert.deepEqual(state.cachedData.test, expected);
      });
    });
  });
});

describe("multiple reducer objects", function () {
  it("should throw an error if the same actionNamespace is used twice", function () {
    new CachedDataReducer<Request, Response>(apiEndpointMock, "duplicatenamespace");
    try {
      new CachedDataReducer<Request, Response>(apiEndpointMock, "duplicatenamespace");
    } catch (e) {
      assert(_.isError(e));
      return;
    }
    assert.fail("Expected to fail after registering a duplicate actionNamespace.");
  });
});
