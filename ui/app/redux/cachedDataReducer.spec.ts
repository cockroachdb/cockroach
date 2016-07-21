import { assert } from "chai";
import _ = require("lodash");
import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducerState } from "./cachedDataReducer";
import { Action } from "../interfaces/action";

describe("basic cachedDataReducer", function () {
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
      it("requestData() creates the correct action type.", function () {
        assert.equal(testReducerObj.requestData(null).type, testReducerObj.REQUEST);
      });

      it("receiveData() creates the correct action type.", function () {
        assert.equal(testReducerObj.receiveData(null, null).type, testReducerObj.RECEIVE);
      });

      it("errorData() creates the correct action type.", function () {
        assert.equal(testReducerObj.errorData(null, null).type, testReducerObj.ERROR);
      });

      it("invalidateData() creates the correct action type.", function () {
        assert.equal(testReducerObj.invalidateData(null).type, testReducerObj.INVALIDATE);
      });
    });

    let reducer = testReducerObj.reducer;

    describe("reducer", function () {
      let state: CachedDataReducerState<Response>;
      beforeEach(() => {
        state = reducer(undefined, { type: "unknown" });
      });

      it("should have the correct default value.", function () {
        expected = new CachedDataReducerState<Response>();
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch requestData", function () {
        state = reducer(state, testReducerObj.requestData(null));
        expected = new CachedDataReducerState<Response>();
        expected.inFlight = true;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch receiveData", function () {
        let expectedResponse = new Response(null);

        state = reducer(state, testReducerObj.receiveData(expectedResponse, null));
        expected = new CachedDataReducerState<Response>();
        expected.valid = true;
        expected.data = expectedResponse;
        expected.lastError = null;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch errorData", function () {
        let e = new Error();

        state = reducer(state, testReducerObj.errorData(e, null));
        expected = new CachedDataReducerState<Response>();
        expected.lastError = e;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch invalidateData", function () {
        state = reducer(state, testReducerObj.invalidateData(null));
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
});

describe("keyed cachedDataReducer", function () {
  class Request {
    constructor(public request: string) { };
  }

  class Response {
    constructor(public response: string) { };
  }

  let apiEndpointMock = (req = new Request(null)) => new Promise((resolve, reject) => resolve(new Response(req.request)));

  let idGenerator = (req: Request) => req.request;

  let expected: KeyedCachedDataReducerState<Response>;

  describe("reducerObj", function () {
    let actionNamespace = "keyedTest";
    let testReducerObj = new CachedDataReducer<Request, Response>(apiEndpointMock, actionNamespace, idGenerator);

    describe("actions", function () {
      it("requestData() creates the correct action type.", function () {
        let requestAction = testReducerObj.requestData(new Request("testRequestRequest"));
        assert.equal(requestAction.type, testReducerObj.REQUEST);
        assert.deepEqual(requestAction.payload, {id: "testRequestRequest", data: undefined});
      });

      it("receiveData() creates the correct action type.", function () {
        let response = new Response("testResponse");
        let receiveAction = testReducerObj.receiveData(response, new Request("testResponseRequest"));
        assert.equal(receiveAction.type, testReducerObj.RECEIVE);
        assert.deepEqual(receiveAction.payload, {id: "testResponseRequest", data: response});
      });

      it("errorData() creates the correct action type.", function () {
        let error = new Error();
        let errorAction = testReducerObj.errorData(error, new Request("testErrorRequest"));
        assert.equal(errorAction.type, testReducerObj.ERROR);
        assert.deepEqual(errorAction.payload, {id: "testErrorRequest", data: error});
      });

      it("invalidateData() creates the correct action type.", function() {
        let invalidateAction = testReducerObj.invalidateData(new Request("testInvalidateRequest"));
        assert.equal(invalidateAction.type, testReducerObj.INVALIDATE);
        assert.deepEqual(invalidateAction.payload, {id: "testInvalidateRequest", data: undefined});
      });
    });

    let reducer = testReducerObj.keyedReducer;

    describe("keyed reducer", function () {
      let state: KeyedCachedDataReducerState<Response>;
      let id: string;
      let request: Request;
      beforeEach(() => {
        state = reducer(undefined, { type: "unknown" });
        id = Math.random().toString();
        request = new Request(id);
      });

      it("should have the correct default value.", function() {
        expected = new KeyedCachedDataReducerState<Response>();
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch requestData", function () {
        state = reducer(state, testReducerObj.requestData(request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].inFlight = true;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch receiveData", function () {
        let expectedResponse = new Response(null);

        state = reducer(state, testReducerObj.receiveData(expectedResponse, request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].valid = true;
        expected[id].data = expectedResponse;
        expected[id].lastError = null;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch errorData", function() {
        let e = new Error();

        state = reducer(state, testReducerObj.errorData(e, request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].lastError = e;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch invalidateData", function() {
        state = reducer(state, testReducerObj.invalidateData(request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        assert.deepEqual(state, expected);
      });
    });
  });
});
