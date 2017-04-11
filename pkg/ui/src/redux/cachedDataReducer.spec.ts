import { assert } from "chai";
import _ from "lodash";
import { Action } from "redux";
import moment from "moment";
import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./cachedDataReducer";

describe("basic cachedDataReducer", function () {
  class Request {
    constructor(public request: string) { }
  }

  class Response {
    constructor(public response: string) { }
  }

  let apiEndpointMock = (req = new Request(null)) => new Promise((resolve, _reject) => resolve(new Response(req.request)));

  let expected: CachedDataReducerState<Response>;

  describe("reducerObj", function () {
    let actionNamespace = "test";
    let testReducerObj = new CachedDataReducer<Request, Response>(apiEndpointMock, actionNamespace);

    describe("actions", function () {
      it("requestData() creates the correct action type.", function () {
        assert.equal(testReducerObj.requestData().type, testReducerObj.REQUEST);
      });

      it("receiveData() creates the correct action type.", function () {
        assert.equal(testReducerObj.receiveData(null).type, testReducerObj.RECEIVE);
      });

      it("errorData() creates the correct action type.", function () {
        assert.equal(testReducerObj.errorData(null).type, testReducerObj.ERROR);
      });

      it("invalidateData() creates the correct action type.", function () {
        assert.equal(testReducerObj.invalidateData().type, testReducerObj.INVALIDATE);
      });
    });

    let reducer = testReducerObj.reducer;
    let testMoment = moment();
    testReducerObj.setTimeSource(() => testMoment);

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
        state = reducer(state, testReducerObj.requestData());
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
        expected.setAt = testMoment;
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

      let mockDispatch = <A extends Action>(action: A): A => {
        state.cachedData.test = testReducerObj.reducer(state.cachedData.test, action);
        return undefined;
      };

      it("correctly dispatches refresh", function () {
        state = {
          cachedData: {
            test: new CachedDataReducerState<Response>(),
          },
        };

        let testString = "refresh test string";

        return testReducerObj.refresh(new Request(testString))(mockDispatch, () => state).then(() => {
          expected = new CachedDataReducerState<Response>();
          expected.valid = true;
          expected.data = new Response(testString);
          expected.setAt = testMoment;
          expected.lastError = null;
          assert.deepEqual(state.cachedData.test, expected);
        });
      });
    });
  });

  describe("multiple reducer objects", function () {
    it("should throw an error if the same actionNamespace is used twice", function () {
      // tslint:disable-next-line:no-unused-expression
      new CachedDataReducer<Request, Response>(apiEndpointMock, "duplicatenamespace");
      try {
        // tslint:disable-next-line:no-unused-expression
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
    constructor(public request: string) { }
  }

  class Response {
    constructor(public response: string) { }
  }

  let apiEndpointMock = (req = new Request(null)) => new Promise((resolve, _reject) => resolve(new Response(req.request)));

  let requestToID = (req: Request) => req.request;

  let expected: KeyedCachedDataReducerState<Response>;

  describe("reducerObj", function () {
    let actionNamespace = "keyedTest";
    let testReducerObj = new KeyedCachedDataReducer<Request, Response>(apiEndpointMock, actionNamespace, requestToID);

    describe("actions", function () {
      it("requestData() creates the correct action type.", function () {
        let request = new Request("testRequestRequest");
        let requestAction = testReducerObj.cachedDataReducer.requestData(request);
        assert.equal(requestAction.type, testReducerObj.cachedDataReducer.REQUEST);
        assert.deepEqual(requestAction.payload, {request});
      });

      it("receiveData() creates the correct action type.", function () {
        let response = new Response("testResponse");
        let request = new Request("testResponseRequest");
        let receiveAction = testReducerObj.cachedDataReducer.receiveData(response, request);
        assert.equal(receiveAction.type, testReducerObj.cachedDataReducer.RECEIVE);
        assert.deepEqual(receiveAction.payload, {request, data: response});
      });

      it("errorData() creates the correct action type.", function () {
        let error = new Error();
        let request = new Request("testErrorRequest");
        let errorAction = testReducerObj.cachedDataReducer.errorData(error, request);
        assert.equal(errorAction.type, testReducerObj.cachedDataReducer.ERROR);
        assert.deepEqual(errorAction.payload, {request, data: error});
      });

      it("invalidateData() creates the correct action type.", function () {
        let request = new Request("testInvalidateRequest");
        let invalidateAction = testReducerObj.cachedDataReducer.invalidateData(request);
        assert.equal(invalidateAction.type, testReducerObj.cachedDataReducer.INVALIDATE);
        assert.deepEqual(invalidateAction.payload, {request});
      });
    });

    let reducer = testReducerObj.reducer;
    let testMoment = moment();
    testReducerObj.setTimeSource(() => testMoment);

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
        state = reducer(state, testReducerObj.cachedDataReducer.requestData(request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].inFlight = true;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch receiveData", function () {
        let expectedResponse = new Response(null);

        state = reducer(state, testReducerObj.cachedDataReducer.receiveData(expectedResponse, request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].valid = true;
        expected[id].data = expectedResponse;
        expected[id].lastError = null;
        expected[id].setAt = testMoment;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch errorData", function() {
        let e = new Error();

        state = reducer(state, testReducerObj.cachedDataReducer.errorData(e, request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].lastError = e;
        assert.deepEqual(state, expected);
      });

      it("should correctly dispatch invalidateData", function() {
        state = reducer(state, testReducerObj.cachedDataReducer.invalidateData(request));
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        assert.deepEqual(state, expected);
      });
    });
  });
});
