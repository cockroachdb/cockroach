import { assert } from "chai";
// import _ = require("lodash");
// import * as fetchMock from "fetch-mock";
import { CachedDataReducerState } from "./cachedDataReducer";
import { KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./keyedCachedDataReducer";
// import * as protos from "../js/protos";
// import { Action } from "../interfaces/action";

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
  let testReducerObj = new KeyedCachedDataReducer<Request, Response>(apiEndpointMock, actionNamespace, idGenerator);

  describe("actions", function () {
    it("requestData() creates the correct action type.", function () {
      let requestAction = testReducerObj.requestData(new Request("testRequestRequest"));
      assert.equal(requestAction.type, testReducerObj.REQUEST);
      assert.deepEqual(requestAction.payload, {id: "testRequestRequest"});
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
      assert.deepEqual(invalidateAction.payload, {id: "testInvalidateRequest"});
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
