// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import { Action } from "redux";
import moment from "moment";
import {
  CachedDataReducer,
  CachedDataReducerState,
  KeyedCachedDataReducer,
  KeyedCachedDataReducerState,
} from "./cachedDataReducer";

describe("basic cachedDataReducer", function() {
  class Request {
    constructor(public request: string) {}
  }

  class Response {
    constructor(public response: string) {}
  }

  const apiEndpointMock: (req: Request) => Promise<Response> = (
    req = new Request(null),
  ) => new Promise((resolve, _reject) => resolve(new Response(req.request)));

  let expected: CachedDataReducerState<Response>;

  describe("reducerObj", function() {
    const actionNamespace = "test";
    const testReducerObj = new CachedDataReducer<Request, Response>(
      apiEndpointMock,
      actionNamespace,
    );

    describe("actions", function() {
      it("requestData() creates the correct action type.", function() {
        expect(testReducerObj.requestData().type).toEqual(
          testReducerObj.REQUEST,
        );
      });

      it("receiveData() creates the correct action type.", function() {
        expect(testReducerObj.receiveData(null).type).toEqual(
          testReducerObj.RECEIVE,
        );
      });

      it("errorData() creates the correct action type.", function() {
        expect(testReducerObj.errorData(null).type).toEqual(
          testReducerObj.ERROR,
        );
      });

      it("invalidateData() creates the correct action type.", function() {
        expect(testReducerObj.invalidateData().type).toEqual(
          testReducerObj.INVALIDATE,
        );
      });
    });

    const reducer = testReducerObj.reducer;
    const testMoment = moment();
    testReducerObj.setTimeSource(() => testMoment);

    describe("reducer", function() {
      let state: CachedDataReducerState<Response>;
      beforeEach(() => {
        state = reducer(undefined, { type: "unknown" });
      });

      it("should have the correct default value.", function() {
        expected = new CachedDataReducerState<Response>();
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch requestData", function() {
        state = reducer(state, testReducerObj.requestData());
        expected = new CachedDataReducerState<Response>();
        expected.inFlight = true;
        expected.requestedAt = testMoment;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch receiveData", function() {
        const expectedResponse = new Response(null);
        state = reducer(
          state,
          testReducerObj.receiveData(expectedResponse, null),
        );
        expected = new CachedDataReducerState<Response>();
        expected.valid = true;
        expected.data = expectedResponse;
        expected.setAt = testMoment;
        expected.lastError = null;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch errorData", function() {
        const e = new Error();

        state = reducer(state, testReducerObj.errorData(e, null));
        expected = new CachedDataReducerState<Response>();
        expected.lastError = e;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch invalidateData", function() {
        state = reducer(state, testReducerObj.invalidateData());
        expected = new CachedDataReducerState<Response>();
        expect(state).toEqual(expected);
      });
    });

    describe("refresh", function() {
      let state: {
        cachedData: {
          test: CachedDataReducerState<Response>;
        };
      };

      const mockDispatch = <A extends Action>(action: A): A => {
        state.cachedData.test = testReducerObj.reducer(
          state.cachedData.test,
          action,
        );
        return undefined;
      };

      it("correctly dispatches refresh", function() {
        state = {
          cachedData: {
            test: new CachedDataReducerState<Response>(),
          },
        };

        const testString = "refresh test string";

        return testReducerObj
          .refresh(new Request(testString))(
            mockDispatch,
            () => state,
            undefined,
          )
          .then(() => {
            expected = new CachedDataReducerState<Response>();
            expected.valid = true;
            expected.data = new Response(testString);
            expected.requestedAt = testMoment;
            expected.setAt = testMoment;
            expected.lastError = null;
            expect(state.cachedData.test).toEqual(expected);
          });
      });
    });
  });

  describe("multiple reducer objects", function() {
    it("should throw an error if the same actionNamespace is used twice", function() {
      new CachedDataReducer<Request, Response>(
        apiEndpointMock,
        "duplicatenamespace",
      );
      try {
        new CachedDataReducer<Request, Response>(
          apiEndpointMock,
          "duplicatenamespace",
        );
      } catch (e) {
        expect(_.isError(e)).toBeTruthy();
        return;
      }
      expect(false).toBe(true);
    });
  });
});

describe("keyed cachedDataReducer", function() {
  class Request {
    constructor(public request: string) {}
  }

  class Response {
    constructor(public response: string) {}
  }

  const apiEndpointMock: (req: Request) => Promise<Response> = (
    req = new Request(null),
  ) => new Promise((resolve, _reject) => resolve(new Response(req.request)));

  const requestToID = (req: Request) => req.request;

  let expected: KeyedCachedDataReducerState<Response>;

  describe("reducerObj", function() {
    const actionNamespace = "keyedTest";
    const testReducerObj = new KeyedCachedDataReducer<Request, Response>(
      apiEndpointMock,
      actionNamespace,
      requestToID,
    );

    describe("actions", function() {
      it("requestData() creates the correct action type.", function() {
        const request = new Request("testRequestRequest");
        const requestAction = testReducerObj.cachedDataReducer.requestData(
          request,
        );
        expect(requestAction.type).toEqual(
          testReducerObj.cachedDataReducer.REQUEST,
        );
        expect(requestAction.payload).toEqual({ request });
      });

      it("receiveData() creates the correct action type.", function() {
        const response = new Response("testResponse");
        const request = new Request("testResponseRequest");
        const receiveAction = testReducerObj.cachedDataReducer.receiveData(
          response,
          request,
        );
        expect(receiveAction.type).toEqual(
          testReducerObj.cachedDataReducer.RECEIVE,
        );
        expect(receiveAction.payload).toEqual({ request, data: response });
      });

      it("errorData() creates the correct action type.", function() {
        const error = new Error();
        const request = new Request("testErrorRequest");
        const errorAction = testReducerObj.cachedDataReducer.errorData(
          error,
          request,
        );
        expect(errorAction.type).toEqual(
          testReducerObj.cachedDataReducer.ERROR,
        );
        expect(errorAction.payload).toEqual({ request, data: error });
      });

      it("invalidateData() creates the correct action type.", function() {
        const request = new Request("testInvalidateRequest");
        const invalidateAction = testReducerObj.cachedDataReducer.invalidateData(
          request,
        );
        expect(invalidateAction.type).toEqual(
          testReducerObj.cachedDataReducer.INVALIDATE,
        );
        expect(invalidateAction.payload).toEqual({ request });
      });
    });

    const reducer = testReducerObj.reducer;
    const testMoment = moment();
    testReducerObj.setTimeSource(() => testMoment);

    describe("keyed reducer", function() {
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
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch requestData", function() {
        state = reducer(
          state,
          testReducerObj.cachedDataReducer.requestData(request),
        );
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].requestedAt = testMoment;
        expected[id].inFlight = true;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch receiveData", function() {
        const expectedResponse = new Response(null);

        state = reducer(
          state,
          testReducerObj.cachedDataReducer.receiveData(
            expectedResponse,
            request,
          ),
        );
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].valid = true;
        expected[id].data = expectedResponse;
        expected[id].lastError = null;
        expected[id].setAt = testMoment;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch errorData", function() {
        const e = new Error();

        state = reducer(
          state,
          testReducerObj.cachedDataReducer.errorData(e, request),
        );
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expected[id].lastError = e;
        expect(state).toEqual(expected);
      });

      it("should correctly dispatch invalidateData", function() {
        state = reducer(
          state,
          testReducerObj.cachedDataReducer.invalidateData(request),
        );
        expected = new KeyedCachedDataReducerState<Response>();
        expected[id] = new CachedDataReducerState<Response>();
        expect(state).toEqual(expected);
      });
    });
  });
});
