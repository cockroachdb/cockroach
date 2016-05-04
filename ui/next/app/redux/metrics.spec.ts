import { assert } from "chai";
import _ = require("lodash");
import Long = require("long");
import * as fetchMock from "fetch-mock";

import * as protos from "../js/protos";
import reducer, * as metrics from "./metrics";
import { Action } from "../interfaces/action";

type Query = cockroach.ts.Query;
type Request = cockroach.ts.TimeSeriesQueryRequest;
type Response = cockroach.ts.TimeSeriesQueryResponse;
type Result = cockroach.ts.TimeSeriesQueryResponse.Result;

describe("metrics reducer", function() {
  describe("actions", function() {
    it("requestMetrics() creates the correct action type.", function() {
      assert.equal(metrics.requestMetrics("id", null).type, metrics.REQUEST);
    });

    it("receiveMetrics() creates the correct action type.", function() {
      assert.equal(metrics.receiveMetrics("id", null).type, metrics.RECEIVE);
    });

    it("errorMetrics() creates the correct action type.", function() {
      assert.equal(metrics.errorMetrics("id", null).type, metrics.ERROR);
    });

    it("fetchMetrics() creates the correct action type.", function() {
      assert.equal(metrics.fetchMetrics().type, metrics.FETCH);
    });

    it("fetchMetrics() creates the correct action type.", function() {
      assert.equal(metrics.fetchMetricsComplete().type, metrics.FETCH_COMPLETE);
    });
  });

  describe("reducer", function() {
    let componentID = "test-component";
    let state: metrics.MetricQueryState;

    beforeEach(() => {
      state = reducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      let expected = {
        inFlight: 0,
        queries: metrics.metricQueriesReducer(undefined, { type: "unknown" }),
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestMetrics", function() {
      let request: Request =  {
        start_nanos: Long.fromInt(0),
        end_nanos: Long.fromInt(10),
        queries: [
          {
            name: "test.metric.1",
          },
          {
            name: "test.metric.2",
          },
        ],
      };
      state = reducer(state, metrics.requestMetrics(componentID, request));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].currentRequest, request);
      assert.isUndefined(state.queries[componentID].currentData);
      assert.isUndefined(state.queries[componentID].currentError);
    });

    it("should correctly dispatch receiveMetrics", function() {
      let response: Response = {
        results: [
          {
            datapoints: [],
          },
        ],
      };
      state = reducer(state, metrics.receiveMetrics(componentID, response));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].currentData, response);
      assert.isUndefined(state.queries[componentID].currentRequest);
      assert.isUndefined(state.queries[componentID].currentError);
    });

    it("should correctly dispatch errorMetrics", function() {
      let error: Error = new Error("An error occurred");
      state = reducer(state, metrics.errorMetrics(componentID, error));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].currentError, error);
      assert.isUndefined(state.queries[componentID].currentRequest);
      assert.isUndefined(state.queries[componentID].currentData);
    });

    it("should correctly dispatch fetchMetrics and fetchMetricsComplete", function() {
      state = reducer(state, metrics.fetchMetrics());
      assert.equal(state.inFlight, 1);
      state = reducer(state, metrics.fetchMetrics());
      assert.equal(state.inFlight, 2);
      state = reducer(state, metrics.fetchMetricsComplete());
      assert.equal(state.inFlight, 1);
    });
  });

  describe("queryMetrics asynchronous action", function() {
    type timespan = [Long, Long];

    // Helper function to generate metrics request.
    let createRequest = function(ts: timespan, ...names: string[]): Request {
      return new protos.cockroach.ts.TimeSeriesQueryRequest({
        start_nanos: ts[0],
        end_nanos: ts[1],
        queries: _.map(names, (s) => {
          return {
            name: s,
          };
        }),
      });
    };

    // Mock of metrics state.
    let mockMetricsState: metrics.MetricQueryState;
    let mockDispatch = function(action: Action) {
      mockMetricsState = reducer(mockMetricsState, action);
    };
    let queryMetrics = function(id: string, request: Request): Promise<void> {
      return metrics.queryMetrics(id, request)(mockDispatch);
    };

    beforeEach(function () {
      mockMetricsState = undefined;
    });

    afterEach(function () {
      fetchMock.restore();
    });

    it ("correctly batches multiple calls", function () {
      this.timeout(1000);

      // Mock out fetch server; we are only expecting requests to /ts/query,
      // which we simply reflect with an empty set of datapoints.
      fetchMock.mock("/ts/query", "post", (url: string, requestObj: any) => {
          // Assert that metric store's "inFlight" is 1.
          assert.equal(mockMetricsState.inFlight, 1);

          let request = JSON.parse(requestObj.body) as Request;
          return {
            body: {
              results: _.map(request.queries, (q) => {
                return {
                  query: q,
                  datapoints: [],
                };
              }),
            },
          };
      });

      // Dispatch several requests. Requests are divided among two timespans,
      // which should result in two batches.
      let shortTimespan: timespan = [Long.fromNumber(400), Long.fromNumber(500)];
      let longTimespan: timespan = [Long.fromNumber(0), Long.fromNumber(500)];
      queryMetrics("id.1", createRequest(shortTimespan, "short.1", "short.2"));
      queryMetrics("id.2", createRequest(longTimespan, "long.1"));
      queryMetrics("id.3", createRequest(shortTimespan, "short.3"));
      queryMetrics("id.4", createRequest(shortTimespan, "short.4"));
      let p = queryMetrics("id.5", createRequest(longTimespan, "long.2", "long.3"));

      // Queries should already be present, but unfulfilled.
      assert.lengthOf(_.keys(mockMetricsState.queries), 5);
      _.each(mockMetricsState.queries, (q) => {
        assert.isDefined(q.currentRequest);
        assert.isUndefined(q.currentData);
      });

      return p.then(() => {
        // Assert that the server got the correct number of requests (2).
        assert.lengthOf(fetchMock.calls("/ts/query"), 2);
        // Assert that the mock metrics state has 5 queries.
        assert.lengthOf(_.keys(mockMetricsState.queries), 5);
        _.each(mockMetricsState.queries, (q) => {
          assert.isDefined(q.currentRequest);
          assert.isUndefined(q.currentError);
          assert.isDefined(q.currentData, "data not defined for query " + q.id);
        });
        // Assert that inFlight is 0.
        assert.equal(mockMetricsState.inFlight, 0);
      });
    });

    it ("correctly responds to errors.", function () {
      this.timeout(1000);

      // Mock out fetch server; send a positive reply to the first request, and
      // an error to the second request.
      let successSent = false;
      fetchMock.mock("/ts/query", "post", (url: string, requestObj: any) => {
          // Assert that metric store's "inFlight" is 1.
          assert.equal(mockMetricsState.inFlight, 1);

          let request = JSON.parse(requestObj.body) as Request;
          if (successSent) {
            return { status: 500 };
          }
          successSent = true;
          return {
            body: {
              results: _.map(request.queries, (q) => {
                return {
                  query: q,
                  datapoints: [],
                };
              }),
            },
          };
      });

      // Dispatch several requests. Requests are divided among two timespans,
      // which should result in two batches.
      let shortTimespan: timespan = [Long.fromNumber(400), Long.fromNumber(500)];
      let longTimespan: timespan = [Long.fromNumber(0), Long.fromNumber(500)];
      queryMetrics("id.1", createRequest(shortTimespan, "short.1", "short.2"));
      let p = queryMetrics("id.2", createRequest(longTimespan, "long.1"));

      // Queries should already be present, but unfulfilled.
      assert.lengthOf(_.keys(mockMetricsState.queries), 2);
      _.each(mockMetricsState.queries, (q) => {
        assert.isDefined(q.currentRequest);
        assert.isUndefined(q.currentData);
      });

      return p.then(() => {
        // Assert that the server got the correct number of requests (2).
        assert.lengthOf(fetchMock.calls("/ts/query"), 2);
        // Assert that the mock metrics state has 2 queries.
        assert.lengthOf(_.keys(mockMetricsState.queries), 2);
        // Assert query with id.1 has results.
        let q1 = mockMetricsState.queries["id.1"];
        assert.isDefined(q1);
        assert.isDefined(q1.currentData);
        assert.isUndefined(q1.currentError);
        // Assert query with id.2 has an error.
        let q2 = mockMetricsState.queries["id.2"];
        assert.isDefined(q2);
        assert.isDefined(q2.currentError);
        assert.isUndefined(q2.currentData);
        // Assert that inFlight is 0.
        assert.equal(mockMetricsState.inFlight, 0);
      });
    });
  });
});
