import { assert } from "chai";
import _ = require("lodash");
import Long = require("long");
import * as fetchMock from "../util/fetch-mock";

import * as protos from "../js/protos";
import reducer, * as metrics from "./metrics";
import { Action } from "../interfaces/action";

type TSRequestMessage = cockroach.ts.tspb.TimeSeriesQueryRequestMessage;

describe("metrics reducer", function() {
  describe("actions", function() {
    it("requestMetrics() creates the correct action type.", function() {
      assert.equal(metrics.requestMetrics("id", null).type, metrics.REQUEST);
    });

    it("receiveMetrics() creates the correct action type.", function() {
      assert.equal(metrics.receiveMetrics("id", null, null).type, metrics.RECEIVE);
    });

    it("errorMetrics() creates the correct action type.", function() {
      assert.equal(metrics.errorMetrics("id", null).type, metrics.ERROR);
    });

    it("fetchMetrics() creates the correct action type.", function() {
      assert.equal(metrics.fetchMetrics().type, metrics.FETCH);
    });

    it("fetchMetricsComplete() creates the correct action type.", function() {
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
        queries: metrics.metricQuerySetReducer(undefined, { type: "unknown" }),
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestMetrics", function() {
      let request = new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
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
      });
      state = reducer(state, metrics.requestMetrics(componentID, request));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].nextRequest, request);
      assert.isUndefined(state.queries[componentID].data);
      assert.isUndefined(state.queries[componentID].error);
    });

    it("should correctly dispatch receiveMetrics", function() {
      let response = new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: [
          {
            datapoints: [],
          },
        ],
      });
      let request = new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: Long.fromInt(0),
        end_nanos: Long.fromInt(10),
        queries: [
          {
            name: "test.metric.1",
          },
        ],
      });
      state = reducer(state, metrics.receiveMetrics(componentID, request, response));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].data, response);
      assert.equal(state.queries[componentID].request, request);
      assert.isUndefined(state.queries[componentID].nextRequest);
      assert.isUndefined(state.queries[componentID].error);
    });

    it("should correctly dispatch errorMetrics", function() {
      let error: Error = new Error("An error occurred");
      state = reducer(state, metrics.errorMetrics(componentID, error));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].error, error);
      assert.isUndefined(state.queries[componentID].request);
      assert.isUndefined(state.queries[componentID].data);
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
    let createRequest = function(ts: timespan, ...names: string[]): TSRequestMessage {
      return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
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
    let queryMetrics = function(id: string, request: TSRequestMessage): Promise<void> {
      return metrics.queryMetrics(id, request)(mockDispatch);
    };

    beforeEach(function () {
      mockMetricsState = undefined;
    });

    afterEach(fetchMock.restore);

    it ("correctly batches multiple calls", function () {
      this.timeout(1000);

      // Mock out fetch server; we are only expecting requests to /ts/query,
      // which we simply reflect with an empty set of datapoints.
      fetchMock.mock({
        matcher: "/ts/query",
        method: "POST",
        response: (url: string, requestObj: RequestInit) => {
          // Assert that metric store's "inFlight" is 1 or 2.
          assert.isAtLeast(mockMetricsState.inFlight, 1);
          assert.isAtMost(mockMetricsState.inFlight, 2);

          let request = protos.cockroach.ts.tspb.TimeSeriesQueryRequest.decode(requestObj.body as ArrayBuffer);

          return {
            body: new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
              results: _.map(request.queries, (q) => {
                return {
                  query: q,
                  datapoints: [],
                };
              }),
            }).toArrayBuffer(),
          };
        },
      });

      // Dispatch several requests. Requests are divided among two timespans,
      // which should result in two batches.
      let shortTimespan: timespan = [Long.fromNumber(400), Long.fromNumber(500)];
      let longTimespan: timespan = [Long.fromNumber(0), Long.fromNumber(500)];
      queryMetrics("id.1", createRequest(shortTimespan, "short.1", "short.2"));
      queryMetrics("id.2", createRequest(longTimespan, "long.1"));
      queryMetrics("id.3", createRequest(shortTimespan, "short.3"));
      queryMetrics("id.4", createRequest(shortTimespan, "short.4"));
      let p1 = queryMetrics("id.5", createRequest(longTimespan, "long.2", "long.3"));

      // Queries should already be present, but unfulfilled.
      assert.lengthOf(_.keys(mockMetricsState.queries), 5);
      _.each(mockMetricsState.queries, (q) => {
        assert.isDefined(q.nextRequest);
        assert.isUndefined(q.data);
        assert.isUndefined(q.request);
      });

      // Dispatch an additional query for the short timespan, but in a
      // setTimeout - this should result in a separate batch.
      let p2 = new Promise<void>((resolve, reject) => {
        setTimeout(() => {
          resolve(queryMetrics("id.6", createRequest(shortTimespan, "short.6")));
        });
      });

      return Promise.all([p1, p2]).then(() => {
        // Assert that the server got the correct number of requests (2).
        assert.lengthOf(fetchMock.calls("/ts/query"), 3);
        // Assert that the mock metrics state has 5 queries.
        assert.lengthOf(_.keys(mockMetricsState.queries), 6);
        _.each(mockMetricsState.queries, (q) => {
          assert.isDefined(q.request);
          assert.isUndefined(q.error);
          assert.isDefined(q.data, "data not defined for query " + q.id);
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
      fetchMock.mock({
        matcher: "/ts/query",
        method: "POST",
        response: (url: string, requestObj: RequestInit) => {
          // Assert that metric store's "inFlight" is 1.
          assert.equal(mockMetricsState.inFlight, 1);

          if (successSent) {
            return { throws: new Error() };
          }
          successSent = true;

          let request = protos.cockroach.ts.tspb.TimeSeriesQueryRequest.decode(requestObj.body as ArrayBuffer);

          return {
            body: new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
              results: _.map(request.queries, (q) => {
                return {
                  query: q,
                  datapoints: [],
                };
              }),
            }).toArrayBuffer(),
          };
        },
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
        assert.isDefined(q.nextRequest);
        assert.isUndefined(q.data);
      });

      return p.then(() => {
        // Assert that the server got the correct number of requests (2).
        assert.lengthOf(fetchMock.calls("/ts/query"), 2);
        // Assert that the mock metrics state has 2 queries.
        assert.lengthOf(_.keys(mockMetricsState.queries), 2);
        // Assert query with id.1 has results.
        let q1 = mockMetricsState.queries["id.1"];
        assert.isDefined(q1);
        assert.isDefined(q1.data);
        assert.isUndefined(q1.error);
        // Assert query with id.2 has an error.
        let q2 = mockMetricsState.queries["id.2"];
        assert.isDefined(q2);
        assert.isDefined(q2.error);
        assert.isUndefined(q2.data);
        // Assert that inFlight is 0.
        assert.equal(mockMetricsState.inFlight, 0);
      });
    });
  });
});
