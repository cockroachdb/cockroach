import { assert } from "chai";
import _ from "lodash";
import Long from "long";

import { delay } from "redux-saga";
import { AllEffect, call, ForkEffect, put, take } from "redux-saga/effects";
import { queryTimeSeries } from "src/util/api";
import * as protos from "src/js/protos";

import * as metrics from "./metrics";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;

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
    const componentID = "test-component";
    let state: metrics.MetricsState;

    beforeEach(() => {
      state = metrics.metricsReducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function() {
      const expected = {
        inFlight: 0,
        queries: metrics.metricQuerySetReducer(undefined, { type: "unknown" }),
      };
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch requestMetrics", function() {
      const request = new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: Long.fromNumber(0),
        end_nanos: Long.fromNumber(10),
        queries: [
          {
            name: "test.metric.1",
          },
          {
            name: "test.metric.2",
          },
        ],
      });
      state = metrics.metricsReducer(state, metrics.requestMetrics(componentID, request));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].nextRequest, request);
      assert.isUndefined(state.queries[componentID].data);
      assert.isUndefined(state.queries[componentID].error);
    });

    it("should correctly dispatch receiveMetrics with an unmatching nextRequest", function() {
      const response = new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: [
          {
            datapoints: [],
          },
        ],
      });
      const request = new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: Long.fromNumber(0),
        end_nanos: Long.fromNumber(10),
        queries: [
          {
            name: "test.metric.1",
          },
        ],
      });
      state = metrics.metricsReducer(state, metrics.receiveMetrics(componentID, request, response));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].data, null);
      assert.equal(state.queries[componentID].request, null);
      assert.isUndefined(state.queries[componentID].nextRequest);
      assert.isUndefined(state.queries[componentID].error);
    });

    it("should correctly dispatch receiveMetrics with a matching nextRequest", function() {
      const response = new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: [
          {
            datapoints: [],
          },
        ],
      });
      const request = new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: Long.fromNumber(0),
        end_nanos: Long.fromNumber(10),
        queries: [
          {
            name: "test.metric.1",
          },
        ],
      });
      // populate nextRequest
      state = metrics.metricsReducer(state, metrics.requestMetrics(componentID, request));
      state = metrics.metricsReducer(state, metrics.receiveMetrics(componentID, request, response));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].data, response);
      assert.equal(state.queries[componentID].request, request);
      assert.isUndefined(state.queries[componentID].error);
    });

    it("should correctly dispatch errorMetrics", function() {
      const error: Error = new Error("An error occurred");
      state = metrics.metricsReducer(state, metrics.errorMetrics(componentID, error));
      assert.isDefined(state.queries);
      assert.isDefined(state.queries[componentID]);
      assert.lengthOf(_.keys(state.queries), 1);
      assert.equal(state.queries[componentID].error, error);
      assert.isUndefined(state.queries[componentID].request);
      assert.isUndefined(state.queries[componentID].data);
    });

    it("should correctly dispatch fetchMetrics and fetchMetricsComplete", function() {
      state = metrics.metricsReducer(state, metrics.fetchMetrics());
      assert.equal(state.inFlight, 1);
      state = metrics.metricsReducer(state, metrics.fetchMetrics());
      assert.equal(state.inFlight, 2);
      state = metrics.metricsReducer(state, metrics.fetchMetricsComplete());
      assert.equal(state.inFlight, 1);
    });
  });

  describe("saga functions", function() {
    type timespan = [Long, Long];
    const shortTimespan: timespan = [Long.fromNumber(400), Long.fromNumber(500)];
    const longTimespan: timespan = [Long.fromNumber(0), Long.fromNumber(500)];

    // Helper function to generate metrics request.
    function createRequest(ts: timespan, ...names: string[]): TSRequest {
      return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: ts[0],
        end_nanos: ts[1],
        queries: _.map(names, (s) => {
          return {
            name: s,
          };
        }),
      });
    }

    function createResponse(queries: protos.cockroach.ts.tspb.Query$Properties[]) {
      return new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: queries.map(q => {
          return {
            query: q,
            datapoints: [],
          };
        }),
      });
    }

    describe("queryMetricsSaga", function() {
      it("initially waits for incoming request objects", function () {
        const saga = metrics.queryMetricsSaga();
        assert.deepEqual(saga.next().value, take(metrics.REQUEST));
      });

      it("correctly accumulates batches", function () {
        const requestAction = metrics.requestMetrics("id", createRequest(shortTimespan, "short.1"));
        const saga = metrics.queryMetricsSaga();
        saga.next();

        // Pass in a request, the generator should put the request to the
        // redux store, fork "sendBatches" process, then await another request.
        assert.deepEqual(saga.next(requestAction).value, put(requestAction));
        const sendBatchesFork = saga.next().value as ForkEffect;
        assert.isDefined(sendBatchesFork.FORK);
        assert.deepEqual(saga.next().value, take(metrics.REQUEST));

        // Pass in two additional requests, the generator should not yield another
        // fork.
        for (let i = 0; i < 2; i++) {
          assert.deepEqual(saga.next(requestAction).value, put(requestAction));
          assert.deepEqual(saga.next().value, take(metrics.REQUEST));
        }

        // Run the fork function. It should initially call delay (to defer to the
        // event queue), then call sendRequestBatches with the currently
        // accumulated batches, then complete.
        const accumulateBatch = sendBatchesFork.FORK.fn() as IterableIterator<any>;
        assert.deepEqual(accumulateBatch.next().value, call(delay, 0));
        assert.deepEqual(
          accumulateBatch.next().value,
          call(
            metrics.batchAndSendRequests,
            [requestAction.payload, requestAction.payload, requestAction.payload],
          ),
        );
        assert.isTrue(accumulateBatch.next().done);

        // Pass in a request, the generator should again yield a "fork" followed
        // by another take for request objects.
        assert.deepEqual(saga.next(requestAction).value, put(requestAction));
        assert.isDefined((saga.next().value as ForkEffect).FORK);
        assert.deepEqual(saga.next().value, take(metrics.REQUEST));
      });
    });

    describe("batchAndSendRequests", function() {
      it("sendBatches correctly batches multiple requests", function () {
        const shortRequests = [
          metrics.requestMetrics("id", createRequest(shortTimespan, "short.1")).payload,
          metrics.requestMetrics("id", createRequest(shortTimespan, "short.2", "short.3")).payload,
          metrics.requestMetrics("id", createRequest(shortTimespan, "short.4")).payload,
        ];
        const longRequests = [
          metrics.requestMetrics("id", createRequest(longTimespan, "long.1")).payload,
          metrics.requestMetrics("id", createRequest(longTimespan, "long.2", "long.3")).payload,
          metrics.requestMetrics("id", createRequest(longTimespan, "long.4", "long.5")).payload,
        ];

        // Mix the requests together and send the combined request set.
        const mixedRequests = _.flatMap(shortRequests, (short, i) => [short, longRequests[i]]);
        const sendBatches = metrics.batchAndSendRequests(mixedRequests);

        // sendBatches next puts a "fetchMetrics" action into the store.
        assert.deepEqual(sendBatches.next().value, put(metrics.fetchMetrics()));

        // Next, sendBatches dispatches a "all" effect with a "call" for each
        // batch; there should be two batches in total, one containing the
        // short requests and one containing the long requests. The order
        // of requests in each batch is maintained.
        const allEffect = sendBatches.next().value as AllEffect;
        assert.isArray(allEffect.ALL);
        assert.deepEqual(allEffect.ALL, [
          call(metrics.sendRequestBatch, shortRequests),
          call(metrics.sendRequestBatch, longRequests),
        ]);

        // After completion, puts "fetchMetricsComplete" to store.
        assert.deepEqual(sendBatches.next().value, put(metrics.fetchMetricsComplete()));
        assert.isTrue(sendBatches.next().done);
      });
    });

    describe("sendRequestBatch", function() {
      const requests = [
        metrics.requestMetrics("id1", createRequest(shortTimespan, "short.1")).payload,
        metrics.requestMetrics("id2", createRequest(shortTimespan, "short.2", "short.3")).payload,
        metrics.requestMetrics("id3", createRequest(shortTimespan, "short.4")).payload,
      ];

      it("correctly sends batch as single request, correctly handles valid response", function() {
        const sendBatch = metrics.sendRequestBatch(requests);
        const expectedRequest = createRequest(shortTimespan, "short.1", "short.2", "short.3", "short.4");
        assert.deepEqual(sendBatch.next().value, call(queryTimeSeries, expectedRequest));

        // Return a valid response.
        const response = createResponse(expectedRequest.queries);

        // Expect three puts to the underlying store.
        const actualEffects = [
          sendBatch.next(response).value,
          sendBatch.next().value,
          sendBatch.next().value,
        ];
        const expectedEffects = requests.map(req => put(metrics.receiveMetrics(
          req.id,
          req.data,
          createResponse(req.data.queries),
        )));
        assert.deepEqual(actualEffects, expectedEffects);
        assert.isTrue(sendBatch.next().done);
      });

      it("correctly handles error response", function() {
        const sendBatch = metrics.sendRequestBatch(requests);
        const expectedRequest = createRequest(shortTimespan, "short.1", "short.2", "short.3", "short.4");
        assert.deepEqual(sendBatch.next().value, call(queryTimeSeries, expectedRequest));

        // Throw an exception, a network error.  Expect three puts, which are
        // are error responses.
        const err = new Error("network error");
        const actualEffects = [
          sendBatch.throw(err).value,
          sendBatch.next().value,
          sendBatch.next().value,
        ];
        const expectedEffects = requests.map(req => put(metrics.errorMetrics(
          req.id,
          err,
        )));
        assert.deepEqual(actualEffects, expectedEffects);
        assert.isTrue(sendBatch.next().done);
      });
    });
  });
});
