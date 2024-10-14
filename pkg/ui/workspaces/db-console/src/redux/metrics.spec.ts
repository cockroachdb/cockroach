// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { keys } from "d3";
import flatMap from "lodash/flatMap";
import map from "lodash/map";
import Long from "long";
import { call, put, delay } from "redux-saga/effects";
import { expectSaga, testSaga } from "redux-saga-test-plan";
import * as matchers from "redux-saga-test-plan/matchers";

import * as protos from "src/js/protos";
import { queryTimeSeries, TimeSeriesQueryRequestMessage } from "src/util/api";

import * as metrics from "./metrics";

type TSRequest = protos.cockroach.ts.tspb.TimeSeriesQueryRequest;

describe("metrics reducer", function () {
  describe("actions", function () {
    it("requestMetrics() creates the correct action type.", function () {
      expect(metrics.requestMetrics("id", null).type).toEqual(metrics.REQUEST);
    });

    it("receiveMetrics() creates the correct action type.", function () {
      expect(metrics.receiveMetrics("id", null, null).type).toEqual(
        metrics.RECEIVE,
      );
    });

    it("errorMetrics() creates the correct action type.", function () {
      expect(metrics.errorMetrics("id", null).type).toEqual(metrics.ERROR);
    });

    it("fetchMetrics() creates the correct action type.", function () {
      expect(metrics.fetchMetrics().type).toEqual(metrics.FETCH);
    });

    it("fetchMetricsComplete() creates the correct action type.", function () {
      expect(metrics.fetchMetricsComplete().type).toEqual(
        metrics.FETCH_COMPLETE,
      );
    });
  });

  describe("reducer", function () {
    const componentID = "test-component";
    let state: metrics.MetricsState;

    beforeEach(() => {
      state = metrics.metricsReducer(undefined, { type: "unknown" });
    });

    it("should have the correct default value.", function () {
      const expected = {
        inFlight: 0,
        queries: metrics.metricQuerySetReducer(undefined, { type: "unknown" }),
      };
      expect(state).toEqual(expected);
    });

    it("should correctly dispatch requestMetrics", function () {
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
      state = metrics.metricsReducer(
        state,
        metrics.requestMetrics(componentID, request),
      );
      expect(state.queries).toBeDefined();
      expect(state.queries[componentID]).toBeDefined();
      expect(keys(state.queries).length).toBe(1);
      expect(state.queries[componentID].nextRequest).toEqual(request);
      expect(state.queries[componentID].data).toBeUndefined();
      expect(state.queries[componentID].error).toBeUndefined();
    });

    it("should correctly dispatch receiveMetrics with an unmatching nextRequest", function () {
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
      state = metrics.metricsReducer(
        state,
        metrics.receiveMetrics(componentID, request, response),
      );
      expect(state.queries).toBeDefined();
      expect(state.queries[componentID]).toBeDefined();
      expect(keys(state.queries).length).toBe(1);
      expect(state.queries[componentID].data).toBeUndefined();
      expect(state.queries[componentID].request).toBeUndefined();
      expect(state.queries[componentID].nextRequest).toBeUndefined();
      expect(state.queries[componentID].error).toBeUndefined();
    });

    it("should correctly dispatch receiveMetrics with a matching nextRequest", function () {
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
      state = metrics.metricsReducer(
        state,
        metrics.requestMetrics(componentID, request),
      );
      state = metrics.metricsReducer(
        state,
        metrics.receiveMetrics(componentID, request, response),
      );
      expect(state.queries).toBeDefined();
      expect(state.queries[componentID]).toBeDefined();
      expect(keys(state.queries).length).toBe(1);
      expect(state.queries[componentID].data).toEqual(response);
      expect(state.queries[componentID].request).toEqual(request);
      expect(state.queries[componentID].error).toBeUndefined();
    });

    it("should correctly dispatch errorMetrics", function () {
      const error: Error = new Error("An error occurred");
      state = metrics.metricsReducer(
        state,
        metrics.errorMetrics(componentID, error),
      );
      expect(state.queries).toBeDefined();
      expect(state.queries[componentID]).toBeDefined();
      expect(keys(state.queries).length).toBe(1);
      expect(state.queries[componentID].error).toEqual(error);
      expect(state.queries[componentID].request).toBeUndefined();
      expect(state.queries[componentID].data).toBeUndefined();
    });

    it("should correctly dispatch fetchMetrics and fetchMetricsComplete", function () {
      state = metrics.metricsReducer(state, metrics.fetchMetrics());
      expect(state.inFlight).toBe(1);
      state = metrics.metricsReducer(state, metrics.fetchMetrics());
      expect(state.inFlight).toBe(2);
      state = metrics.metricsReducer(state, metrics.fetchMetricsComplete());
      expect(state.inFlight).toBe(1);
    });
  });

  describe("saga functions", function () {
    type timespan = [Long, Long];
    const shortTimespan: timespan = [
      Long.fromNumber(400),
      Long.fromNumber(500),
    ];
    const longTimespan: timespan = [Long.fromNumber(0), Long.fromNumber(500)];

    // Helper function to generate metrics request.
    function createRequest(ts: timespan, ...names: string[]): TSRequest {
      return new protos.cockroach.ts.tspb.TimeSeriesQueryRequest({
        start_nanos: ts[0],
        end_nanos: ts[1],
        queries: map(names, s => {
          return {
            name: s,
          };
        }),
      });
    }

    function createResponse(
      queries: protos.cockroach.ts.tspb.IQuery[],
      datapoints: protos.cockroach.ts.tspb.TimeSeriesDatapoint[] = [],
    ) {
      return new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
        results: queries.map(query => {
          return {
            query,
            datapoints,
          };
        }),
      });
    }

    function createDatapoints(val: number) {
      const result: protos.cockroach.ts.tspb.TimeSeriesDatapoint[] = [];
      for (let i = 0; i < val; i++) {
        result.push(
          new protos.cockroach.ts.tspb.TimeSeriesDatapoint({
            timestamp_nanos: new Long(val),
            value: val,
          }),
        );
      }
      return result;
    }

    describe("queryMetricsSaga plan", function () {
      it("initially waits for incoming request objects", function () {
        testSaga(metrics.queryMetricsSaga).next().take(metrics.REQUEST);
      });

      it("correctly accumulates batches", function () {
        const requestAction = metrics.requestMetrics(
          "id",
          createRequest(shortTimespan, "short.1"),
        );
        const beginAction = metrics.beginMetrics(
          requestAction.payload.id,
          requestAction.payload.data,
        );

        return (
          expectSaga(metrics.queryMetricsSaga)
            // Stub out calls to batchAndSendRequests.
            .provide([[matchers.call.fn(metrics.batchAndSendRequests), null]])
            // Dispatch six requests, with delays inserted in order to trigger
            // batch sends.
            .dispatch(requestAction)
            .dispatch(requestAction)
            .dispatch(requestAction)
            .delay(0)
            .dispatch(requestAction)
            .delay(0)
            .dispatch(requestAction)
            .dispatch(requestAction)
            .run()
            .then(result => {
              const { effects } = result;
              // Verify the order of call dispatches.
              expect(effects.call).toEqual([
                delay(0),
                call(metrics.batchAndSendRequests, [
                  requestAction.payload,
                  requestAction.payload,
                  requestAction.payload,
                ]),
                delay(0),
                call(metrics.batchAndSendRequests, [requestAction.payload]),
                delay(0),
                call(metrics.batchAndSendRequests, [
                  requestAction.payload,
                  requestAction.payload,
                ]),
              ]);
              // Verify that all beginAction puts were dispatched.
              expect(effects.put).toEqual([
                put(beginAction),
                put(beginAction),
                put(beginAction),
                put(beginAction),
                put(beginAction),
                put(beginAction),
              ]);
            })
        );
      });
    });

    describe("batchAndSendRequests", function () {
      it("sendBatches correctly batches multiple requests", function () {
        const shortRequests = [
          metrics.requestMetrics("id", createRequest(shortTimespan, "short.1"))
            .payload,
          metrics.requestMetrics(
            "id",
            createRequest(shortTimespan, "short.2", "short.3"),
          ).payload,
          metrics.requestMetrics("id", createRequest(shortTimespan, "short.4"))
            .payload,
        ];
        const longRequests = [
          metrics.requestMetrics("id", createRequest(longTimespan, "long.1"))
            .payload,
          metrics.requestMetrics(
            "id",
            createRequest(longTimespan, "long.2", "long.3"),
          ).payload,
          metrics.requestMetrics(
            "id",
            createRequest(longTimespan, "long.4", "long.5"),
          ).payload,
        ];

        // Mix the requests together and send the combined request set.
        const mixedRequests = flatMap(shortRequests, (short, i) => [
          short,
          longRequests[i],
        ]);

        testSaga(metrics.batchAndSendRequests, mixedRequests)
          // sendBatches next puts a "fetchMetrics" action into the store.
          .next()
          .put(metrics.fetchMetrics())
          .next()
          // Next, sendBatches dispatches a "all" effect with a "call" for each
          // batch; there should be two batches in total, one containing the
          // short requests and one containing the long requests. The order of
          // requests in each batch is maintained.
          .all([
            call(metrics.sendRequestBatch, shortRequests),
            call(metrics.sendRequestBatch, longRequests),
          ])
          // After completion, puts "fetchMetricsComplete" to store.
          .next()
          .put(metrics.fetchMetricsComplete())
          .next()
          .isDone();
      });
    });

    describe("sendRequestBatch", function () {
      const requests = [
        metrics.requestMetrics("id1", createRequest(shortTimespan, "short.1"))
          .payload,
        metrics.requestMetrics(
          "id2",
          createRequest(shortTimespan, "short.2", "short.3"),
        ).payload,
        metrics.requestMetrics("id3", createRequest(shortTimespan, "short.4"))
          .payload,
      ];

      it("correctly sends batch as single request, correctly handles valid response", function () {
        // The expected request that will be generated by sendRequestBatch.
        const expectedRequest = createRequest(
          shortTimespan,
          "short.1",
          "short.2",
          "short.3",
          "short.4",
        );
        // Return a valid response.
        const response = createResponse(expectedRequest.queries);
        // Generate the expected put effects to be generated after receiving the response.
        const expectedEffects = map(requests, req =>
          metrics.receiveMetrics(
            req.id,
            req.data,
            createResponse(req.data.queries),
          ),
        );

        testSaga(metrics.sendRequestBatch, requests)
          .next()
          .call(queryTimeSeries, expectedRequest)
          .next(response)
          .put(expectedEffects[0])
          .next()
          .put(expectedEffects[1])
          .next()
          .put(expectedEffects[2])
          .next()
          .isDone();
      });

      it("correctly handles error response", function () {
        // The expected request that will be generated by sendRequestBatch.
        const expectedRequest = createRequest(
          shortTimespan,
          "short.1",
          "short.2",
          "short.3",
          "short.4",
        );
        // Return an error response.
        const err = new Error("network error");
        // Generate the expected put effects to be generated after receiving the response.
        const expectedEffects = map(requests, req =>
          metrics.errorMetrics(req.id, err),
        );

        testSaga(metrics.sendRequestBatch, requests)
          .next()
          .call(queryTimeSeries, expectedRequest)
          .throw(err)
          .put(expectedEffects[0])
          .next()
          .put(expectedEffects[1])
          .next()
          .put(expectedEffects[2])
          .next()
          .isDone();
      });
    });

    describe("integration test", function () {
      const shortRequests = [
        metrics.requestMetrics("id.0", createRequest(shortTimespan, "short.1")),
        metrics.requestMetrics(
          "id.2",
          createRequest(shortTimespan, "short.2", "short.3"),
        ),
        metrics.requestMetrics("id.4", createRequest(shortTimespan, "short.4")),
      ];
      const longRequests = [
        metrics.requestMetrics("id.1", createRequest(longTimespan, "long.1")),
        metrics.requestMetrics(
          "id.3",
          createRequest(longTimespan, "long.2", "long.3"),
        ),
        metrics.requestMetrics(
          "id.5",
          createRequest(longTimespan, "long.4", "long.5"),
        ),
      ];

      const createMetricsState = (
        id: string,
        ts: timespan,
        metricNames: string[],
        datapointCount: number,
      ): metrics.MetricsQuery => {
        const request = createRequest(ts, ...metricNames);
        const state = new metrics.MetricsQuery(id);
        state.request = request;
        state.nextRequest = request;
        state.data = createResponse(
          request.queries,
          createDatapoints(datapointCount),
        );
        state.error = undefined;
        return state;
      };

      const createMetricsErrorState = (
        id: string,
        ts: timespan,
        metricNames: string[],
        err: Error,
      ): metrics.MetricsQuery => {
        const request = createRequest(ts, ...metricNames);
        const state = new metrics.MetricsQuery(id);
        state.nextRequest = request;
        state.error = err;
        return state;
      };

      const createMetricsInFlightState = (
        id: string,
        ts: timespan,
        metricNames: string[],
      ): metrics.MetricsQuery => {
        const request = createRequest(ts, ...metricNames);
        const state = new metrics.MetricsQuery(id);
        state.nextRequest = request;
        return state;
      };

      it("handles success correctly", function () {
        const expectedState = new metrics.MetricsState();
        expectedState.inFlight = 0;
        expectedState.queries = {
          "id.0": createMetricsState("id.0", shortTimespan, ["short.1"], 3),
          "id.1": createMetricsState("id.1", longTimespan, ["long.1"], 3),
          "id.2": createMetricsState(
            "id.2",
            shortTimespan,
            ["short.2", "short.3"],
            3,
          ),
          "id.3": createMetricsState(
            "id.3",
            longTimespan,
            ["long.2", "long.3"],
            3,
          ),
          "id.4": createMetricsState("id.4", shortTimespan, ["short.4"], 3),
          "id.5": createMetricsState(
            "id.5",
            longTimespan,
            ["long.4", "long.5"],
            3,
          ),
        };

        return expectSaga(metrics.queryMetricsSaga)
          .withReducer(metrics.metricsReducer)
          .hasFinalState(expectedState)
          .provide({
            call(effect, next) {
              if (effect.fn === queryTimeSeries) {
                return new Promise(resolve => {
                  setTimeout(
                    () =>
                      resolve(
                        createResponse(
                          (effect.args[0] as TimeSeriesQueryRequestMessage)
                            .queries,
                          createDatapoints(3),
                        ),
                      ),
                    10,
                  );
                });
              }
              return next();
            },
          })
          .dispatch(shortRequests[0])
          .dispatch(longRequests[0])
          .dispatch(shortRequests[1])
          .delay(0)
          .dispatch(longRequests[1])
          .dispatch(shortRequests[2])
          .dispatch(longRequests[2])
          .run();
      });

      it("handles errors correctly", function () {
        const fakeError = new Error("connection error");

        const expectedState = new metrics.MetricsState();
        expectedState.inFlight = 0;
        expectedState.queries = {
          "id.0": createMetricsState("id.0", shortTimespan, ["short.1"], 3),
          "id.1": createMetricsState("id.1", longTimespan, ["long.1"], 3),
          "id.2": createMetricsState(
            "id.2",
            shortTimespan,
            ["short.2", "short.3"],
            3,
          ),
          "id.3": createMetricsErrorState(
            "id.3",
            longTimespan,
            ["long.2", "long.3"],
            fakeError,
          ),
          "id.4": createMetricsErrorState(
            "id.4",
            shortTimespan,
            ["short.4"],
            fakeError,
          ),
          "id.5": createMetricsErrorState(
            "id.5",
            longTimespan,
            ["long.4", "long.5"],
            fakeError,
          ),
        };

        let callCounter = 0;
        return expectSaga(metrics.queryMetricsSaga)
          .withReducer(metrics.metricsReducer)
          .hasFinalState(expectedState)
          .provide({
            call(effect, next) {
              if (effect.fn === queryTimeSeries) {
                callCounter++;
                if (callCounter > 2) {
                  throw fakeError;
                }
                return createResponse(
                  (effect.args[0] as TimeSeriesQueryRequestMessage).queries,
                  createDatapoints(3),
                );
              }
              return next();
            },
          })
          .dispatch(shortRequests[0])
          .dispatch(longRequests[0])
          .dispatch(shortRequests[1])
          .delay(0)
          .dispatch(longRequests[1])
          .dispatch(shortRequests[2])
          .dispatch(longRequests[2])
          .run();
      });

      it("handles inflight counter correctly", function () {
        const expectedState = new metrics.MetricsState();
        expectedState.inFlight = 1;
        expectedState.queries = {
          "id.0": createMetricsState("id.0", shortTimespan, ["short.1"], 3),
          "id.1": createMetricsState("id.1", longTimespan, ["long.1"], 3),
          "id.2": createMetricsState(
            "id.2",
            shortTimespan,
            ["short.2", "short.3"],
            3,
          ),
          "id.3": createMetricsInFlightState("id.3", longTimespan, [
            "long.2",
            "long.3",
          ]),
          "id.4": createMetricsInFlightState("id.4", shortTimespan, [
            "short.4",
          ]),
          "id.5": createMetricsInFlightState("id.5", longTimespan, [
            "long.4",
            "long.5",
          ]),
        };

        let callCounter = 0;
        return expectSaga(metrics.queryMetricsSaga)
          .withReducer(metrics.metricsReducer)
          .hasFinalState(expectedState)
          .provide({
            call(effect, next) {
              if (effect.fn === queryTimeSeries) {
                callCounter++;
                if (callCounter > 2) {
                  // return a promise that never resolves.
                  return new Promise(_resolve => {});
                }
                return createResponse(
                  (effect.args[0] as TimeSeriesQueryRequestMessage).queries,
                  createDatapoints(3),
                );
              }
              return next();
            },
          })
          .dispatch(shortRequests[0])
          .dispatch(longRequests[0])
          .dispatch(shortRequests[1])
          .delay(0)
          .dispatch(longRequests[1])
          .dispatch(shortRequests[2])
          .dispatch(longRequests[2])
          .run();
      });
    });
  });
});
