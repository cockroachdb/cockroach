// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import { channel } from "redux-saga";
import { delay, call } from "redux-saga/effects";
import { expectSaga, testSaga } from "redux-saga-test-plan";

import { queryManagerReducer } from "./reducer";
import {
  refresh,
  autoRefresh,
  stopAutoRefresh,
  ManagedQuerySagaState,
  processQueryManagementAction,
  queryManagerSaga,
  timeToNextRefresh,
  getMoment,
  DEFAULT_REFRESH_INTERVAL,
  DEFAULT_RETRY_DELAY,
} from "./saga";

describe("Query Management Saga", function () {
  let queryCounterCalled = 0;
  const testQueryCounter = {
    id: "testQueryCounter",
    refreshInterval: moment.duration(50),
    retryDelay: moment.duration(500),
    querySaga: function* () {
      yield delay(0);
      yield call(() => queryCounterCalled++);
    },
  };

  const sentinelError = new Error("error");
  let queryErrorCalled = 0;
  const testQueryError = {
    id: "testQueryError",
    refreshInterval: moment.duration(500),
    retryDelay: moment.duration(50),
    // eslint-disable-next-line require-yield
    querySaga: function* (): IterableIterator<void> {
      queryErrorCalled++;
      throw sentinelError;
    },
  };

  beforeEach(function () {
    queryCounterCalled = 0;
    queryErrorCalled = 0;
  });

  describe("integration tests", function () {
    describe("REFRESH action", function () {
      it("immediately runs a saga when refresh is called", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(refresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(1);
          });
      });
      it("does not run refresh again if query is currently in progress", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(refresh(testQueryCounter))
          .dispatch(refresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(1);
          });
      });
      it("does refresh again if query is allowed to finish.", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(refresh(testQueryCounter))
          .delay(10)
          .dispatch(refresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(2);
          });
      });
      it("correctly records error (and does not retry).", function () {
        return (
          expectSaga(queryManagerSaga)
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            .withReducer(queryManagerReducer)
            .dispatch(refresh(testQueryError))
            .silentRun()
            .then(runResult => {
              expect(typeof runResult.storeState[testQueryError.id]).toBe(
                "object",
              );
              expect(runResult.storeState[testQueryError.id].lastError).toEqual(
                sentinelError,
              );
              expect(runResult.storeState[testQueryError.id].isRunning).toBe(
                false,
              );
            })
        );
      });
      it("immediately runs a saga if refresh is called even if AUTO_REFRESH wait is active", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(autoRefresh(testQueryCounter))
          .delay(10)
          .dispatch(refresh(testQueryCounter))
          .dispatch(stopAutoRefresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(2);
          });
      });
    });
    describe("AUTO_REFRESH/STOP_AUTO_REFRESH action", function () {
      it("immediately runs if query result is out of date", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(autoRefresh(testQueryCounter))
          .dispatch(stopAutoRefresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(1);
          });
      });
      it("does not run again if query result is considered current.", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(refresh(testQueryCounter))
          .dispatch(autoRefresh(testQueryCounter))
          .dispatch(stopAutoRefresh(testQueryCounter))
          .silentRun()
          .then(() => {
            expect(queryCounterCalled).toBe(1);
          });
      });
      it("runs again after a delay while refresh refcount is positive.", function () {
        const tester = expectSaga(queryManagerSaga);

        // A query which stops itself by dispatching a stopAutoRefresh
        // after being called some number of times.
        let queryCalled = 0;
        const selfStopQuery = {
          id: "selfStopQuery",
          refreshInterval: moment.duration(5),
          querySaga: function* (): IterableIterator<void> {
            queryCalled++;
            if (queryCalled > 3) {
              tester.dispatch(stopAutoRefresh(selfStopQuery));
            }
            yield;
          },
        };
        return tester
          .dispatch(autoRefresh(selfStopQuery))
          .dispatch(autoRefresh(selfStopQuery))
          .dispatch(autoRefresh(selfStopQuery))
          .silentRun()
          .then(() => {
            expect(queryCalled).toBe(6);
          });
      });
      it("Uses retry delay when errors are encountered", function () {
        return expectSaga(queryManagerSaga)
          .dispatch(autoRefresh(testQueryError))
          .silentRun(200)
          .then(() => {
            // RefreshTimeout is high enough that it would only be
            // called once.
            expect(queryErrorCalled).toBeGreaterThanOrEqual(3);
          });
      });
      it("sets inRunning flag on reducer when query is running.", function () {
        const neverResolveQuery = {
          id: "explicitResolveQuery",
          refreshInterval: moment.duration(0),
          querySaga: function* (): IterableIterator<Promise<void>> {
            yield new Promise((_resolve, _reject) => {});
          },
        };
        return expectSaga(queryManagerSaga)
          .withReducer(queryManagerReducer)
          .dispatch(refresh(neverResolveQuery))
          .dispatch(refresh(testQueryCounter))
          .silentRun()
          .then(runResult => {
            expect(runResult.storeState[neverResolveQuery.id].isRunning).toBe(
              true,
            );
            expect(runResult.storeState[testQueryCounter.id].isRunning).toBe(
              false,
            );
            expect(queryCounterCalled).toBe(1);
          });
      });
      it("continues to count AUTO_REFRESH refcounts even while query is running", function () {
        let queryCalledCount = 0;
        let resolveQuery: () => void;
        const explicitResolveQuery = {
          id: "explicitResolveQuery",
          refreshInterval: moment.duration(0),
          querySaga: function* (): IterableIterator<Promise<void>> {
            queryCalledCount++;
            yield new Promise((resolve, _reject) => {
              resolveQuery = resolve;
            });
          },
        };
        return (async function () {
          const tester = expectSaga(queryManagerSaga).dispatch(
            refresh(explicitResolveQuery),
          );

          const testFinished = tester.silentRun();
          await delay(0);

          // Query is now in progress, waiting on explicit resolve to
          // complete. Dispatch two autoRefresh requests, which should
          // still be serviced.
          tester
            .dispatch(autoRefresh(explicitResolveQuery))
            .dispatch(autoRefresh(explicitResolveQuery));

          // resolve the query, which should result in the query
          // immediately being called again due to the auto-refresh
          // count.
          await delay(0);
          resolveQuery();

          // Dispatch stopAutoRefresh and resolve the query. This
          // should still result in the query being called again,
          // because autoRefresh has not been fully decremented.
          tester.dispatch(stopAutoRefresh(explicitResolveQuery));
          await delay(0);
          resolveQuery();

          // Fully decrement stopAutoRefresh and resolve the query.
          // Query should not be called again.
          tester.dispatch(stopAutoRefresh(explicitResolveQuery));
          await delay(0);
          resolveQuery();
          await testFinished;

          expect(queryCalledCount).toBe(3);
        })();
      });
    });
  });

  describe("component unit tests", function () {
    describe("processQueryManagementAction", function () {
      it("initially processes first action", function () {
        const state = new ManagedQuerySagaState();
        state.channel = channel<any>();
        testSaga(processQueryManagementAction, state)
          .next()
          .take(state.channel);
      });
      it("correctly handles REFRESH action", function () {
        const state = new ManagedQuerySagaState();
        state.channel = channel<any>();
        testSaga(processQueryManagementAction, state)
          .next()
          .take(state.channel)
          .next(refresh(testQueryCounter))
          .isDone();
        const expected = new ManagedQuerySagaState();
        expected.channel = state.channel;
        expected.shouldRefreshQuery = true;
        expect(state).toEqual(expected);
      });
      it("correctly handles AUTO_REFRESH action", function () {
        const state = new ManagedQuerySagaState();
        state.channel = channel<any>();
        testSaga(processQueryManagementAction, state)
          .next()
          .take(state.channel)
          .next(autoRefresh(testQueryCounter))
          .isDone();
        const expected = new ManagedQuerySagaState();
        expected.channel = state.channel;
        expected.autoRefreshCount = 1;
        expect(state.autoRefreshCount).toBe(1);
        expect(state).toEqual(expected);
      });
      it("correctly handles STOP_AUTO_REFRESH action", function () {
        const state = new ManagedQuerySagaState();
        state.channel = channel<any>();
        testSaga(processQueryManagementAction, state)
          .next()
          .take(state.channel)
          .next(stopAutoRefresh(testQueryCounter))
          .isDone();
        const expected = new ManagedQuerySagaState();
        expected.channel = state.channel;
        expected.autoRefreshCount = -1;
        expect(state.autoRefreshCount).toBe(-1);
        expect(state).toEqual(expected);
      });
    });

    describe("timeToNextRefresh", function () {
      it("returns 0 if the query has never run.", function () {
        const state = new ManagedQuerySagaState();
        testSaga(timeToNextRefresh, state).next().returns(0);
      });
      it("applies refresh interval if specified.", function () {
        const state = new ManagedQuerySagaState();
        state.query = testQueryCounter;
        state.queryCompletedAt = moment(5000);
        testSaga(timeToNextRefresh, state)
          .next()
          .call(getMoment)
          .next(5030)
          .returns(testQueryCounter.refreshInterval.asMilliseconds() - 30);
      });
      it("applies default refresh interval if none specified.", function () {
        const state = new ManagedQuerySagaState();
        state.query = {
          id: "defaultQuery",
          querySaga: function* () {
            yield null;
          },
        };
        state.queryCompletedAt = moment(5000);
        testSaga(timeToNextRefresh, state)
          .next()
          .call(getMoment)
          .next(5030)
          .returns(DEFAULT_REFRESH_INTERVAL.asMilliseconds() - 30);
      });
      it("applies retry delay in error case if specified.", function () {
        const state = new ManagedQuerySagaState();
        state.query = testQueryCounter;
        state.queryCompletedAt = moment(5000);
        state.lastAttemptFailed = true;
        testSaga(timeToNextRefresh, state)
          .next()
          .call(getMoment)
          .next(5030)
          .returns(testQueryCounter.retryDelay.asMilliseconds() - 30);
      });
      it("applies default retry delay in error case if none specified.", function () {
        const state = new ManagedQuerySagaState();
        state.query = {
          id: "defaultQuery",
          querySaga: function* () {
            yield null;
          },
        };
        state.queryCompletedAt = moment(5000);
        state.lastAttemptFailed = true;
        testSaga(timeToNextRefresh, state)
          .next()
          .call(getMoment)
          .next(5030)
          .returns(DEFAULT_RETRY_DELAY.asMilliseconds() - 30);
      });
    });
  });
});
