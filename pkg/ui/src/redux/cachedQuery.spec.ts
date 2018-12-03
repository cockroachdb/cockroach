// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import { assert } from "chai";
import _ from "lodash";
import { combineReducers } from "redux";
import { queryManagerSaga } from "./queryManager/saga";
import { defaultCachedQueryOptions, CachedQuery, cachedQueryResultReducer, keyedQuery } from "./cachedQuery";
import { refresh } from "./queryManager/saga";
import { delay } from "redux-saga";
import { call } from "redux-saga/effects";

import { expectSaga } from "redux-saga-test-plan";
import { queryManagerReducer } from "./queryManager/reducer";
import * as moment from "moment";

describe("cachedQuery", function() {
  it("has correct defaults for refreshInterval/retryDelay", function () {
    const query = new CachedQuery("test", () => null);
    assert.equal(query.refreshInterval, defaultCachedQueryOptions.refreshInterval);
    assert.equal(query.retryDelay, defaultCachedQueryOptions.retryDelay);
  });

  it("correctly assigns refreshInterval/retryDelay", function () {
    const query = new CachedQuery("test", () => null, {
      refreshInterval: moment.duration(50, "s"),
      retryDelay: moment.duration(75, "s"),
    });
    assert.equal(query.refreshInterval.asMilliseconds(), 50000);
    assert.equal(query.retryDelay.asMilliseconds(), 75000);
  });

  it("correctly overwrites values", function() {
    let x = 0;
    const countQ = new CachedQuery("count", () => {
      x++;
      return x;
    });
    return expectSaga(queryManagerSaga)
      .withReducer(combineReducers({
        cachedDataNew: cachedQueryResultReducer,
        queryManager: queryManagerReducer,
      }))
      .dispatch(refresh(countQ))
      .dispatch(refresh(countQ))
      .dispatch(refresh(countQ))
      .run()
      .then(runResult => {
        assert.equal(countQ.selectResult(runResult.storeState), 3);
      });
  });

  it("correctly sets and selects results and errors", function() {
    const genQ = new CachedQuery<string>(
      "id1",
      function *() {
        yield call(delay, 0);
        return "value";
      },
    );

    const promiseQ = new CachedQuery(
      "id2",
      function() {
        return new Promise((resolve, _reject) => setTimeout(() => resolve("fulfilled"), 1));
      },
    );

    const testErr = new Error("error");
    const errorQ = new CachedQuery(
      "id3",
      function() {
        throw testErr;
      },
    );

    const errorPromiseQ = new CachedQuery(
      "id4",
      function() {
        return new Promise((_resolve, reject) => setTimeout(() => reject(testErr), 1));
      },
    );

    return expectSaga(queryManagerSaga)
      .withReducer(combineReducers({
        cachedDataNew: cachedQueryResultReducer,
        queryManager: queryManagerReducer,
      }))
      .dispatch(refresh(genQ))
      .dispatch(refresh(errorQ))
      .dispatch(refresh(promiseQ))
      .dispatch(refresh(errorPromiseQ))
      .delay(5)
      .run()
      .then(runResult => {
        assert.equal(genQ.selectResult(runResult.storeState), "value");
        assert.isNull(genQ.selectError(runResult.storeState));

        assert.equal(promiseQ.selectResult(runResult.storeState), "fulfilled");
        assert.isNull(promiseQ.selectError(runResult.storeState));

        assert.isUndefined(errorQ.selectResult(runResult.storeState));
        assert.equal(errorQ.selectError(runResult.storeState), testErr);

        assert.isUndefined(errorPromiseQ.selectResult(runResult.storeState));
        assert.equal(errorPromiseQ.selectError(runResult.storeState), testErr);
      });
  });
});

describe("keyedQuery", function() {
  it("correctly memoizes queries", function() {
    const kq1 = keyedQuery("testkey.1", () => "val1");
    const kq2 = keyedQuery("testkey.2", () => "val2");
    const kq3 = keyedQuery("testkey.1", () => "val1");
    assert.equal(kq1, kq3);
    assert.notEqual(kq1, kq2);
  });

  it("has correct defaults for refreshInterval/retryDelay", function () {
    const query = keyedQuery("testkey.defaultoptions", () => "val1");
    assert.equal(query.refreshInterval, defaultCachedQueryOptions.refreshInterval);
    assert.equal(query.retryDelay, defaultCachedQueryOptions.retryDelay);
  });

  it("correctly assigns refreshInterval/retryDelay", function () {
    const query = keyedQuery("testkey.customoptions", () => "val2", {
      refreshInterval: moment.duration(50, "s"),
      retryDelay: moment.duration(75, "s"),
    });
    assert.equal(query.refreshInterval.asMilliseconds(), 50000);
    assert.equal(query.retryDelay.asMilliseconds(), 75000);
  });
});
