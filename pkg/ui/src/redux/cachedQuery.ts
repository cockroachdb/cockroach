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

/**
 * This module maintains the state of read-only data fetched from the cluster.
 * Data is fetched from an API endpoint in either 'util/api' or
 * 'util/cockroachlabsAPI'
 */

import _ from "lodash";
import { Action } from "redux";
import moment from "moment";
import { call, put, select } from "redux-saga/effects";

import { ManagedQuery } from "src/redux/queryManager/saga";
import nextState from "src/util/nextState";
import { AdminUIState } from "./state";

import { PayloadAction } from "src/interfaces/action";

const SET_STATE = "cockroachui/CachedQueryState/SET_STATE";

export interface CachedQueryResultState {
  [key: string]: any;
}

export interface CachedQueryPayload {
  key: string;
  result: any;
}

// Reducer that handles updates to cached query results.
export function cachedQueryResultReducer(
  state: CachedQueryResultState = {}, action: Action,
): CachedQueryResultState {
  if (_.isNil(action)) {
    return state;
  }

  switch (action.type) {
    case SET_STATE:
      const { payload } = action as PayloadAction<CachedQueryPayload>;
      return nextState(state, {
        [payload.key]: payload.result,
      });
    default:
      return state;
  }
}

export function setResult<TResult>(
  query: CachedQuery<TResult>, result: any,
): PayloadAction<CachedQueryPayload> {
  return {
    type: SET_STATE,
    payload: {
      key: query.id,
      result,
    },
  };
}

interface CachedQueryOptions {
  // The interval at which this query should be refreshed if it is being
  // auto-refreshed. Default is ten seconds.
  refreshInterval?: moment.Duration;
  // The delay after which an auto-refreshing query will be retried after
  // a failure. Default is two seconds.
  retryDelay?: moment.Duration;
}

export const defaultCachedQueryOptions: CachedQueryOptions = {
  refreshInterval: moment.duration(10, "s"),
  retryDelay: moment.duration(2, "s"),
};

// CachedQuery represents the simplest expression of a ManagedQuery: a function
// which makes one remote request and stores the result of that request without
// modification. The function provided to CachedQuery can return either a plain
// result, a promise for a result, or can be generator function which eventully
// returns the result
//
// CachedQuery also provides two selectors which can be used to select (from
// AdminUIState) either the currently cached result of the query *or* the most
// recently encountered error when trying to query.
export class CachedQuery<TResult> implements ManagedQuery {
  // The interval at which this query should be refreshed if it is being
  // auto-refreshed. Default is ten seconds.
  public refreshInterval: moment.Duration;
  // The delay after which an auto-refreshing query will be retried after
  // a failure. Default is two seconds.
  public retryDelay: moment.Duration;

  public querySaga = (() => {
    const self = this;
    return function *(): any {
      const result = yield call(self.query);=
      yield put(setResult(self, result));
    };
  })();

  public selectResult = (state: AdminUIState): TResult => {
    return state.cachedQueries[this.id];
  }

  public selectError = (state: AdminUIState) => {
    const queryData = state.queryManager[this.id];
    return queryData ? queryData.lastError : undefined;
  }

  constructor(
    public id: string,
    private query: () => IterableIterator<any> | Promise<TResult> | TResult,
    options?: CachedQueryOptions,
  ) {
    options = _.assign({}, defaultCachedQueryOptions, options);
    this.refreshInterval = options.refreshInterval;
    this.retryDelay = options.retryDelay;
  }
}

// Keyed queries, where several related queries are maintained which hit the
// same endpoint with different requests.
// NOTE: This memoization function is the intersection of performance and
// convenience; it uses a global variable. Its intention is to be used similarly
// to template instantiation; that is, when calling keyedQuery with a specific
// key, the expected result *must* be deterministic.
const keyedQueryMemo: {[key: string]: CachedQuery<any>} = {};

export function keyedQuery<TResult>(
  key: string,
  fn: () => IterableIterator<any> | Promise<TResult> | TResult,
  options?: CachedQueryOptions,
): CachedQuery<TResult> {
  if (keyedQueryMemo[key]) {
    return keyedQueryMemo[key];
  }
  const newQuery = new CachedQuery(key, fn, options);
  keyedQueryMemo[key] = newQuery;
  return newQuery;
}
