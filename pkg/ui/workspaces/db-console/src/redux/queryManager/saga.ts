// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { Action } from "redux";
import { channel, Task, Channel } from "redux-saga";
import {
  call,
  cancel,
  fork,
  join,
  put,
  race,
  take,
  delay,
} from "redux-saga/effects";

import { queryBegin, queryComplete, queryError } from "./reducer";

export const DEFAULT_REFRESH_INTERVAL = moment.duration(10, "s");
export const DEFAULT_RETRY_DELAY = moment.duration(2, "s");

/**
 * A ManagedQuery describes an asynchronous query that can have its execution
 * managed by the query management saga.
 *
 * Managed queries are executed by dispatching redux actions:
 * + refresh(query) can be used to immediately run the query.
 * + autoRefresh(query) will begin automatically refreshing the query
 *   automatically on a cadence.
 * + stopAutoRefresh(query) will stop automatically refreshing the query.
 *
 * Note that "autoRefresh" and "stopAutoRefresh" events are counted; the
 * query will refresh as long as there is at least one auto_refresh() action
 * that has not been canceled by a stop_auto_refresh().
 */
interface ManagedQuery {
  // A string ID that distinguishes this query from all other queries.
  id: string;
  // The interval at which this query should be refreshed if it is being
  // auto-refreshed. Default is ten seconds.
  refreshInterval?: moment.Duration;
  // The delay after which an auto-refreshing query will be retried after
  // a failure. Default is two seconds.
  retryDelay?: moment.Duration;
  // A redux saga task that should execute the query and put its results into
  // the store. This method can yield any of the normal redux saga effects.
  querySaga: () => IterableIterator<any>;
}

export const QUERY_REFRESH = "cockroachui/query/QUERY_REFRESH";
export const QUERY_AUTO_REFRESH = "cockroachui/query/QUERY_AUTO_REFRESH";
export const QUERY_STOP_AUTO_REFRESH =
  "cockroachui/query/QUERY_STOP_AUTO_REFRESH";

interface QueryRefreshAction extends Action {
  type: typeof QUERY_REFRESH;
  query: ManagedQuery;
}

interface QueryAutoRefreshAction extends Action {
  type: typeof QUERY_AUTO_REFRESH;
  query: ManagedQuery;
}

interface QueryStopRefreshAction extends Action {
  type: typeof QUERY_STOP_AUTO_REFRESH;
  query: ManagedQuery;
}

type QueryManagementAction =
  | QueryRefreshAction
  | QueryAutoRefreshAction
  | QueryStopRefreshAction;

/**
 * refresh indicates that a managed query should run immediately.
 */
export function refresh(query: ManagedQuery): QueryRefreshAction {
  return {
    type: QUERY_REFRESH,
    query: query,
  };
}

/**
 * autoRefresh indicates that a managed query should start automatically
 * refreshing on a regular cadence.
 */
export function autoRefresh(query: ManagedQuery): QueryAutoRefreshAction {
  return {
    type: QUERY_AUTO_REFRESH,
    query: query,
  };
}

/**
 * stopAutoRefresh indicates that a managed query no longer needs to automatically
 * refresh.
 */
export function stopAutoRefresh(query: ManagedQuery): QueryStopRefreshAction {
  return {
    type: QUERY_STOP_AUTO_REFRESH,
    query: query,
  };
}

/**
 * Contains state information about a managed query which has been run by the
 * manager.
 */
export class ManagedQuerySagaState {
  // The query being managed.
  query: ManagedQuery;
  // The saga task which is managing this query, either running it or
  // auto-refreshing it. If the auto-refresh count drops to zero, this saga
  // may complete (the manager will start a new saga if the query starts
  // again).
  sagaTask: Task;
  // A saga channel that the main query management saga will use to dispatch
  // events to the query-specific saga.
  channel: Channel<QueryManagementAction>;
  // The number of components currently requesting that this query be
  // auto-refreshed. This is the result of incrementing on autoRefresh()
  // actions and decrementing on stopAutoRefresh() actions.
  autoRefreshCount: number = 0;
  // If true, the query saga needs to run the underlying query immediately. If
  // this is false, the saga will delay until it needs to be refreshed (or
  // will exit if autoRefreshCount is zero,)
  shouldRefreshQuery: boolean;
  // Contains the time at which the query last completed, either successfully
  // or with an error.
  queryCompletedAt: moment.Moment;
  // True if the last attempt to run this query ended in an error.
  lastAttemptFailed: boolean;
}

/**
 * Contains state needed by a running query manager saga.
 */
export class QueryManagerSagaState {
  private queryStates: { [queryId: string]: ManagedQuerySagaState } = {};

  // Retrieve the ManagedQuerySagaState for the query with the given id.
  // Creates a new state object if the given id has not yet been encountered.
  getQueryState(query: ManagedQuery) {
    const { id } = query;
    if (!Object.prototype.hasOwnProperty.call(this.queryStates, id)) {
      this.queryStates[id] = new ManagedQuerySagaState();
      this.queryStates[id].query = query;
    }
    return this.queryStates[id];
  }
}

/**
 * The top-level saga responsible for dispatching events to the child sagas
 * which manage individual queries.
 */
export function* queryManagerSaga() {
  const queryManagerState = new QueryManagerSagaState();

  while (true) {
    const qmAction: QueryManagementAction = yield take([
      QUERY_REFRESH,
      QUERY_AUTO_REFRESH,
      QUERY_STOP_AUTO_REFRESH,
    ]);

    // Fork a saga to manage this query if it is not already running.
    const state = queryManagerState.getQueryState(qmAction.query);
    if (!taskIsRunning(state.sagaTask)) {
      state.channel = channel<QueryManagementAction>();
      state.sagaTask = yield fork(managedQuerySaga, state);
    }
    yield put(state.channel, qmAction);
  }
}

/**
 * Saga used to manages the execution of an individual query.
 */
export function* managedQuerySaga(state: ManagedQuerySagaState) {
  // Process the initial action.
  yield call(processQueryManagementAction, state);

  // Run loop while we either need to run the query immediately, or if there
  // are any components requesting this query should auto refresh.
  while (state.shouldRefreshQuery || state.autoRefreshCount > 0) {
    if (state.shouldRefreshQuery) {
      yield call(refreshQuery, state);
    }

    if (state.autoRefreshCount > 0) {
      yield call(waitForNextRefresh, state);
    }
  }
}

/**
 * Processes the next QueryManagementAction dispatched to this query.
 */
export function* processQueryManagementAction(state: ManagedQuerySagaState) {
  const { type } = (yield take(state.channel)) as QueryManagementAction;
  switch (type) {
    case QUERY_REFRESH:
      state.shouldRefreshQuery = true;
      break;
    case QUERY_AUTO_REFRESH:
      state.autoRefreshCount += 1;
      break;
    case QUERY_STOP_AUTO_REFRESH:
      state.autoRefreshCount -= 1;
      break;
    default:
      break;
  }
}

/**
 * refreshQuery is the execution state of the query management saga
 * when the query is being executed.
 */
export function* refreshQuery(state: ManagedQuerySagaState) {
  // TODO(davidh): add return type annotations to generators
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const queryTask = yield fork(runQuery, state);
  while (queryTask.isRunning()) {
    // While the query is running, we still need to increment or
    // decrement the auto-refresh count.
    yield race({
      finished: join(queryTask),
      nextAction: call(processQueryManagementAction, state),
    });
  }
  state.shouldRefreshQuery = false;
}

/**
 * waitForNextRefresh is the execution state of the query management saga
 * when it is waiting to automatically refresh.
 */
export function* waitForNextRefresh(state: ManagedQuerySagaState) {
  // If this query should be auto-refreshed, compute the time until
  // the query is out of date. If the request is already out of date,
  // refresh the query immediately.

  // TODO(davidh): add return type annotations to generators
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const delayTime = yield call(timeToNextRefresh, state);
  if (delayTime <= 0) {
    state.shouldRefreshQuery = true;
    return;
  }

  // TODO(davidh): add return type annotations to generators
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  const delayTask = yield fork(delayGenerator, delayTime);
  while (delayTask.isRunning()) {
    yield race({
      finished: join(delayTask),
      nextAction: call(processQueryManagementAction, state),
    });
    // If a request comes in to run the query immediately, or if the
    // auto-refresh count drops to zero, cancel the delay task.
    if (state.shouldRefreshQuery || state.autoRefreshCount <= 0) {
      yield cancel(delayTask);
      return;
    }
  }

  state.shouldRefreshQuery = true;
}

/**
 * Calculates the number of milliseconds until the given query needs to be
 * refreshed.
 */
export function* timeToNextRefresh(state: ManagedQuerySagaState) {
  if (!state.queryCompletedAt) {
    return 0;
  }

  let interval: moment.Duration;
  if (state.lastAttemptFailed) {
    interval = state.query.retryDelay || DEFAULT_RETRY_DELAY;
  } else {
    interval = state.query.refreshInterval || DEFAULT_REFRESH_INTERVAL;
  }

  // Yielding to moment lets us easily mock time in tests.
  const now: moment.Moment = yield call(getMoment);
  const dueAt = state.queryCompletedAt.clone().add(interval);
  return dueAt.diff(now);
}

/**
 * Runs the underlying query of the supplied managed query.
 *
 * This task will catch any errors thrown by the query.
 *
 * This task is also responsible for putting information about the query into
 * the query management reducer (the saga itself doesn't use the information in
 * the reducer; it is provided in order to give visibility to other components
 * in the system).
 */
export function* runQuery(state: ManagedQuerySagaState) {
  const { id, querySaga } = state.query;

  let err: Error;
  try {
    yield put(queryBegin(id));
    yield call(querySaga);
  } catch (e) {
    err = e;
  }

  // Yielding to moment lets us easily mock time in tests.
  state.queryCompletedAt = yield call(getMoment);
  if (err) {
    state.lastAttemptFailed = true;
    yield put(queryError(id, err, state.queryCompletedAt));
  } else {
    state.lastAttemptFailed = false;
    yield put(queryComplete(id, state.queryCompletedAt));
  }
}

// getMoment is a function that can be dispatched to redux-saga's "call" effect.
// Saga doesn't like using the bare moment object because moment is also an
// object and Saga chooses the wrong overload.
export function getMoment() {
  return moment();
}

// delayGenerator wraps the delay function so that it can be forked. Note that
// redux saga itself does support forking arbitrary promise-returning functions,
// but redux-saga-test-plan does not.
// https://github.com/jfairbank/redux-saga-test-plan/issues/139
function* delayGenerator(delayTime: number) {
  yield delay(delayTime);
}

/**
 * Utility that returns true if the provided task is running. Helpful for use
 * when a task-containing variable may be null.
 */
function taskIsRunning(task: Task | null) {
  return task && task.isRunning();
}
