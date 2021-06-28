// Copyright 2018 The Cockroach Authors.
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

import nextState from "src/util/nextState";

const QUERY_BEGIN = "cockroachui/queries/QUERY_BEGIN";
const QUERY_ERROR = "cockroachui/queries/QUERY_ERROR";
const QUERY_COMPLETE = "cockroachui/queries/QUERY_COMPLETE";

interface QueryBeginAction extends Action {
  type: typeof QUERY_BEGIN;
  payload: {
    id: string;
  };
}

interface QueryErrorAction extends Action {
  type: typeof QUERY_ERROR;
  payload: {
    id: string;
    error: Error;
    timestamp: moment.Moment;
  };
}

interface QueryCompleteAction extends Action {
  type: typeof QUERY_COMPLETE;
  payload: {
    id: string;
    timestamp: moment.Moment;
  };
}

type QueryAction = QueryBeginAction | QueryErrorAction | QueryCompleteAction;

/**
 * queryBegin is dispatched by the query manager whenever the query with the
 * given ID has started execution.
 */
export function queryBegin(id: string): QueryBeginAction {
  return {
    type: QUERY_BEGIN,
    payload: {
      id,
    },
  };
}

/**
 * queryError is dispatched by the query manager whenever the query with the
 * given ID has stopped due to an error condition. This action contains the
 * returned error, along with the timestamp when the error was received.
 */
export function queryError(
  id: string,
  error: Error,
  timestamp: moment.Moment,
): QueryErrorAction {
  return {
    type: QUERY_ERROR,
    payload: {
      id,
      error,
      timestamp,
    },
  };
}

/**
 * queryComplete is dispatched by the query manager whenever the query with the
 * given ID has completed successfully. It includes the timestamp when the query
 * was completed.
 */
export function queryComplete(
  id: string,
  timestamp: moment.Moment,
): QueryCompleteAction {
  return {
    type: QUERY_COMPLETE,
    payload: {
      id,
      timestamp,
    },
  };
}

/**
 * ManagedQueryState maintains the current state for a single managed query.
 */
export class ManagedQueryState {
  // True if this query is currently running asynchronously.
  isRunning = false;
  // If the previous attempt to run this query ended with an error, this field
  // contains that error.
  lastError: Error = null;
  // Contains the timestamp when the query last compeleted, regardless of
  // whether it succeeded or encountered an error.
  completedAt: moment.Moment = null;
}

/**
 * managedQueryReducer reduces actions for a single managed query.
 */
export function managedQueryReducer(
  state = new ManagedQueryState(),
  action: QueryAction,
): ManagedQueryState {
  switch (action.type) {
    case QUERY_BEGIN:
      return nextState(state, {
        isRunning: true,
        lastError: null,
        completedAt: null,
      });
    case QUERY_ERROR:
      return nextState(state, {
        isRunning: false,
        lastError: action.payload.error,
        completedAt: action.payload.timestamp,
      });
    case QUERY_COMPLETE:
      return nextState(state, {
        isRunning: false,
        lastError: null,
        completedAt: action.payload.timestamp,
      });
    default:
      return state;
  }
}

/**
 * QueryManagerState maintains the state for all queries being managed.
 */
export interface QueryManagerState {
  [id: string]: ManagedQueryState;
}

/**
 * queryManagerReducer reduces actions for a query collection, multiplexing
 * incoming actions to individual query reducers by ID.
 */
export function queryManagerReducer(
  state: QueryManagerState = {},
  action: QueryAction,
): QueryManagerState {
  switch (action.type) {
    case QUERY_BEGIN:
    case QUERY_ERROR:
    case QUERY_COMPLETE:
      return {
        ...state,
        [action.payload.id]: managedQueryReducer(
          state[action.payload.id],
          action,
        ),
      };
    default:
      return state;
  }
}
