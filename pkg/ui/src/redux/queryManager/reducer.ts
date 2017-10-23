import moment from "moment";
import _ from "lodash";
import { Action } from "redux";

const QUERY_BEGIN = "cockroachui/queries/QUERY_BEGIN";
const QUERY_ERROR = "cockroachui/queries/QUERY_ERROR";
const QUERY_COMPLETE = "cockroachui/queries/QUERY_COMPLETE";

class QueryBeginAction implements Action {
    type: typeof QUERY_BEGIN;
    id: string;
}

interface QueryErrorAction extends Action {
    type: typeof QUERY_ERROR;
    id: string;
    error: Error;
    timestamp: moment.Moment;
}

interface QueryCompleteAction extends Action {
    type: typeof QUERY_COMPLETE;
    id: string;
    timestamp: moment.Moment;
}

type QueryAction = QueryBeginAction | QueryErrorAction | QueryCompleteAction;

/**
 * queryBegin is dispatched by the query manager whenever the query with the
 * given ID has started execution.
 */
export function queryBegin(id: string): QueryBeginAction {
    return {
        type: QUERY_BEGIN,
        id,
    };
}

/**
 * queryError is dispatched by the query manager whenever the query with the
 * given ID has stopped due to an error condition. This action contains the
 * returned error, along with the timestamp when the error was received.
 */
export function queryError(id: string, error: Error, timestamp: moment.Moment): QueryErrorAction {
    return {
        type: QUERY_ERROR,
        id,
        error,
        timestamp,
    };
}

/**
 * queryComplete is dispatched by the query manager whenever the query with the
 * given ID has completed successfully. It includes the timestamp when the query
 * was completed.
 */
export function queryComplete(id: string, timestamp: moment.Moment): QueryCompleteAction {
    return {
        type: QUERY_COMPLETE,
        id,
        timestamp,
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
    queriedAt: moment.Moment = null;
}

/**
 * managedQueryReducer reduces actions for a single managed query.
 */
export function managedQueryReducer(
    state = new ManagedQueryState(), action: QueryAction,
): ManagedQueryState {
    if (_.isNil(action)) {
        return state;
    }

    switch (action.type) {
        case QUERY_BEGIN:
            state = _.clone(state);
            state.isRunning = true;
            state.lastError = null;
            state.queriedAt = null;
            return state;
        case QUERY_ERROR:
            state = _.clone(state);
            state.isRunning = false;
            state.lastError = action.error;
            state.queriedAt = action.timestamp;
            return state;
        case QUERY_COMPLETE:
            state = _.clone(state);
            state.isRunning = false;
            state.queriedAt = action.timestamp;
            return state;
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
    state: QueryManagerState = {}, action: QueryAction,
): QueryManagerState {
    if (_.isNil(action)) {
        return state;
    }

    switch (action.type) {
        case QUERY_BEGIN:
        case QUERY_ERROR:
        case QUERY_COMPLETE:
            return {
                ...state,
                [action.id]: managedQueryReducer(state[action.id], action),
            };
        default:
            return state;
    }
}
