/**
 * This module maintains the state of read-only, data about databases and tables
 * Data is fetched from the '/_admin/v1/' endpoint via 'util/api'
 * Currently the data is always refreshed.
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { getDatabaseList } from "../util/api";

const REQUEST = "cockroachui/databases/REQUEST";
const RECEIVE = "cockroachui/databases/RECEIVE";
const ERROR = "cockroachui/databases/ERROR";

/**
 * Represents the current state of a database list query.
 */
export class DatabaseListState {
  // True if an asynchronous fetch is currently in progress.
  inFlight = false;
  // Holds the last error returned from a failed query.
  lastError: Error;
  // Holds the last set of DatabasesResponse objects queried from the server.
  databaseList: cockroach.server.DatabasesResponse;
}

/**
 * Redux reducer which processes actions related to the database list query.
 */
export default function reducer(state: DatabaseListState = new DatabaseListState(), action: Action): DatabaseListState {
  // @mrtracy when would this happen?
  if (action === undefined) {
    return;
  }

  let stateClone = _.clone(state);

  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      stateClone.inFlight = true;
      return stateClone;
    case RECEIVE:
      // The results of a request have been received.
      let { payload: databaseList } = action as PayloadAction<cockroach.server.DatabasesResponse>;
      stateClone.inFlight = false;
      stateClone.databaseList = databaseList;
      stateClone.lastError = null;
      return stateClone;
    case ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      stateClone.inFlight = false;
      stateClone.lastError = error;
      return stateClone;
    default:
      return state;
  }
}

/**
 * requestDatabaseList indicates that an asynchronous database list request has been
 * started.
 */
export function requestDatabaseList(): Action {
  return {
    type: REQUEST,
  };
}

/**
 * receiveDatabaseList indicates that database list has been successfully fetched from
 * the server.
 *
 * @param statuses Status data returned from the server.
 */
export function receiveDatabaseList(databases: cockroach.server.DatabasesResponse): PayloadAction<cockroach.server.DatabasesResponse> {
  return {
    type: RECEIVE,
    payload: databases,
  };
}

/**
 * errorDatabaseList indicates that an error occurred while fetching database list.
 *
 * @param error Error that occurred while fetching.
 */
export function errorDatabaseList(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

/**
 * refreshDatabaseList is the primary action creator that should be used with a database
 * list. Dispatching it will attempt to asynchronously refresh the database list
 * if its results are no longer considered valid.
 */
export function refreshDatabaseList() {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { databaseList }: {databaseList: DatabaseListState} = getState();
    // If there is a query in flight, or if the most recent results are still
    // valid, do nothing.
    if (databaseList.inFlight || databaseList.databaseList) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestDatabaseList());

    // Fetch database list from the servers and convert it to JSON.
    getDatabaseList().then((dbList) => {
      // Dispatch the processed results to the store.
      dispatch(receiveDatabaseList(dbList));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorDatabaseList(error));
    });
  };
}

