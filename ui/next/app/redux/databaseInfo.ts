/**
 * This module maintains the state of read-only, data about databases and tables
 * Data is fetched from the '/_admin/v1/' endpoint via 'util/api'
 * Currently the data is always refreshed.
 */

import * as _ from "lodash";
import { Dispatch, combineReducers } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { getDatabaseList, getDatabaseDetails, getTableDetails } from "../util/api";

type DatabasesResponseMessage = cockroach.server.DatabasesResponseMessage;
type DatabaseDetailsResponseMessage = cockroach.server.DatabaseDetailsResponseMessage;
type TableDetailsResponseMessage = cockroach.server.TableDetailsResponseMessage;

export const DATABASES_REQUEST = "cockroachui/databases/DATABASES_REQUEST";
export const DATABASES_RECEIVE = "cockroachui/databases/DATABASES_RECEIVE";
export const DATABASES_ERROR = "cockroachui/databases/DATABASES_ERROR";
export const DATABASES_INVALIDATE = "cockroachui/databases/DATABASES_INVALIDATE";

export const DATABASE_DETAILS_REQUEST = "cockroachui/databases/DATABASE_DETAILS_REQUEST";
export const DATABASE_DETAILS_RECEIVE = "cockroachui/databases/DATABASE_DETAILS_RECEIVE";
export const DATABASE_DETAILS_ERROR = "cockroachui/databases/DATABASE_DETAILS_ERROR";
export const DATABASE_DETAILS_INVALIDATE = "cockroachui/databases/DATABASE_DETAILS_INVALIDATE";

export const TABLE_DETAILS_REQUEST = "cockroachui/databases/TABLE_DETAILS_REQUEST";
export const TABLE_DETAILS_RECEIVE = "cockroachui/databases/TABLE_DETAILS_RECEIVE";
export const TABLE_DETAILS_ERROR = "cockroachui/databases/TABLE_DETAILS_ERROR";
export const TABLE_DETAILS_INVALIDATE = "cockroachui/databases/TABLE_DETAILS_INVALIDATE";

export class DatabasesState {
  data: DatabasesResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

export class DatabaseDetailsState {
  data: DatabaseDetailsResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

export class TableDetailsState {
  data: TableDetailsResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

export class AllDatabaseDetailsState {
  [id: string]: DatabaseDetailsState
}

export class AllTableDetailsState {
  [id: string]: DatabaseDetailsState
}

/**
 * Represents the current state of a database list query.
 */
export class DatabaseInfoState {
  databases: DatabasesState;
  databaseDetails: AllDatabaseDetailsState;
  tableDetails: AllTableDetailsState;
}

/**
 * Redux reducer which processes actions related to the database list query.
 */
export function databasesReducer(state: DatabasesState = new DatabasesState(), action: Action): DatabasesState {
  switch (action.type) {
    case DATABASES_REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case DATABASES_RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<cockroach.server.DatabasesResponseMessage>;
      state = _.clone(state);
      state.inFlight = false;
      state.data = payload;
      state.valid = true;
      state.lastError = null;
      return state;
    case DATABASES_ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      state = _.clone(state);
      state.inFlight = false;
      state.lastError = error;
      state.valid = false;
      return state;
    case DATABASES_INVALIDATE:
      state = _.clone(state);
      state.valid = false;
      return state;
    default:
      return state;
  }
}

interface WithID<T> {
  id: string;
  data: T;
}

/* TODO: @mrtracy - you mentioned a generic cached data reducer. This should be
   added to that as well.
*/

export function databaseDetailsReducer(state: AllDatabaseDetailsState = new AllDatabaseDetailsState(), action: Action): AllDatabaseDetailsState {
  switch (action.type) {
    case DATABASE_DETAILS_REQUEST:
      let { payload: id } = action as PayloadAction<string>;
      state = _.clone(state);
      state[id] = _.clone(state[id]) || new DatabaseDetailsState();
      state[id].inFlight = true;
      return state;
    case DATABASE_DETAILS_RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<WithID<DatabaseDetailsResponseMessage>>;
      state = _.clone(state);
      state[payload.id] = _.clone(state[payload.id]) || new DatabaseDetailsState();
      state[payload.id].inFlight = false;
      state[payload.id].data = payload.data;
      state[payload.id].valid = true;
      state[payload.id].lastError = null;
      return state;
    case DATABASE_DETAILS_ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<WithID<Error>>;
      state = _.clone(state);
      state[error.id] = _.clone(state[error.id]) || new DatabaseDetailsState();
      state[error.id].inFlight = false;
      state[error.id].valid = false;
      state[error.id].lastError = error.data;
      return state;
    case DATABASE_DETAILS_INVALIDATE:
      let { payload: invalidID } = action as PayloadAction<string>;
      state = _.clone(state);
      state[invalidID] = _.clone(state[invalidID]) || new DatabaseDetailsState();
      state[invalidID].valid = false;
      return state;
    default:
      return state;
  }
}

export function tableDetailsReducer(state: AllTableDetailsState = new AllTableDetailsState(), action: Action): AllTableDetailsState {
  switch (action.type) {
    case TABLE_DETAILS_REQUEST:
      let { payload: id } = action as PayloadAction<string>;
      state = _.clone(state);
      state[id] = _.clone(state[id]) || new TableDetailsState();
      state[id].inFlight = true;
      return state;
    case TABLE_DETAILS_RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<WithID<TableDetailsResponseMessage>>;
      state = _.clone(state);
      state[payload.id] = _.clone(state[payload.id]) || new TableDetailsState();
      state[payload.id].inFlight = false;
      state[payload.id].data = payload.data;
      state[payload.id].valid = true;
      state[payload.id].lastError = null;
      return state;
    case TABLE_DETAILS_ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<WithID<Error>>;
      state = _.clone(state);
      state[error.id] = _.clone(state[error.id]) || new TableDetailsState();
      state[error.id].inFlight = false;
      state[error.id].valid = false;
      state[error.id].lastError = error.data;
      return state;
    case TABLE_DETAILS_INVALIDATE:
      let { payload: invalidID } = action as PayloadAction<string>;
      state = _.clone(state);
      state[invalidID] = _.clone(state[invalidID]) || new TableDetailsState();
      state[invalidID].valid = false;
      return state;
    default:
      return state;
  }
}

export default combineReducers({
  databases: databasesReducer,
  databaseDetails: databaseDetailsReducer,
  tableDetails: tableDetailsReducer,
});

/**
 * requestDatabaseList indicates that an asynchronous database list request has been
 * started.
 */
export function requestDatabases(): Action {
  return {
    type: DATABASES_REQUEST,
  };
}

/**
 * receiveDatabaseList indicates that database list has been successfully fetched from
 * the server.
 *
 * @param statuses Status data returned from the server.
 */
export function receiveDatabases(databases: DatabasesResponseMessage): PayloadAction<DatabasesResponseMessage> {
  return {
    type: DATABASES_RECEIVE,
    payload: databases,
  };
}

/**
 * errorDatabaseList indicates that an error occurred while fetching database list.
 *
 * @param error Error that occurred while fetching.
 */
export function errorDatabases(error: Error): PayloadAction<Error> {
  return {
    type: DATABASES_ERROR,
    payload: error,
  };
}

export function invalidateDatabases(error: Error): Action {
  return {
    type: DATABASES_INVALIDATE,
  };
}

export function requestDatabaseDetails(id: string): PayloadAction<string> {
  return {
    type: DATABASE_DETAILS_REQUEST,
    payload: id,
  };
}

export function receiveDatabaseDetails(id: string, data: DatabaseDetailsResponseMessage): PayloadAction<WithID<DatabaseDetailsResponseMessage>> {
  return {
    type: DATABASE_DETAILS_RECEIVE,
    payload: { id, data },
  };
}

export function errorDatabaseDetails(id: string, data: Error): PayloadAction<WithID<Error>> {
  return {
    type: DATABASE_DETAILS_ERROR,
    payload: { id, data },
  };
}

export function invalidateDatabaseDetails(id: string): PayloadAction<string> {
  return {
    type: DATABASE_DETAILS_INVALIDATE,
    payload: id,
  };
}

export function requestTableDetails(id: string): PayloadAction<string> {
  return {
    type: TABLE_DETAILS_REQUEST,
    payload: id,
  };
}

export function receiveTableDetails(id: string, data: TableDetailsResponseMessage): PayloadAction<WithID<TableDetailsResponseMessage>> {
  return {
    type: TABLE_DETAILS_RECEIVE,
    payload: { id, data },
  };
}

export function errorTableDetails(id: string, data: Error): PayloadAction<WithID<Error>> {
  return {
    type: TABLE_DETAILS_ERROR,
    payload: { id, data },
  };
}

export function invalidateTableDetails(id: string): PayloadAction<string> {
  return {
    type: TABLE_DETAILS_INVALIDATE,
    payload: id,
  };
}

/**
 * refreshDatabaseList is the primary action creator that should be used with a database
 * list. Dispatching it will attempt to asynchronously refresh the database list
 * if its results are no longer considered valid.
 */
export function refreshDatabases() {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { databaseInfo }: {databaseInfo: DatabaseInfoState} = getState();

    // TODO: Currently we only refresh when the page is revisited. Eventually
    // we should refresh on a timer like other components.

    // Don't refresh if a request is already in flight
    if (databaseInfo.databases && databaseInfo.databases.inFlight) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestDatabases());

    // Fetch database list from the servers and convert it to JSON.
    getDatabaseList().then((dbList) => {
      // Dispatch the processed results to the store.
      dispatch(receiveDatabases(dbList));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorDatabases(error));
    });
  };
}

export function refreshDatabaseDetails(id: string) {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { databaseInfo }: {databaseInfo: DatabaseInfoState} = getState();

    // Don't refresh if a request is already in flight
    if (databaseInfo.databaseDetails && databaseInfo.databaseDetails[id] && databaseInfo.databaseDetails[id].inFlight) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestDatabaseDetails(id));

    // Fetch database list from the servers and convert it to JSON.
    getDatabaseDetails({database: id}).then((data) => {
      // Dispatch the processed results to the store.
      dispatch(receiveDatabaseDetails(id, data));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorDatabaseDetails(id, error));
    });
  };
}

export function refreshTableDetails(database: string, table: string) {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { databaseInfo }: {databaseInfo: DatabaseInfoState} = getState();

    // TODO (maxlang): this won't work for exotic database/table names.
    let id = database + "." + table;

    // Don't refresh if a request is already in flight
    if (databaseInfo.tableDetails && databaseInfo.tableDetails[id] && databaseInfo.tableDetails[id].inFlight) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestTableDetails(id));

    // Fetch database list from the servers and convert it to JSON.
    getTableDetails({ database, table }).then((data) => {
      // Dispatch the processed results to the store.
      dispatch(receiveTableDetails(id, data));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorTableDetails(id, error));
    });
  };
}
