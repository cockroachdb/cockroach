/**
 * This module maintains the state of read-only data about version currency.
 * Data is fetched from the Cockroach Labs' servers.
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { versionCheck } from "../util/cockroachlabsAPI";

import { VersionList } from "../interfaces/cockroachlabs";

export const REQUEST = "cockroachui/version/REQUEST";
export const RECEIVE = "cockroachui/version/RECEIVE";
export const ERROR = "cockroachui/version/ERROR";
export const INVALIDATE = "cockroachui/version/INVALIDATE";

export class VersionState {
  data: VersionList;
  inFlight = false;
  valid = false;
  lastError: Error;
}

/**
 * Redux reducer which processes actions related to the version query.
 */
export default function reducer(state: VersionState = new VersionState(), action: Action): VersionState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<VersionList>;
      state = _.clone(state);
      state.inFlight = false;
      state.data = payload;
      state.valid = true;
      state.lastError = null;
      return state;
    case ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      state = _.clone(state);
      state.inFlight = false;
      state.lastError = error;
      state.valid = false;
      return state;
    case INVALIDATE:
      state = _.clone(state);
      state.valid = false;
      return state;
    default:
      return state;
  }
}

export function requestVersion(): Action {
  return {
    type: REQUEST,
  };
}

export function receiveVersion(version: VersionList): PayloadAction<VersionList> {
  return {
    type: RECEIVE,
    payload: version,
  };
}

export function errorVersion(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

export function invalidateVersion(): Action {
  return {
    type: INVALIDATE,
  };
}

/**
 * refreshVersion is the primary action creator that should be used to determine
 * whether the version of Cockroach is out of date and how many newer versions there are.
 * Dispatching it will attempt to asynchronously refresh the version list
 * if its results are no longer considered valid.
 */
export function refreshVersion(clusterID: string, buildtag: string) {
  return (dispatch: Dispatch, getState: () => any) => {
    let { version }: {version: VersionState} = getState();

    // Don't refresh if a request is already in flight
    if (version && (version.inFlight || version.valid)) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestVersion());
    // Fetch version list from the Cockroach Labs servers
    // The promise is returned for testing.
    return versionCheck(clusterID, buildtag).then((e) => {
      // Dispatch the processed results to the store.
      dispatch(receiveVersion(e));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorVersion(error));
    });
  };
}
