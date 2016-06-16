/**
 * This module maintains the state of read-only data about the cluster
 * Data is fetched from the '/_admin/v1/cluster' endpoint via 'util/api'
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { getCluster } from "../util/api";

type ClusterResponseMessage = cockroach.server.serverpb.ClusterResponseMessage;

export const REQUEST = "cockroachui/cluster/REQUEST";
export const RECEIVE = "cockroachui/cluster/RECEIVE";
export const ERROR = "cockroachui/cluster/ERROR";
export const INVALIDATE = "cockroachui/cluster/INVALIDATE";

export class ClusterState {
  data: ClusterResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

/**
 * Redux reducer which processes actions related to the cluster query.
 */
export default function reducer(state: ClusterState = new ClusterState(), action: Action): ClusterState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      state = _.clone(state);
      state.inFlight = true;
      return state;
    case RECEIVE:
      // The results of a request have been received.
      let { payload } = action as PayloadAction<ClusterResponseMessage>;
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

export function requestCluster(): Action {
  return {
    type: REQUEST,
  };
}

export function receiveCluster(cluster: ClusterResponseMessage): PayloadAction<ClusterResponseMessage> {
  return {
    type: RECEIVE,
    payload: cluster,
  };
}

export function errorCluster(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

export function invalidateCluster(): Action {
  return {
    type: INVALIDATE,
  };
}

/**
 * refreshCluster is the primary action creator that should be used to determine cluster info.
 * Dispatching it will attempt to asynchronously refresh the cluster info
 * if its results are no longer considered valid.
 */
export function refreshCluster() {
  return (dispatch: Dispatch, getState: () => any) => {
    let { cluster }: {cluster: ClusterState} = getState();

    // TODO: Currently we only refresh when the page is revisited. Eventually
    // we should refresh on a timer like other components.

    // Don't refresh if a request is already in flight
    if (cluster && (cluster.inFlight || cluster.valid)) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestCluster());
    // Fetch cluster from the servers and convert it to JSON.
    // The promise is returned for testing.
    return getCluster().then((e) => {
      // Dispatch the processed results to the store.
      dispatch(receiveCluster(e));
    }).catch((error: Error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorCluster(error));
    });
  };
}
