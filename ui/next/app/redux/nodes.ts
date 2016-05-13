/**
 * This module maintains the state of a read-only, periodically refreshed query
 * for the status of all nodes in the cluster. Data is fetched from the
 * '/_status/nodes' endpoint.
 */

import _ = require("lodash");
import { Dispatch } from "redux";
import assign = require("object-assign");
import { Action, PayloadAction } from "../interfaces/action";
import { NodeStatus, RollupStoreMetrics } from "../util/proto";
import "isomorphic-fetch";

import * as protos from "../js/protos";

const REQUEST = "cockroachui/nodes/REQUEST";
const RECEIVE = "cockroachui/nodes/RECEIVE";
const ERROR = "cockroachui/nodes/ERROR";
const INVALIDATE = "cockroachui/nodes/INVALIDATE";

/**
 * Represents the current state of a nodes status query.
 */
export class NodeStatusState {
  // True if an asynchronous fetch is currently in progress.
  inFlight = false;
  // True if the contents of 'statuses' is considered current.
  isValid = false;
  // Holds the last error returned from a failed query.
  lastError: Error;
  // Holds the last set of NodeStatus objects queried from the server.
  statuses: NodeStatus[];
}

/**
 * Redux reducer which processes actions related to the nodes status query.
 */
export default function reducer(state: NodeStatusState = new NodeStatusState(), action: Action): NodeStatusState {
  switch (action.type) {
    case REQUEST:
      // A request is in progress.
      return assign({}, state, {
        inFlight: true,
      });
    case RECEIVE:
      // The results of a request have been received.
      let { payload: statuses } = action as PayloadAction<NodeStatus[]>;
      return assign({}, state, {
        inFlight: false,
        isValid: true,
        statuses: statuses,
        lastError: null,
      });
    case ERROR:
      // A request failed.
      let { payload: error } = action as PayloadAction<Error>;
      return assign({}, state, {
        inFlight: false,
        isValid: false,
        lastError: error,
      });
    case INVALIDATE:
      // The currently cached request should no longer be considered valid.
      return assign({}, state, {
        isValid: false,
      });
    default:
      return state;
  }
}

/**
 * requestNodes indicates that an asynchronous node status request has been
 * started.
 */
export function requestNodes(): Action {
  return {
    type: REQUEST,
  };
}

/**
 * receiveNodes indicates that node status has been successfully fetched from
 * the server.
 *
 * @param statuses Status data returned from the server.
 */
export function receiveNodes(statuses: NodeStatus[]): PayloadAction<NodeStatus[]> {
  return {
    type: RECEIVE,
    payload: statuses,
  };
}

/**
 * errorNodes indicates that an error occurred while fetching node status.
 *
 * @param error Error that occurred while fetching.
 */
export function errorNodes(error: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: error,
  };
}

/**
 * invalidateNodes will invalidate the currently stored node status results.
 */
export function invalidateNodes(): Action {
  return {
    type: INVALIDATE,
  };
}

// The duration for which a status result is considered valid.
const statusValidDurationMS = 10 * 1000;

/**
 * refreshNodes is the primary action creator that should be used with a node
 * status. Dispatching it will attempt to asynchronously refresh the node status
 * if its results are no longer considered valid.
 */
export function refreshNodes() {
  return (dispatch: Dispatch, getState: () => any): void => {
    let { nodes }: {nodes: NodeStatusState} = getState();

    // If there is a query in flight, or if the most recent results are still
    // valid, do nothing.
    if (nodes.inFlight || nodes.isValid) {
      return;
    }

    // Note that a query is currently in flight.
    dispatch(requestNodes());

    // Fetch node status from the servers and convert it to JSON.
    fetch("/_status/nodes").then((response) => {
      return response.json() as cockroach.server.NodesResponse;
    }).then((json) => {
      // Extract the result, an array of NodeStatus objects.
      let { nodes: jsonResult } = json;
      // Roll up store status metrics, additively, on each node status.
      let result = _.map(jsonResult, (nsObj) => {
        let ns = new protos.cockroach.server.status.NodeStatus(nsObj);
        RollupStoreMetrics(ns);
        return ns;
      });

      // Dispatch the processed results to the store.
      dispatch(receiveNodes(result));

      // Set a timeout which will later invalidate the results.
      setTimeout(() => dispatch(invalidateNodes()), statusValidDurationMS);
    }).catch((error) => {
      // If an error occurred during the fetch, dispatch the received error to
      // the store.
      dispatch(errorNodes(error));
    });
  };
}

