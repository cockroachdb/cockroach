/**
 * This module maintains the state of read-only data about the cluster.
 * Data is fetched from the '/_admin/v1/cluster' endpoint via 'util/api'.
 */

import "whatwg-fetch";
import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";

interface Function {
    name: string;
}

export class CachedDataReducerState<TResponseMessage> {
  data: TResponseMessage;
  inFlight = false;
  valid = false;
  lastError: Error;
}

export class CachedDataReducer<TRequest, TResponseMessage> {
  REQUEST: string;
  RECEIVE: string;
  ERROR: string;
  INVALIDATE: string;

  constructor(private apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, private namespace: string, private invalidationPeriod?: number) {
    this.REQUEST = `cockroachui/${namespace}/REQUEST`;
    this.RECEIVE = `cockroachui/${namespace}/RECEIVE`;
    this.ERROR = `cockroachui/${namespace}/ERROR`;
    this.INVALIDATE = `cockroachui/${namespace}/INVALIDATE`;
  }

  /**
   * Redux reducer which processes actions related to the cluster query.
   */
  reducer = (state = new CachedDataReducerState<TResponseMessage>(), action: Action): CachedDataReducerState<TResponseMessage> => {
    switch (action.type) {
      case this.REQUEST:
        // A request is in progress.
        state = _.clone(state);
        state.inFlight = true;
        return state;
      case this.RECEIVE:
        // The results of a request have been received.
        let { payload } = action as PayloadAction<TResponseMessage>;
        state = _.clone(state);
        state.inFlight = false;
        state.data = payload;
        state.valid = true;
        state.lastError = null;
        return state;
      case this.ERROR:
        // A request failed.
        let { payload: error } = action as PayloadAction<Error>;
        state = _.clone(state);
        state.inFlight = false;
        state.lastError = error;
        state.valid = false;
        return state;
      case this.INVALIDATE:
        state = _.clone(state);
        state.valid = false;
        return state;
      default:
        return state;
    }
  }

  requestData = (): Action => {
    return {
      type: this.REQUEST,
    };
  }

  receiveData = (cluster: TResponseMessage): PayloadAction<TResponseMessage> => {
    return {
      type: this.RECEIVE,
      payload: cluster,
    };
  }

  errorData = (error: Error): PayloadAction<Error> => {
    return {
      type: this.ERROR,
      payload: error,
    };
  }

  invalidateData = (): Action => {
    return {
      type: this.INVALIDATE,
    };
  }

  /**
   * refreshCluster is the primary action creator that should be used to determine cluster info.
   * Dispatching it will attempt to asynchronously refresh the cluster info
   * if its results are no longer considered valid.
   */
  refresh = (req?: TRequest) => {
    return (dispatch: Dispatch, getState: () => any) => {
      let { cluster }: {cluster: CachedDataReducerState<TResponseMessage>} = getState();

      // Don't refresh if a request is already in flight
      if (cluster && (cluster.inFlight || (_.isNumber(this.invalidationPeriod) && cluster.valid))) {
        return;
      }

      // Note that a query is currently in flight.
      dispatch(this.requestData());
      // Fetch cluster from the servers and convert it to JSON.
      // The promise is returned for testing.
      return this.apiEndpoint(req).then((data) => {
        // Dispatch the processed results to the store.
        dispatch(this.receiveData(data));
      }).catch((error: Error) => {
        // If an error occurred during the fetch, dispatch the received error to
        // the store.
        dispatch(this.errorData(error));
      }).then(() => {
        if (_.isNumber(this.invalidationPeriod)) {
          setTimeout(() => dispatch(this.invalidateData()), this.invalidationPeriod);
        }
      });
    };
  }
}
