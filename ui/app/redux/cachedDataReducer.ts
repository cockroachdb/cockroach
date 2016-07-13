/**
 * This module maintains the state of read-only data fetched from the cluster.
 * Data is fetched from an API endpoint in either 'util/api' or 'util/cockroachlabsAPI'
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction } from "../interfaces/action";
import { assert } from "chai";

// CachedDataReducerState is used to track the state of the cached data.
export class CachedDataReducerState<TResponseMessage> {
  data: TResponseMessage; // the latest data received
  inFlight = false; // true if a request is in flight
  valid = false; // true if data has been received and has not been invalidated
  lastError: Error; // populated with the most recent error, if the last request failed
}

/**
 * CachedDataReducer is a wrapper object that contains a redux reducer and
 * a number of redux actions. The "reducer" attribute is the reducer and the "refresh"
 * attribute is the main action creator that refreshes the data when dispatched.
 *
 * Each instance of this class is instantiated with an api endpoint with request type
 * TRequest and response type Promise<TResponseMessage>.
 *
 * TODO(maxlang): Currently this only works with simple API endpoints where there's
 * only one value for the entire application. We need to add support for keyed data
 * reducers where multiple responses are stored based on which value is requested.
 */
export class CachedDataReducer<TRequest, TResponseMessage> {
  // Track all the currently seen namespaces, to ensure there isn't a conflict
  private static namespaces: { [actionNamespace: string]: boolean } = {};

  // Actions
  REQUEST: string; // make a new request
  RECEIVE: string; // receive new data
  ERROR: string; // request encountered an error
  INVALIDATE: string; // invalidate data

/**
 *  apiEndpoint - The API endpoint the "refresh" function will use to refresh data.
 *  actionNamespace - A unique namespace for the redux actions.
 *  invalidationPeriodMillis - The number of milliseconds after data is received after which it will be invalidated.
 */
  constructor(private apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, public actionNamespace: string, private invalidationPeriodMillis?: number) {
    // check actionNamespace
    assert.notProperty(CachedDataReducer.namespaces, actionNamespace, "Expected actionNamespace to be unique.");
    CachedDataReducer.namespaces[actionNamespace] = true;

    this.REQUEST = `cockroachui/CachedDataReducer/${actionNamespace}/REQUEST`;
    this.RECEIVE = `cockroachui/CachedDataReducer/${actionNamespace}/RECEIVE`;
    this.ERROR = `cockroachui/CachedDataReducer/${actionNamespace}/ERROR`;
    this.INVALIDATE = `cockroachui/CachedDataReducer/${actionNamespace}/INVALIDATE`;
  }

  /**
   * Redux reducer which processes actions related to the api endpoint query.
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
        // The data is invalidated.
        state = _.clone(state);
        state.valid = false;
        return state;
      default:
        return state;
    }
  }

  // requestData is the REQUEST action creator.
  requestData = (): Action => {
    return {
      type: this.REQUEST,
    };
  }

  // receiveData is the RECEIVE action creator.
  receiveData = (cluster: TResponseMessage): PayloadAction<TResponseMessage> => {
    return {
      type: this.RECEIVE,
      payload: cluster,
    };
  }

  // errorData is the ERROR action creator.
  errorData = (error: Error): PayloadAction<Error> => {
    return {
      type: this.ERROR,
      payload: error,
    };
  }

  // invalidateData is the INVALIDATE action creator.
  invalidateData = (): Action => {
    return {
      type: this.INVALIDATE,
    };
  }

  /**
   * refresh is the primary action creator that should be used to refresh the cached data.
   * Dispatching it will attempt to asynchronously refresh the cached data if and only if:
   * - a request is not in flight AND
   *   - its results are not considered valid OR
   *   - it has no invalidation period (otherwise the data would never be invalidated)
   */
  refresh = <S>(req?: TRequest) => {
    return (dispatch: Dispatch<S>, getState: () => any) => {
      let state: CachedDataReducerState<TResponseMessage> = getState().cachedData[this.actionNamespace];

      if (state && (state.inFlight || (_.isNumber(this.invalidationPeriodMillis) && state.valid))) {
        return;
      }

      // Note that after dispatching requestData, state.inFlight is true
      dispatch(this.requestData());
      // Fetch data from the servers. The promise is only returned for use in tests.
      return this.apiEndpoint(req).then((data) => {
        // Dispatch the results to the store.
        dispatch(this.receiveData(data));
      }).catch((error: Error) => {
        // If an error occurred during the fetch, dispatch the received error to the store.
        dispatch(this.errorData(error));
      }).then(() => {
        // If an invalidation period is specified, invalidate the data after that time has elapsed.
        if (_.isNumber(this.invalidationPeriodMillis)) {
          setTimeout(() => dispatch(this.invalidateData()), this.invalidationPeriodMillis);
        }
      });
    };
  }
}
