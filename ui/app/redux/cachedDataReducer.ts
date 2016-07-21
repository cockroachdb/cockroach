/**
 * This module maintains the state of read-only data fetched from the cluster.
 * Data is fetched from an API endpoint in either 'util/api' or 'util/cockroachlabsAPI'
 */

import * as _ from "lodash";
import { Dispatch } from "redux";
import { Action, PayloadAction, WithID } from "../interfaces/action";
import { assert } from "chai";

// CachedDataReducerState is used to track the state of the cached data.
export class CachedDataReducerState<TResponseMessage> {
  data: TResponseMessage; // the latest data received
  inFlight = false; // true if a request is in flight
  valid = false; // true if data has been received and has not been invalidated
  lastError: Error; // populated with the most recent error, if the last request failed
}

// KeyedCachedDataReducerState is used to track the state of the cached data
// that is associated with a key.
export class KeyedCachedDataReducerState<TResponseMessage> {
  [id: string]: CachedDataReducerState<TResponseMessage>;
}

/**
 * CachedDataReducer is a wrapper object that contains a redux reducer and
 * a number of redux actions. The "reducer" attribute is the reducer and the "refresh"
 * attribute is the main action creator that refreshes the data when dispatched.
 *
 * Each instance of this class is instantiated with an api endpoint with request type
 * TRequest and response type Promise<TResponseMessage>.
 *
 * NOTE: If an idGenerator is specified in the constructor, the data will be cached under a key
 */
export class CachedDataReducer<TRequest, TResponseMessage> {
  // Track all the currently seen namespaces, to ensure there isn't a conflict
  private static namespaces: { [actionNamespace: string]: boolean } = {};

  // Actions
  REQUEST: string; // make a new request
  RECEIVE: string; // receive new data
  ERROR: string; // request encountered an error
  INVALIDATE: string; // invalidate data

  // defined here because
  private idGenerator: (req: TRequest) => string;

  /**
   *  There are two constructors for CachedDataReducer.
   *  apiEndpoint - The API endpoint the "refresh" function will use to refresh data.
   *  actionNamespace - A unique namespace for the redux actions.
   *  idGenerator (optional) - A function that takes a TRequest and returns a string. Used as a key for the stored state data.
   *  invalidationPeriodMillis (optional) - The number of milliseconds after data is received after which it will be invalidated.
   */
  constructor(apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, actionNamespace: string, invalidationPeriodMillis?: number);
  constructor(apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, actionNamespace: string, idGenerator: (req: TRequest) => string, invalidationPeriodMillis?: number);
  constructor(protected apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, public actionNamespace: string, idGenerator: ((req: TRequest) => string) | number, protected invalidationPeriodMillis?: number) {
    // check actionNamespace
    assert.notProperty(CachedDataReducer.namespaces, actionNamespace, "Expected actionNamespace to be unique.");
    CachedDataReducer.namespaces[actionNamespace] = true;

    this.REQUEST = `cockroachui/CachedDataReducer/${actionNamespace}/REQUEST`;
    this.RECEIVE = `cockroachui/CachedDataReducer/${actionNamespace}/RECEIVE`;
    this.ERROR = `cockroachui/CachedDataReducer/${actionNamespace}/ERROR`;
    this.INVALIDATE = `cockroachui/CachedDataReducer/${actionNamespace}/INVALIDATE`;

    // Handle constructor overloading
    // If idGenerator isn't a function, the caller used the simpler form of the constructor with no idGenerator.
    // We assign this.idGenerator to a function and that always returns a default key
    // and reassign idGenerator to invalidationPeriodMillis.
    if (typeof idGenerator === "function") {
      this.idGenerator = idGenerator;
    } else {
      this.invalidationPeriodMillis = idGenerator;
    }
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

  /**
   * Redux keyed reducer which pulls out the id from the action payload
   * and then runs the reducer on the action with only the data.
   */
  keyedReducer = (state = new KeyedCachedDataReducerState<TResponseMessage>(), action: Action): KeyedCachedDataReducerState<TResponseMessage> => {
    switch (action.type) {
      case this.REQUEST:
      case this.RECEIVE:
      case this.ERROR:
      case this.INVALIDATE:
        let { id, data } = (action as PayloadAction<WithID<TResponseMessage|Error>>).payload;
        state = _.clone(state);
        state[id] = this.reducer(state[id], {
          type: action.type,
          payload: data,
        } as PayloadAction<typeof data>);
        return state;
      default:
        return state;
    }
  }

  // wrapIfKeyed wraps the given data with an ID if idGenerator exists.
  wrapIfKeyed<T>(req: TRequest, data?: T): WithID<T> | T {
    return this.idGenerator ? { id: this.idGenerator(req), data } : data;
  }

  // requestData is the REQUEST action creator.
  requestData = (req: TRequest): PayloadAction<typeof undefined|WithID<typeof undefined>> => {
    return {
      type: this.REQUEST,
      payload: this.wrapIfKeyed(req),
    };
  }

  // receiveData is the RECEIVE action creator.
  receiveData = (data: TResponseMessage, req: TRequest): PayloadAction<TResponseMessage|WithID<TResponseMessage>> => {
    return {
      type: this.RECEIVE,
      payload: this.wrapIfKeyed(req, data),
    };
  }

  // errorData is the ERROR action creator.
  errorData = (error: Error, req: TRequest): PayloadAction<Error|WithID<Error>> => {
    return {
      type: this.ERROR,
      payload: this.wrapIfKeyed(req, error),
    };
  }

  // invalidateData is the INVALIDATE action creator.
  invalidateData = (req: TRequest): PayloadAction<typeof undefined|WithID<typeof undefined>> => {
    return {
      type: this.INVALIDATE,
      payload: this.wrapIfKeyed(req),
    };
  }

  // getReducerState returns this reducer's state given a state object and a TRequest object.
  // The TRequest object is needed to pass into the idGenerator.
  getReducerState(state: any, req: TRequest): CachedDataReducerState<TResponseMessage> {
    return this.idGenerator ?
      state.cachedData[this.actionNamespace][this.idGenerator(req)] :
      state.cachedData[this.actionNamespace];
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
      let state: CachedDataReducerState<TResponseMessage> = this.getReducerState(getState(), req);

      if (state && (state.inFlight || (_.isNumber(this.invalidationPeriodMillis) && state.valid))) {
        return;
      }

      // Note that after dispatching requestData, state.inFlight is true
      dispatch(this.requestData(req));
      // Fetch data from the servers. The promise is only returned for use in tests.
      return this.apiEndpoint(req).then((data) => {
        // Dispatch the results to the store.
        dispatch(this.receiveData(data, req));
      }).catch((error: Error) => {
        // If an error occurred during the fetch, dispatch the received error to the store.
        dispatch(this.errorData(error, req));
      }).then(() => {
        // If an invalidation period is specified, invalidate the data after that time has elapsed.
        if (_.isNumber(this.invalidationPeriodMillis)) {
          setTimeout(() => dispatch(this.invalidateData(req)), this.invalidationPeriodMillis);
        }
      });
    };
  }
}
