// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * This module maintains the state of read-only data fetched from the cluster.
 * Data is fetched from an API endpoint in either 'util/api' or
 * 'util/cockroachlabsAPI'
 */

import assert from "assert";

import { util as clusterUiUtil } from "@cockroachlabs/cluster-ui";
import { push } from "connected-react-router";
import { createHashHistory } from "history";
import clone from "lodash/clone";
import isNil from "lodash/isNil";
import moment from "moment-timezone";
import { Action } from "redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";

import { PayloadAction, WithRequest } from "src/interfaces/action";
import { getLoginPage } from "src/redux/login";
import { APIRequestFn } from "src/util/api";

import { clearTenantCookie } from "./cookies";

const { isForbiddenRequestError, maybeError } = clusterUiUtil;

export interface WithPaginationRequest {
  page_size: number;
  page_token: string;
}

export interface WithPaginationResponse {
  next_page_token: string;
}

// CachedDataReducerState is used to track the state of the cached data.
export class CachedDataReducerState<TResponseMessage> {
  data?: TResponseMessage; // the latest data received
  inFlight = false; // true if a request is in flight
  valid = false; // true if data has been received and has not been invalidated
  requestedAt?: moment.Moment; // Timestamp when data was last requested.
  setAt?: moment.Moment; // Timestamp when this data was last updated.
  lastError?: Error; // populated with the most recent error, if the last request failed
  unauthorized = false; // If lastError was a 403 error, we avoid refreshing.
}

// KeyedCachedDataReducerState is used to track the state of the cached data
// that is associated with a key.
// This error is a false positive because we do use 'TResponseMessage' to type
// CachedDataReducerState. We'll suppress the error to make the linter happy.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export class KeyedCachedDataReducerState<TResponseMessage> {
  [id: string]: CachedDataReducerState<TResponseMessage>;
}

export class PaginatedCachedDataReducerState<TResponseMessage> {
  // the latest data received
  data?: {
    [id: string]: TResponseMessage;
  };
  inFlight = false; // true if a request is in flight
  valid = false; // true if data has been received and has not been invalidated
  requestedAt?: moment.Moment; // Timestamp when data was last requested.
  setAt?: moment.Moment; // Timestamp when this data was last updated.
  lastError?: Error; // populated with the most recent error, if the last request failed
  unauthorized = false; // If lastError was a 403 error, we avoid refreshing.

  constructor() {
    this.data = {};
  }
}

/**
 * CachedDataReducer is a wrapper object that contains a redux reducer and a
 * number of redux actions. The reducer method is the reducer and the refresh
 * method is the main action creator that refreshes the data when dispatched.
 *
 * Each instance of this class is instantiated with an api endpoint with request
 * type TRequest and response type Promise<TResponseMessage>.
 */
export class CachedDataReducer<
  TRequest,
  TResponseMessage,
  TActionNamespace extends string = string,
> {
  // Track all the currently seen namespaces, to ensure there isn't a conflict
  private static namespaces: { [actionNamespace: string]: boolean } = {};

  // Actions
  REQUEST: string; // make a new request
  RECEIVE: string; // receive new data
  ERROR: string; // request encountered an error
  INVALIDATE: string; // invalidate data
  INVALIDATE_ALL: string; // invalidate all data on keyed cache
  pendingRequestStarted: moment.Moment | null;

  /**
   * apiEndpoint - The API endpoint used to refresh data.
   * actionNamespace - A unique namespace for the redux actions.
   * invalidationPeriod (optional) - The duration after
   *   data is received after which it will be invalidated.
   * requestTimeout (optional)
   *  allowReplacementRequests (optional) - allows requests to be replaced
   * while they are in flight.
   */
  constructor(
    protected apiEndpoint: APIRequestFn<TRequest, TResponseMessage>,
    public actionNamespace: TActionNamespace,
    protected invalidationPeriod?: moment.Duration,
    protected requestTimeout?: moment.Duration,
    protected allowReplacementRequests = false,
  ) {
    // check actionNamespace
    assert(
      !Object.prototype.hasOwnProperty.call(
        CachedDataReducer.namespaces,
        actionNamespace,
      ),
      "Expected actionNamespace to be unique.",
    );
    CachedDataReducer.namespaces[actionNamespace] = true;

    this.pendingRequestStarted = null;
    this.REQUEST = `cockroachui/CachedDataReducer/${actionNamespace}/REQUEST`;
    this.RECEIVE = `cockroachui/CachedDataReducer/${actionNamespace}/RECEIVE`;
    this.ERROR = `cockroachui/CachedDataReducer/${actionNamespace}/ERROR`;
    this.INVALIDATE = `cockroachui/CachedDataReducer/${actionNamespace}/INVALIDATE`;
    this.INVALIDATE_ALL = `cockroachui/CachedDataReducer/${actionNamespace}/INVALIDATE_ALL`;
  }

  /**
   * setTimeSource overrides the source of timestamps used by this component.
   * Intended for use in tests only.
   */
  setTimeSource(timeSource: { (): moment.Moment }) {
    this.timeSource = timeSource;
  }

  /**
   * Redux reducer which processes actions related to the api endpoint query.
   */
  reducer = (
    state = new CachedDataReducerState<TResponseMessage>(),
    action: Action,
  ): CachedDataReducerState<TResponseMessage> => {
    if (isNil(action)) {
      return state;
    }

    switch (action.type) {
      case this.REQUEST:
        // A request is in progress.
        state = clone(state);
        state.requestedAt = this.timeSource();
        state.inFlight = true;
        return state;
      case this.RECEIVE: {
        // The results of a request have been received.
        const { payload } = action as PayloadAction<
          WithRequest<TResponseMessage, TRequest>
        >;
        state = clone(state);
        state.inFlight = false;
        state.data = payload.data;
        state.setAt = this.timeSource();
        state.valid = true;
        state.lastError = null;
        return state;
      }
      case this.ERROR: {
        // A request failed.
        const { payload: error } = action as PayloadAction<
          WithRequest<Error, TRequest>
        >;
        state = clone(state);
        state.inFlight = false;
        state.lastError = error.data;
        state.valid = false;
        if (isForbiddenRequestError(error.data)) {
          state.unauthorized = true;
        }
        return state;
      }
      case this.INVALIDATE:
        // The data is invalidated.
        state = clone(state);
        state.valid = false;
        return state;
      default:
        return state;
    }
  };

  // requestData is the REQUEST action creator.
  requestData = (
    request?: TRequest,
  ): PayloadAction<WithRequest<void, TRequest>> => {
    return {
      type: this.REQUEST,
      payload: { request },
    };
  };

  // receiveData is the RECEIVE action creator.
  receiveData = (
    data: TResponseMessage,
    request?: TRequest,
  ): PayloadAction<WithRequest<TResponseMessage, TRequest>> => {
    return {
      type: this.RECEIVE,
      payload: { request, data },
    };
  };

  // errorData is the ERROR action creator.
  errorData = (
    error: Error,
    request?: TRequest,
  ): PayloadAction<WithRequest<Error, TRequest>> => {
    return {
      type: this.ERROR,
      payload: { request, data: error },
    };
  };

  // invalidateData is the INVALIDATE action creator.
  invalidateData = (
    request?: TRequest,
  ): PayloadAction<WithRequest<void, TRequest>> => {
    return {
      type: this.INVALIDATE,
      payload: { request },
    };
  };

  // invalidateAllData is the INVALIDATE_ALL action creator.
  invalidateAllData = (
    request?: TRequest,
  ): PayloadAction<WithRequest<void, TRequest>> => {
    return {
      type: this.INVALIDATE_ALL,
      payload: { request },
    };
  };

  cancelPendingRequest = (): void => {
    this.pendingRequestStarted = null;
  };

  setPendingRequestTime = (): moment.Moment => {
    this.pendingRequestStarted = moment.utc();
    return this.pendingRequestStarted;
  };

  /**
   * refresh is the primary action creator that should be used to refresh the
   * cached data. Dispatching it will attempt to asynchronously refresh the
   * cached data if and only if:
   * - a request is not in flight AND
   *   - its results are not considered valid OR
   *   - it has no invalidation period
   *
   * req - the request associated with this call to refresh. It includes any
   *   parameters passed to the API call.
   * stateAccessor (optional) - a helper function that accesses this reducer's
   *   state given the global state object
   */
  refresh = <S>(
    req?: TRequest,
    stateAccessor = (state: any, _req: TRequest) =>
      state.cachedData[this.actionNamespace],
  ): ThunkAction<any, S, any, Action> => {
    return (dispatch: ThunkDispatch<S, unknown, Action>, getState: () => S) => {
      const state: CachedDataReducerState<TResponseMessage> = stateAccessor(
        getState(),
        req,
      );

      if (
        state &&
        ((state.inFlight && !this.allowReplacementRequests) ||
          state.unauthorized ||
          (this.invalidationPeriod && state.valid))
      ) {
        return;
      }

      this.cancelPendingRequest();
      const pendingRequestStarted = this.setPendingRequestTime();

      // Note that after dispatching requestData, state.inFlight is true
      dispatch(this.requestData(req));
      // Fetch data from the servers. Return the promise for use in tests.
      return this.apiEndpoint(req, this.requestTimeout)
        .then(
          data => {
            // Check if we are replacing requests. If so, do not update the reducer
            // state if this is not the latest request.
            if (
              this.allowReplacementRequests &&
              this.pendingRequestStarted !== pendingRequestStarted
            ) {
              return;
            }
            // Dispatch the results to the store.
            dispatch(this.receiveData(data, req));
          },
          (error: Error) => {
            if (
              this.allowReplacementRequests &&
              this.pendingRequestStarted !== pendingRequestStarted
            ) {
              return;
            }

            // TODO(couchand): This is a really myopic way to check for HTTP
            // codes.  However, at the moment that's all that the underlying
            // timeoutFetch offers.  Major changes to this plumbing are warranted.
            if (error.message === "Unauthorized") {
              // Clearing the tenant cookie is necessary when we force a login
              // because otherwise the DB routing will continue routing to that
              // specific tenant.
              clearTenantCookie();
              // TODO(couchand): This is an unpleasant dependency snuck in here...
              const { location } = createHashHistory();
              if (
                location &&
                !location.pathname.startsWith("/login") &&
                !location.pathname.startsWith("/jwt")
              ) {
                dispatch(push(getLoginPage(location)));
              }
            }

            // If an error occurred during the fetch, add it to the store.
            // Wait 1s to record the error to avoid spamming errors.
            // TODO(maxlang): Fix error handling more comprehensively.
            // Tracked in #8699
            setTimeout(() => dispatch(this.errorData(error, req)), 1000);
          },
        )
        .then(() => {
          if (
            this.allowReplacementRequests &&
            this.pendingRequestStarted !== pendingRequestStarted
          ) {
            return;
          }
          // Invalidate data after the invalidation period if one exists.
          if (this.invalidationPeriod) {
            setTimeout(
              () => dispatch(this.invalidateData(req)),
              this.invalidationPeriod.asMilliseconds(),
            );
          }
        });
    };
  };

  private timeSource: { (): moment.Moment } = () => moment();
}

/**
 * KeyedCachedDataReducer is a wrapper object that contains a redux reducer and
 * an instance of CachedDataReducer. The reducer method is the reducer and the
 * refresh method is the main action creator that refreshes the data when
 * dispatched. All action creators and the basic reducer are from the
 * CachedDataReducer instance.
 *
 * Each instance of this class is instantiated with an api endpoint with request
 * type TRequest and response type Promise<TResponseMessage>.
 */
export class KeyedCachedDataReducer<
  TRequest,
  TResponseMessage,
  TActionNamespace extends string = string,
> {
  cachedDataReducer: CachedDataReducer<
    TRequest,
    TResponseMessage,
    TActionNamespace
  >;

  /**
   * apiEndpoint - The API endpoint used to refresh data.
   * actionNamespace - A unique namespace for the redux actions.
   * requestToID - A function that takes a TRequest and returns a string. Used
   *   as a key to store data returned from that request
   * invalidationPeriod (optional) - The duration after
   *   data is received after which it will be invalidated.
   * requestTimeout (optional)
   * apiEndpoint, actionNamespace, invalidationPeriod and requestTimeout are all
   * passed to the CachedDataReducer constructor
   */
  constructor(
    protected apiEndpoint: (req: TRequest) => Promise<TResponseMessage>,
    public actionNamespace: TActionNamespace,
    private requestToID: (req: TRequest) => string,
    protected invalidationPeriod?: moment.Duration,
    protected requestTimeout?: moment.Duration,
  ) {
    this.cachedDataReducer = new CachedDataReducer<
      TRequest,
      TResponseMessage,
      TActionNamespace
    >(apiEndpoint, actionNamespace, invalidationPeriod, requestTimeout);
  }

  /**
   * setTimeSource overrides the source of timestamps used by this component.
   * Intended for use in tests only.
   */
  setTimeSource(timeSource: { (): moment.Moment }) {
    this.cachedDataReducer.setTimeSource(timeSource);
  }

  /**
   * refresh calls the internal CachedDataReducer's refresh function using a
   * default stateAccessor that indexes in to the state based on a key generated
   * from the request.
   */
  refresh = (
    req?: TRequest,
    stateAccessor = (state: any, r: TRequest) =>
      state.cachedData[this.cachedDataReducer.actionNamespace][
        this.requestToID(r)
      ],
  ) => this.cachedDataReducer.refresh(req, stateAccessor);

  /**
   * Keyed redux reducer which pulls out the id from the action payload and then
   * runs the CachedDataReducer reducer on the action.
   */
  reducer = (
    state = new KeyedCachedDataReducerState<TResponseMessage>(),
    action: Action,
  ): KeyedCachedDataReducerState<TResponseMessage> => {
    if (isNil(action)) {
      return state;
    }

    switch (action.type) {
      case this.cachedDataReducer.REQUEST:
      case this.cachedDataReducer.RECEIVE:
      case this.cachedDataReducer.ERROR:
      case this.cachedDataReducer.INVALIDATE: {
        const { request } = (
          action as PayloadAction<
            WithRequest<TResponseMessage | Error | void, TRequest>
          >
        ).payload;
        const id = this.requestToID(request);
        state = clone(state);
        state[id] = this.cachedDataReducer.reducer(state[id], action);
        return state;
      }
      case this.cachedDataReducer.INVALIDATE_ALL: {
        state = clone(state);
        const keys = Object.keys(state);
        for (const key in keys) {
          state[key] = this.cachedDataReducer.reducer(state[key], action);
        }
        return state;
      }
      default:
        return state;
    }
  };
}

export class PaginatedCachedDataReducer<
  TRequest extends WithPaginationRequest,
  TResponseMessage extends WithPaginationResponse,
  TActionNamespace extends string = string,
> {
  cachedDataReducer: CachedDataReducer<
    TRequest,
    TResponseMessage,
    TActionNamespace
  >;

  CLEAR_DATA: string; // clear previously received data.
  RECEIVE_COMPLETED: string; // action indicates that there's no more data to receive.

  constructor(
    protected apiEndpoint: APIRequestFn<TRequest, TResponseMessage>,
    public actionNamespace: TActionNamespace,
    private requestToID: (req: TRequest) => string,
    private pageLimit: number = 1000,
    protected invalidationPeriod?: moment.Duration,
    protected requestTimeout?: moment.Duration,
  ) {
    this.cachedDataReducer = new CachedDataReducer<
      TRequest,
      TResponseMessage,
      TActionNamespace
    >(apiEndpoint, actionNamespace, invalidationPeriod, requestTimeout);

    this.CLEAR_DATA = `cockroachui/CachedDataReducer/${actionNamespace}/CLEAR_DATA`;
    this.RECEIVE_COMPLETED = `cockroachui/CachedDataReducer/${actionNamespace}/RECEIVE_COMPLETED`;
  }

  // receiveCompleted is an action creator to indicate that there is no more data to receive.
  receiveCompleted = (
    request?: TRequest,
  ): PayloadAction<WithRequest<Error, TRequest>> => {
    return {
      type: this.RECEIVE_COMPLETED,
      payload: { request },
    };
  };

  // clearData is an action creator to delete previously received data.
  clearData = (
    request?: TRequest,
  ): PayloadAction<WithRequest<Error, TRequest>> => {
    return {
      type: this.CLEAR_DATA,
      payload: { request },
    };
  };

  reducer = (
    state = new PaginatedCachedDataReducerState<TResponseMessage>(),
    action: Action,
  ): PaginatedCachedDataReducerState<TResponseMessage> => {
    if (isNil(action)) {
      return state;
    }

    switch (action.type) {
      case this.cachedDataReducer.REQUEST:
        // A request is in progress.
        state = clone(state);
        state.requestedAt = this.timeSource();
        state.inFlight = true;
        return state;
      case this.cachedDataReducer.RECEIVE: {
        // The results of a request have been received.
        const { request, data } = (
          action as PayloadAction<WithRequest<TResponseMessage, TRequest>>
        ).payload;
        const id = this.requestToID(request);
        state = clone(state);
        state.inFlight = true;
        state.data[id] = data;
        state.valid = false;
        state.setAt = this.timeSource();
        state.lastError = null;
        return state;
      }
      case this.RECEIVE_COMPLETED: {
        state = clone(state);
        state.inFlight = false;
        state.setAt = this.timeSource();
        state.valid = true;
        state.lastError = null;
        return state;
      }
      case this.CLEAR_DATA: {
        state = clone(state);
        state.data = {};
        state.inFlight = false;
        state.setAt = undefined;
        state.requestedAt = undefined;
        state.valid = false;
        return state;
      }
      case this.cachedDataReducer.ERROR: {
        // A request failed.
        const { payload: error } = action as PayloadAction<
          WithRequest<Error, TRequest>
        >;
        state = clone(state);
        state.inFlight = false;
        state.lastError = error.data;
        state.valid = false;
        if (isForbiddenRequestError(error.data)) {
          state.unauthorized = true;
        }
        return state;
      }
      case this.cachedDataReducer.INVALIDATE:
        // The data is invalidated.
        state = clone(state);
        state.valid = false;
        return state;
      default:
        return state;
    }
  };

  refresh = <S>(
    req?: TRequest,
    stateAccessor = (state: any, _: TRequest) =>
      state.cachedData[this.actionNamespace],
  ): ThunkAction<any, S, any, Action> => {
    return async (
      dispatch: ThunkDispatch<S, unknown, Action>,
      getState: () => S,
    ) => {
      // Override page_token and page_size fields to ensure that the exact same
      // pagination params are used across the entire app because all the results
      // are stored in the single store's slice. The current implementation doesn't
      // support requesting data with different page sizes or calls to an arbitrary page.
      req.page_token = "";
      req.page_size = this.pageLimit;

      const state: PaginatedCachedDataReducerState<TResponseMessage> =
        stateAccessor(getState(), req);
      if (
        state &&
        (state.inFlight ||
          (this.invalidationPeriod && state.valid) ||
          state.unauthorized)
      ) {
        return;
      }

      // clean up previously received data to ensure that fresh data won't be mixed with
      // previously invalidated data.
      dispatch(this.clearData(req));

      let hasMoreData = true;
      while (hasMoreData) {
        dispatch(this.cachedDataReducer.requestData(req));
        try {
          const resp = await this.apiEndpoint(req, this.requestTimeout);
          dispatch(this.cachedDataReducer.receiveData(resp, req));
          if (req.page_token === resp.next_page_token) {
            // there's no more data to request since next page token is the same as current one.
            hasMoreData = false;
            dispatch(this.receiveCompleted(req));
          } else {
            req.page_token = resp.next_page_token;
          }
        } catch (e) {
          const error = maybeError(e);
          // duplicate the same error handling as in base CachedDataReducer#refresh method.
          if ((error as Error).message === "Unauthorized") {
            // TODO(couchand): This is an unpleasant dependency snuck in here...
            const { location } = createHashHistory();
            if (
              location &&
              !location.pathname.startsWith("/login") &&
              !location.pathname.startsWith("/jwt")
            ) {
              dispatch(push(getLoginPage(location)));
            }
          }
          setTimeout(
            () => dispatch(this.cachedDataReducer.errorData(error, req)),
            1000,
          );
          break;
        } finally {
          // Invalidate data after the invalidation period if one exists.
          if (this.invalidationPeriod) {
            setTimeout(
              () => dispatch(this.cachedDataReducer.invalidateData(req)),
              this.invalidationPeriod.asMilliseconds(),
            );
          }
        }
      }
    };
  };

  /**
   * setTimeSource overrides the source of timestamps used by this component.
   * Intended for use in tests only.
   */
  setTimeSource(timeSource: { (): moment.Moment }) {
    this.timeSource = timeSource;
  }

  private timeSource: { (): moment.Moment } = () => moment();
}
