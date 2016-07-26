/**
 * This module maintains the state of CockroachDB time series queries needed by
 * the web application. Cached query data is maintained separately for
 * individual components (e.g. different graphs); components are distinguished
 * in the reducer by a unique ID.
 */

import _ = require("lodash");
import { Dispatch } from "redux";

import * as protos from  "../js/protos";
import { Action, PayloadAction } from "../interfaces/action";
import { queryTimeSeries } from "../util/api";

type TSRequestMessage = cockroach.ts.tspb.TimeSeriesQueryRequestMessage;
type TSResponseMessage = cockroach.ts.tspb.TimeSeriesQueryResponseMessage;

export const REQUEST = "cockroachui/metrics/REQUEST";
export const RECEIVE = "cockroachui/metrics/RECEIVE";
export const ERROR = "cockroachui/metrics/ERROR";
export const FETCH = "cockroachui/metrics/FETCH";
export const FETCH_COMPLETE = "cockroachui/metrics/FETCH_COMPLETE";

/**
 * WithID is a convenient interface for associating arbitrary data structures
 * with a component ID.
 */
interface WithID<T> {
  id: string;
  data: T;
}

/**
 * A request/response pair.
 */
interface RequestWithResponse {
  request: TSRequestMessage;
  response: TSResponseMessage;
}

/**
 * MetricsQuery maintains the cached data for a single component.
 */
export class MetricsQuery {
  // ID of the component which owns this data.
  id: string;
  // The currently cached response data for this component.
  data: TSResponseMessage;
  // If the immediately previous request attempt returned an error, rather than
  // a response, it is maintained here. Null if the previous request was
  // successful.
  error: Error;
  // The previous request, which will have resulted in either "data" or "error"
  // being populated.
  request: TSRequestMessage;
  // A possibly outstanding request used to retrieve data from the server for this
  // component. This may represent a currently in-flight query, and thus is not
  // necessarily the request used to retrieve the current value of "data".
  nextRequest: TSRequestMessage;

  constructor(id: string) {
    this.id = id;
  }
}

/**
 * metricsQueryReducer is a reducer which modifies the state of a single
 * MetricsQuery object.
 */
function metricsQueryReducer(state: MetricsQuery, action: Action) {
  switch (action.type) {
    // This component has requested a new set of metrics from the server.
    case REQUEST:
      let { payload: request } = action as PayloadAction<WithID<TSRequestMessage>>;
      state = _.clone(state);
      state.nextRequest = request.data;
      return state;

    // Results for a previous request have been received from the server.
    case RECEIVE:
      let { payload: response } = action as PayloadAction<WithID<RequestWithResponse>>;
      state = _.clone(state);
      state.data = response.data.response;
      state.request = response.data.request;
      state.error = undefined;
      return state;

    // The previous query for metrics for this component encountered an error.
    case ERROR:
      let { payload: error } = action as PayloadAction<WithID<Error>>;
      state = _.clone(state);
      state.error = error.data;
      return state;

    default:
      return state;
  }
}

/**
 * MetricsQueries is a collection of individual MetricsQuery objects, indexed by
 * component id.
 */
interface MetricQuerySet {
  [id: string]: MetricsQuery;
}

/**
 * metricsQueriesReducer dispatches actions to the correct MetricsQuery, based
 * on the ID of the actions.
 */
export function metricQuerySetReducer(state: MetricQuerySet = {}, action: Action) {
  switch (action.type) {
    case REQUEST:
    case RECEIVE:
    case ERROR:
      // All of these requests should be dispatched to a MetricQuery in the
      // collection. If a MetricQuery with that ID does not yet exist, create it.
      let { id } = (action as PayloadAction<WithID<any>>).payload;
      state = _.clone(state);
      state[id] = metricsQueryReducer(state[id] || new MetricsQuery(id), action);
      return state;

    default:
      return state;
  }
}

/**
 * MetricQueryState maintains a MetricQuerySet collection, along with some
 * metadata relevant to server queries.
 */
export class MetricQueryState {
  // A count of the number of in-flight fetch requests.
  inFlight = 0;
  // The collection of MetricQuery objects.
  queries: MetricQuerySet;
}

/**
 * The metrics reducer accepts events for individual MetricQuery objects,
 * dispatching them based on ID. It also accepts actions which indicate the
 * state of the connection to the server.
 */
export default function reducer(state: MetricQueryState = new MetricQueryState(), action: Action): MetricQueryState {
  switch (action.type) {
    // A new fetch request to the server is now in flight.
    case FETCH:
      state = _.clone(state);
      state.inFlight += 1;
      return state;

    // A fetch request to the server has completed.
    case FETCH_COMPLETE:
      state = _.clone(state);
      state.inFlight -= 1;
      return state;

    // Other actions may be handled by the metricsQueryReducer.
    default:
      state = _.clone(state);
      state.queries = metricQuerySetReducer(state.queries, action);
      return state;
  }
}

/**
 * requestMetrics indicates that a component is requesting new data from the
 * server.
 */
export function requestMetrics(id: string, request: TSRequestMessage): PayloadAction<WithID<TSRequestMessage>> {
  return {
    type: REQUEST,
    payload: {
      id: id,
      data: request,
    },
  };
}

/**
 * receiveMetrics indicates that a previous request from this component has been
 * fulfilled by the server.
 */
export function receiveMetrics(id: string, request: TSRequestMessage,
                               response: TSResponseMessage): PayloadAction<WithID<RequestWithResponse>> {
  return {
    type: RECEIVE,
    payload: {
      id: id,
      data: {
        request: request,
        response: response,
      },
    },
  };
}

/**
 * errorMetrics indicates that a pervious request from this component could not
 * be fulfilled due to an error.
 */
export function errorMetrics(id: string, error: Error): PayloadAction<WithID<Error>> {
  return {
    type: ERROR,
    payload: {
      id: id,
      data: error,
    },
  };
}

/**
 * fetchMetrics indicates that a new asynchronous request to the server is in-flight.
 */
export function fetchMetrics(): Action {
  return {
    type: FETCH,
  };
}

/**
 * fetchMetricsComplete indicates that an in-flight request to the server has
 * completed.
 */
export function fetchMetricsComplete(): Action {
  return {
    type: FETCH_COMPLETE,
  };
}

/**
 * queuedRequests is a list of requests that should be asynchronously sent to
 * the server. As a purely asynchronous concept, this lives outside of the redux
 * store.
 */
let queuedRequests: WithID<TSRequestMessage>[] = [];

/**
 * queuePromise is a promise that will be resolved when the current batch of
 * queued requests has been been processed. This is returned by the queryMetrics
 * thunk method, for use in synchronizing in tests.
 */
let queuePromise: Promise<void> = null;

/**
 * queryMetrics action allows components to asynchronously request new metrics
 * from the server. Components provide their id and a request object for new
 * data.
 *
 * Requests to queryMetrics may be batched when dispatching to the server;
 * specifically, queries which have the same time span can be handled by the
 * server in a single call.
 */
export function queryMetrics<S>(id: string, query: TSRequestMessage) {
  return (dispatch: Dispatch<S>): Promise<void> => {
    // Indicate that this request has been received and queued.
    dispatch(requestMetrics(id, query));
    queuedRequests.push({
      id: id,
      data: query,
    });

    // Only the first queued request is responsible for initiating the fetch
    // process. The fetch process is "debounced" with a setTimeout, and thus
    // multiple queryMetrics actions can be batched into a single fetch from the
    // server.
    if (queuedRequests.length > 1) {
      // Return queuePromise, created by an earlier queueMetrics request.
      return queuePromise;
    }

    queuePromise = new Promise<void>((resolve, reject) => {
      setTimeout(() => {
        // Increment in-flight counter.
        dispatch(fetchMetrics());

        // Construct queryable batches from the set of queued queries. Queries can
        // be dispatched in a batch if they are querying over the same timespan.
        let batches = _.groupBy(queuedRequests, (qr) => timespanKey(qr.data));
        queuedRequests = [];

        // Fetch data from the server for each batch.
        let promises = _.map(batches, (batch) => {
          // Flatten the individual queries from all requests in the batch into a
          // single request.
          // Lodash operations are split into two methods because the lodash
          // typescript definition loses type information when chaining into
          // flatten.
          let unifiedRequest = _.clone(batch[0].data);
          let toFlatten = _.map(batch, (req) => req.data.queries);
          unifiedRequest.queries = _.flatten(toFlatten);

          return queryTimeSeries(unifiedRequest).then((response) => {
            // The number of results should match the queries exactly, and should
            // be in the exact order passed.
            if (response.results.length !== unifiedRequest.queries.length) {
              throw `mismatched count of results (${response.results.length}) and queries (${unifiedRequest.queries.length})`;
            }

            let results = response.results;

            // Match each result in the response to its corresponding original
            // query. Each request may have sent multiple queries in the batch.
            _.each(batch, (request) => {
              let numQueries = request.data.queries.length;
              dispatch(receiveMetrics(request.id, request.data, new protos.cockroach.ts.tspb.TimeSeriesQueryResponse({
                results: results.splice(0, numQueries),
              })));
            });
          }).catch((e: Error) => {
            // Dispatch the error to each individual MetricsQuery which was
            // requesting data.
            _.each(batch, (request) => {
              dispatch(errorMetrics(request.id, e));
            });
          });
        });

        // Wait for all promises to complete, then decrement in-flight counter
        // and resolve queuePromise.
        resolve(Promise.all(promises).then(() => {
          dispatch(fetchMetricsComplete());
        }));
      });
    });

    return queuePromise;
  };
}

function timespanKey(query: TSRequestMessage): string {
  return query.start_nanos.toString() + ":" + query.end_nanos.toString();
}
