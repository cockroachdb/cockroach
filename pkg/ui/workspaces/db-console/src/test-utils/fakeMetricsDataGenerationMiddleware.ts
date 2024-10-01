// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import clone from "lodash/clone";
import Long from "long";
import { Store, Action, Dispatch } from "redux";

import { PayloadAction } from "src/interfaces/action";
import { cockroach } from "src/js/protos";
import { RECEIVE, RequestWithResponse, WithID } from "src/redux/metrics";
import { AdminUIState } from "src/redux/state";

import ITimeSeriesDatapoint = cockroach.ts.tspb.ITimeSeriesDatapoint;

function fakeTimeSeriesDatapoint(timestamp?: Long): ITimeSeriesDatapoint {
  return {
    timestamp_nanos: timestamp || Long.fromNumber(Date.now() * 1000000),
    value: Math.ceil(Math.random() * 100),
  };
}

/**
 * @summary Redux Middleware which intercepts RESPONSE actions with requested metrics data
 * and populates datapoints for entire requested date period.
 * @example Display datapoints for 2 months period even if cluster was created today and
 * there is no available data for requested period, then missing datapoints will be randomly generated.
 * Note: it is only for testing purposes only.
 */
export const fakeMetricsDataGenerationMiddleware =
  (_store: Store<AdminUIState>) =>
  (next: Dispatch<Action>) =>
  (action: Action) => {
    if (action.type === RECEIVE) {
      const originalAction = action as PayloadAction<
        WithID<RequestWithResponse>
      >;
      const { start_nanos, end_nanos, sample_nanos } =
        originalAction.payload.data.request;
      const { results } = originalAction.payload.data.response;
      const expectedDatapointsCount = end_nanos
        .sub(start_nanos)
        .divide(sample_nanos)
        .toNumber();

      const nextResults = results.map(res => {
        const actualDatapointsCount = res.datapoints.length;

        if (actualDatapointsCount >= expectedDatapointsCount) {
          return res;
        }

        const samplePoint =
          actualDatapointsCount > 0
            ? clone(res.datapoints[0])
            : fakeTimeSeriesDatapoint(end_nanos);

        const datapoints = Array(
          expectedDatapointsCount - actualDatapointsCount,
        )
          .fill(1)
          .map<ITimeSeriesDatapoint>((_, idx) => ({
            ...samplePoint,
            timestamp_nanos: samplePoint.timestamp_nanos.subtract(
              sample_nanos.multiply(idx + 1),
            ),
          }))
          .reverse()
          .concat(...res.datapoints);

        return {
          ...res,
          datapoints,
        };
      });

      originalAction.payload.data.response.results = nextResults;
      return next(originalAction);
    } else {
      return next(action);
    }
  };
