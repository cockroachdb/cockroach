// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { useEffect, useCallback, useRef } from "react";
import moment from "moment/moment";

export const usePrevious = <T>(value: T): T | undefined => {
  const ref = useRef<T>();
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
};

const MIN_REQUEST_DELAY_MS = 1000;

/**
 * useScheduleFunction allows the setup of scheduling a provided callback.
 * The callback function will be rescheduled any time one of the provided parameters
 * changes according to the pollingInterval, data validity and last updated time.
 * It will be scheduled as follows:
 * 1. Call immediately if 'scheduleNow' is true, or this is the first call (lastCompleted is null).
 * 2. Otherwise, schedule the function to run based on the lastCompleted time and scheduleInterval
 *    provided.
 *
 * @param callbackFn The call back function to be called at the provided interval.
 * @param scheduleIntervalMs scheduling interval in millis
 * @param scheduleNow schedule function immediately (adding a minimum delay of MIN_REQUEST_DELAY_MS
 *                    from last request)
 * @param shouldReschedule reschedule the function
 * @param lastCompleted the time the function was last completed
 * @returns a function to stop polling
 */
export const useScheduleFunction = (
  callbackFn: () => void,
  scheduleNow: boolean,
  lastCompleted: moment.Moment,
  shouldReschedule: boolean,
  scheduleIntervalMs: number | null,
): (() => void) => {
  const lastReqMade = useRef<moment.Moment>(null);
  const refreshDataTimeout = useRef<NodeJS.Timeout>(null);

  const clearRefreshDataTimeout = useCallback(() => {
    if (refreshDataTimeout.current != null) {
      clearTimeout(refreshDataTimeout.current);
    }
  }, []);

  useEffect(() => {
    const now = moment();
    let nextRefresh = null;

    if (scheduleNow || !lastCompleted) {
      // At most we will make all reqs managed by this hook once every MIN_REQUEST_DELAY_MS.
      nextRefresh = lastReqMade.current
        ? lastReqMade.current.clone().add(MIN_REQUEST_DELAY_MS, "milliseconds")
        : now;
    } else if (shouldReschedule && lastCompleted && scheduleIntervalMs >= 0) {
      nextRefresh = lastCompleted
        .clone()
        .add(scheduleIntervalMs, "milliseconds");
    } else {
      // Invalid parameters, or we don't need to refresh this data (reschedule = false).
      return clearRefreshDataTimeout;
    }

    refreshDataTimeout.current = setTimeout(() => {
      lastReqMade.current = now;
      callbackFn();
    }, Math.max(0, nextRefresh.diff(now, "millisecond")));

    return clearRefreshDataTimeout;
  }, [
    callbackFn,
    clearRefreshDataTimeout,
    scheduleNow,
    lastCompleted,
    shouldReschedule,
    scheduleIntervalMs,
  ]);

  return clearRefreshDataTimeout;
};
