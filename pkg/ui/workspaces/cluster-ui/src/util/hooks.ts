// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useEffect, useCallback, useRef } from "react";
import moment from "moment/moment";

export const usePrevious = <T>(value: T): T | undefined => {
  const ref = useRef<T>();
  useEffect(() => {
    ref.current = value;
  }, [value]);
  return ref.current;
};

const MIN_REQUEST_DELAY_MS = 500;

type VoidFunction = () => void;

/**
 * useScheduleFunction allows the scheduling of a function call at an interval.
 * It will be scheduled as follows:
 * 1. Call immediately if
 *        - `scheduleNow` callback is used
 *        - last completed time is not set
 * 2. Otherwise, reschedule the function if shouldReschedule is true  based on the
 * last completed time and the scheduleInterval provided.
 *
 * @param callbackFn The call back function to be called at the provided interval.
 * @param scheduleIntervalMs scheduling interval in millis
 * @param shouldReschedule whether we should continue to reschedule the function after completion
 * @param lastCompleted the time the function was last completed
 * @returns a tuple containing a function to schedule the function immediately (clearing the prev schedule)
 * and a function to clear the schedule
 */
export const useScheduleFunction = (
  callbackFn: () => void,
  shouldReschedule: boolean,
  scheduleIntervalMs: number | null,
  lastCompleted: moment.Moment | null,
): [VoidFunction, VoidFunction] => {
  const lastReqMade = useRef<moment.Moment>(null);
  const refreshDataTimeout = useRef<NodeJS.Timeout>(null);

  // useRef so we don't have to include this in our dep array.
  const clearSchedule = useCallback(() => {
    if (refreshDataTimeout.current != null) {
      clearTimeout(refreshDataTimeout.current);
    }
  }, []);

  const schedule = useCallback(
    (scheduleNow = false) => {
      const now = moment.utc();
      let nextRefresh: moment.Moment;
      if (scheduleNow) {
        nextRefresh =
          lastReqMade.current
            ?.clone()
            .add(MIN_REQUEST_DELAY_MS, "milliseconds") ?? now;
      } else if (shouldReschedule && scheduleIntervalMs) {
        nextRefresh = (lastCompleted ?? now)
          .clone()
          .add(scheduleIntervalMs, "milliseconds");
      } else {
        // Either we don't need to schedule the function again or we have
        // invalid params to the hook.
        return;
      }

      const timeoutMs = Math.max(0, nextRefresh.diff(now, "millisecond"));
      refreshDataTimeout.current = setTimeout(() => {
        lastReqMade.current = moment.utc();
        // TODO (xinhaoz) if we can swap to using the fetch API more directly here
        // we can abort the api call on refreshes.
        callbackFn();
      }, timeoutMs);
    },
    [shouldReschedule, scheduleIntervalMs, lastCompleted, callbackFn],
  );

  useEffect(() => {
    if (!lastCompleted) schedule(true);
    else schedule();

    return clearSchedule;
  }, [lastCompleted, schedule, clearSchedule]);

  const scheduleNow = useCallback(() => {
    clearSchedule();
    schedule(true);
  }, [schedule, clearSchedule]);

  return [scheduleNow, clearSchedule];
};
