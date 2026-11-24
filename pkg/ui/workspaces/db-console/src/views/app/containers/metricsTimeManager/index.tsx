// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React, { useCallback, useEffect, useRef } from "react";
import { useDispatch, useSelector } from "react-redux";

import { AdminUIState } from "src/redux/state";
import * as timewindow from "src/redux/timeScale";

interface MetricsTimeManagerProps {
  // Optional override method to obtain the current time. Used for tests.
  now?: () => moment.Moment;
}

/**
 * MetricsTimeManager takes responsibility for advancing the current global
 * time window used by metric graphs. It renders nothing, but will dispatch an
 * updated time window into the redux store whenever the previous time window is
 * expired.
 */
function MetricsTimeManager({
  now,
}: MetricsTimeManagerProps): React.ReactElement {
  const timeScale = useSelector((state: AdminUIState) => state.timeScale);
  const dispatch = useDispatch();
  const timeoutRef = useRef<number | null>(null);

  /**
   * setWindow dispatches a new time window, extending backwards from the
   * current time, by deriving the time from timeScale.
   */
  const setWindow = useCallback(
    (ts: timewindow.TimeScaleState) => {
      // The following two `if` blocks check two things that in theory should always be in sync.
      // They check if the time selection is a dynamic, moving, now.
      if (!ts.scale.fixedWindowEnd) {
        if (!ts.metricsTime.isFixedWindow) {
          const currentNow = now ? now() : moment();
          // Update the metrics page window with the new "now" time for polling.
          dispatch(
            timewindow.setMetricsMovingWindow({
              start: currentNow.clone().subtract(ts.scale.windowSize),
              end: currentNow,
            }),
          );
        }
      } else {
        const fixedWindowEnd = ts.scale.fixedWindowEnd;
        // Update the metrics page window with the fixed, custom end time.
        dispatch(
          timewindow.setMetricsMovingWindow({
            start: fixedWindowEnd.clone().subtract(ts.scale.windowSize),
            end: fixedWindowEnd,
          }),
        );
      }
    },
    [now, dispatch],
  );

  /**
   * checkWindow determines when the metrics current time window will expire. If it is
   * already expired, a new time window is dispatched immediately. Otherwise,
   * setTimeout is used to asynchronously set a new time window when the current
   * one expires.
   */
  const checkWindow = useCallback(
    (ts: timewindow.TimeScaleState) => {
      // Clear any existing timeout.
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }

      // If there is no current window, or if scale have changed since this
      // window was generated, set one immediately.
      if (
        !ts.metricsTime.currentWindow ||
        ts.metricsTime.shouldUpdateMetricsWindowFromScale
      ) {
        setWindow(ts);
        return;
      }

      // Fixed time ranges can't expire.
      if (ts.scale.fixedWindowEnd) {
        return;
      }

      const currentNow = now ? now() : moment();
      const currentEnd = ts.metricsTime.currentWindow.end;
      const expires = currentEnd.clone().add(ts.scale.windowValid);
      if (currentNow.isAfter(expires)) {
        // Current time window is expired, reset it.
        setWindow(ts);
      } else {
        // Set a timeout to reset the window when the current window expires.
        const newTimeout = window.setTimeout(
          () => setWindow(ts),
          expires.diff(currentNow).valueOf(),
        );
        timeoutRef.current = newTimeout;
      }
    },
    [now, setWindow],
  );

  // Run on mount and whenever timeScale changes.
  useEffect(() => {
    checkWindow(timeScale);
  }, [timeScale, checkWindow]);

  // Clean up timeout on unmount.
  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
        timeoutRef.current = null;
      }
    };
  }, []);

  // Render nothing.
  return null;
}

export default MetricsTimeManager;
