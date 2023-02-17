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
 * useFetchDataWithPolling allows the setup of making data requests with optional polling.
 * Data will automatically be fetched when the data is not valid or was never updated.
 * Requests will be made at most once per polling interval.
 * @param callbackFn The call back function to be called at the provided interval.
 * @param pollingIntervalMs interval in ms to fetch data
 * @param isDataValid whether the current dasta is valid, if the data is not valid we fetch immediately
 * @param shouldPoll whether we should setup polling
 * @param lastUpdated the time the data was last updated
 * @returns a function to stop polling
 */
export const useFetchDataWithPolling = (
  callbackFn: () => void,
  isDataValid: boolean,
  lastUpdated: moment.Moment,
  shouldPoll: boolean,
  pollingIntervalMs: number | null,
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

    if (!isDataValid || !lastUpdated) {
      // At most we will make all reqs managed by this hook once every MIN_REQUEST_DELAY_MS.
      nextRefresh = lastReqMade.current
        ? lastReqMade.current.clone().add(MIN_REQUEST_DELAY_MS, "milliseconds")
        : now;
    } else if (lastUpdated && pollingIntervalMs) {
      nextRefresh = lastUpdated.clone().add(pollingIntervalMs, "milliseconds");
    } else {
      return;
    }

    if (shouldPoll) {
      refreshDataTimeout.current = setTimeout(() => {
        lastReqMade.current = now;
        callbackFn();
      }, Math.max(0, nextRefresh.diff(now, "millisecond")));
    }

    return clearRefreshDataTimeout;
  }, [
    callbackFn,
    clearRefreshDataTimeout,
    isDataValid,
    lastUpdated,
    shouldPoll,
    pollingIntervalMs,
  ]);

  return clearRefreshDataTimeout;
};
