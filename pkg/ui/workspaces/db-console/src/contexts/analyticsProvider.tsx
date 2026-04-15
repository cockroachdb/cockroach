// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useCluster, useNodes } from "@cockroachlabs/cluster-ui";
import { Location } from "history";
import uniq from "lodash/uniq";
import React, { useEffect, useRef } from "react";

import { analytics, createAnalytics, AnalyticsSync } from "src/redux/analytics";
import { history } from "src/redux/history";

/**
 * AnalyticsProvider initializes the global analytics singleton and feeds it
 * cluster/version data from SWR hooks. It also wires up history-based page
 * tracking. This replaces the old initializeAnalytics() function that
 * depended on the Redux store.
 *
 * This component renders no UI — it only manages the analytics lifecycle.
 */
export const AnalyticsProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const { data: clusterData } = useCluster();
  const { nodeStatuses } = useNodes();
  const analyticsRef = useRef<AnalyticsSync | undefined>(analytics);

  // Initialize analytics singleton and set up history listener on mount.
  useEffect(() => {
    if (!analyticsRef.current) {
      analyticsRef.current = createAnalytics();
    }

    // Record the initial page that was accessed; the listener won't fire
    // for the first page loaded.
    analyticsRef.current.page(history.location);
    analyticsRef.current.identify();

    // Attach a listener to the history object which will track a 'page'
    // event whenever the user navigates to a new path.
    let lastPageLocation: Location = history.location;
    const unlisten = history.listen((location: Location) => {
      // Do not log if the pathname is the same as the previous.
      // Needed because history.listen() fires twice when using hash
      // history; this bug is "won't fix" in the version of history we
      // are using, and upgrading would imply a difficult upgrade to
      // react-router v4.
      // (https://github.com/ReactTraining/history/issues/427).
      if (lastPageLocation && lastPageLocation.pathname === location.pathname) {
        return;
      }
      lastPageLocation = location;
      analyticsRef.current.page(location);
      analyticsRef.current.identify();
    });

    return unlisten;
  }, []);

  // Feed cluster data into the analytics singleton when it becomes available.
  useEffect(() => {
    if (analyticsRef.current) {
      analyticsRef.current.updateCluster(clusterData ?? null);
      // Attempt identify in case versions were already available.
      analyticsRef.current.identify();
    }
  }, [clusterData]);

  // Derive and feed version tags from node statuses.
  useEffect(() => {
    if (analyticsRef.current && nodeStatuses) {
      const versions = uniq(
        nodeStatuses.map(ns => ns.build_info?.tag).filter(Boolean),
      );
      analyticsRef.current.updateVersions(versions);
      // Attempt identify in case cluster data was already available.
      analyticsRef.current.identify();
    }
  }, [nodeStatuses]);

  return <>{children}</>;
};
