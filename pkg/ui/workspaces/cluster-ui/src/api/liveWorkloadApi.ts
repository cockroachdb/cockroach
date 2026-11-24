// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment, { Moment } from "moment-timezone";
import { useContext, useMemo, useState } from "react";

import { getActiveExecutionsWithLockWaits } from "src/activeExecutions/activeStatementUtils";
import { ActiveStatement, ActiveTransaction } from "src/activeExecutions/types";

import { ClusterDetailsContext } from "../contexts";
import { useSwrWithClusterId } from "../util";

import { ClusterLocksResponse, getClusterLocksState } from "./clusterLocksApi";
import { useSessions } from "./sessionsApi";
import { SqlApiResponse } from "./sqlApi";

const CLUSTER_LOCKS_SWR_KEY = "liveWorkload/clusterLocks";

interface UseLiveWorkloadOptions {
  // Polling interval in milliseconds.
  // Set to 0 or undefined to disable polling.
  refreshInterval?: number;
  // When true, uses cached data without revalidating. This is useful
  // for detail pages that display ephemeral data (e.g., active statements)
  // where a revalidation would likely return different results, causing
  // the viewed item to disappear.
  immutable?: boolean;
}

export interface LiveWorkloadData {
  statements: ActiveStatement[];
  transactions: ActiveTransaction[];
  clusterLocks: ClusterLocksResponse | null;
  internalAppNamePrefix: string | null;
  maxSizeApiReached: boolean;
}

export interface UseLiveWorkloadResult {
  data: LiveWorkloadData;
  isLoading: boolean;
  error: Error | null;
  lastUpdated: Moment | null;
  refresh: () => void;
}

export const useLiveWorkload = (
  opts?: UseLiveWorkloadOptions,
): UseLiveWorkloadResult => {
  const { isTenant } = useContext(ClusterDetailsContext);
  const [lastUpdated, setLastUpdated] = useState<Moment | null>(null);

  // When immutable is true, disable all automatic revalidation so the
  // hook only uses cached data (or fetches once if no cache exists).
  const immutableConfig = opts?.immutable
    ? {
        revalidateIfStale: false,
        revalidateOnFocus: false,
        revalidateOnReconnect: false,
      }
    : {};

  const {
    data: sessionsData,
    error: sessionsError,
    isLoading: sessionsLoading,
    refresh: refreshSessions,
  } = useSessions({
    excludeClosedSessions: true,
    refreshInterval: opts?.refreshInterval,
    immutable: opts?.immutable,
    onSuccess: () => setLastUpdated(moment.utc()),
  });

  const {
    data: clusterLocksData,
    error: clusterLocksError,
    mutate: mutateClusterLocks,
  } = useSwrWithClusterId<SqlApiResponse<ClusterLocksResponse>>(
    CLUSTER_LOCKS_SWR_KEY,
    !isTenant ? getClusterLocksState : null,
    {
      revalidateOnFocus: false,
      dedupingInterval: 5000,
      refreshInterval: opts?.refreshInterval,
      ...immutableConfig,
    },
  );

  const activeExecutions = useMemo(
    () =>
      getActiveExecutionsWithLockWaits(
        sessionsData ?? null,
        clusterLocksData?.results ?? null,
      ),
    [sessionsData, clusterLocksData],
  );

  const data: LiveWorkloadData = useMemo(
    () => ({
      statements: activeExecutions.statements,
      transactions: activeExecutions.transactions,
      clusterLocks: clusterLocksData?.results ?? null,
      internalAppNamePrefix: sessionsData?.internal_app_name_prefix ?? null,
      maxSizeApiReached: !!clusterLocksData?.maxSizeReached,
    }),
    [activeExecutions, clusterLocksData, sessionsData],
  );

  const refresh = () => {
    refreshSessions();
    mutateClusterLocks();
  };

  return {
    data,
    isLoading: sessionsLoading,
    error: sessionsError ?? clusterLocksError ?? null,
    lastUpdated,
    refresh,
  };
};
