// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary } from "@cockroachlabs/cluster-ui";
import { useEffect, useMemo } from "react";

import { analytics } from "src/redux/analytics";
import { validateNodes, getVersions } from "src/redux/nodes";

/**
 * VersionSyncToAnalytics is a renderless component that syncs the
 * current node versions to the global AnalyticsSync instance via
 * setVersions(). This replaces the direct redux read that was
 * previously done in AnalyticsSync.identify().
 */
function VersionSyncToAnalytics(): null {
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();

  const versions = useMemo(() => {
    const validated = validateNodes(nodeStatuses, livenessByNodeID);
    return getVersions(validated);
  }, [nodeStatuses, livenessByNodeID]);

  useEffect(() => {
    if (analytics) {
      analytics.setVersions(versions);
    }
  }, [versions]);

  return null;
}

export default VersionSyncToAnalytics;
