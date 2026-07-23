// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useCluster, useNodesSummary } from "@cockroachlabs/cluster-ui";
import has from "lodash/has";
import uniq from "lodash/uniq";
import without from "lodash/without";
import moment from "moment-timezone";
import React, { useMemo } from "react";
import { useSelector, useStore } from "react-redux";

import { useVersionCheck } from "src/hooks/useVersionCheck";
import {
  Alert,
  getNewVersionNotification,
  getStaggeredVersionWarning,
  newVersionDismissedLocalSetting,
  staggeredVersionDismissedSetting,
} from "src/redux/alerts";
import { getValidatedNodes, getNumNodesByVersionsTag } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { VERSION_DISMISSED_KEY } from "src/redux/uiData";
import { AlertBox } from "src/views/shared/components/alertBox";

function AlertSection(): React.ReactElement {
  const store = useStore<AdminUIState>();

  const { data: clusterData } = useCluster();
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();

  // Compute single version from node data for version check.
  const currentVersion = useMemo(() => {
    if (!nodeStatuses?.length) return undefined;
    const builds = uniq(nodeStatuses.map(ns => ns.build_info?.tag));
    return builds.length === 1 ? builds[0] : undefined;
  }, [nodeStatuses]);

  const { newerVersions } = useVersionCheck(
    clusterData?.cluster_id,
    currentVersion,
  );

  const versionMismatchDismissed = useSelector(
    staggeredVersionDismissedSetting.selector,
  );
  const newVersionDismissedPersistentLoaded = useSelector(
    (state: AdminUIState) =>
      state.uiData && has(state.uiData, VERSION_DISMISSED_KEY),
  );
  const newVersionDismissedPersistent = useSelector((state: AdminUIState) => {
    const uiData = state.uiData;
    return (
      (uiData &&
        uiData[VERSION_DISMISSED_KEY] &&
        uiData[VERSION_DISMISSED_KEY].data &&
        moment(uiData[VERSION_DISMISSED_KEY].data)) ||
      moment(0)
    );
  });
  const newVersionDismissedLocal = useSelector(
    newVersionDismissedLocalSetting.selector,
  );

  const alerts: Alert[] = useMemo(() => {
    const validNodes = getValidatedNodes(nodeStatuses, livenessByNodeID);
    const versionsMap = getNumNodesByVersionsTag(validNodes);

    return without(
      [
        getNewVersionNotification(
          newerVersions,
          newVersionDismissedPersistentLoaded,
          newVersionDismissedPersistent,
          newVersionDismissedLocal,
        ),
        getStaggeredVersionWarning(versionsMap, versionMismatchDismissed),
      ],
      null,
      undefined,
    );
  }, [
    nodeStatuses,
    livenessByNodeID,
    versionMismatchDismissed,
    newerVersions,
    newVersionDismissedPersistentLoaded,
    newVersionDismissedPersistent,
    newVersionDismissedLocal,
  ]);

  return (
    <div>
      {alerts.map((a, i) => {
        const { dismiss, ...alertProps } = a;
        const boundDismiss = () => dismiss(store.dispatch, store.getState);
        return <AlertBox key={i} dismiss={boundDismiss} {...alertProps} />;
      })}
    </div>
  );
}

export default AlertSection;
