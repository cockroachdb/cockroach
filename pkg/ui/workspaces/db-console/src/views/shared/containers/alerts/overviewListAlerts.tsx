// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary, useClusterSettings } from "@cockroachlabs/cluster-ui";
import without from "lodash/without";
import React, { useMemo } from "react";
import { useSelector, useStore } from "react-redux";

import {
  Alert,
  getStaggeredVersionWarning,
  getClusterPreserveDowngradeOptionOvertime,
  getUpgradeNotFinalizedWarning,
  staggeredVersionDismissedSetting,
  clusterPreserveDowngradeOptionDismissedSetting,
  upgradeNotFinalizedDismissedSetting,
} from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import {
  getValidatedNodes,
  getNumNodesByVersionsTag,
  getNumNodesByVersions,
} from "src/redux/nodes";
import { AlertBox } from "src/views/shared/components/alertBox";

function OverviewAlertListSection(): React.ReactElement {
  const store = useStore<AdminUIState>();

  // SWR-based data.
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();
  const { settingValues } = useClusterSettings();

  // Redux-only selectors (localSettings — no cachedData).
  const versionMismatchDismissed = useSelector(
    staggeredVersionDismissedSetting.selector,
  );
  const preserveDowngradeDismissed = useSelector(
    clusterPreserveDowngradeOptionDismissedSetting.selector,
  );
  const upgradeNotFinalizedDismissed = useSelector(
    upgradeNotFinalizedDismissedSetting.selector,
  );

  const alerts: Alert[] = useMemo(() => {
    const validNodes = getValidatedNodes(nodeStatuses, livenessByNodeID);
    const versionsTagMap = getNumNodesByVersionsTag(validNodes);
    const versionsMap = getNumNodesByVersions(validNodes);
    const clusterVersion = settingValues?.["version"]?.value ?? "";

    return without(
      [
        getStaggeredVersionWarning(versionsTagMap, versionMismatchDismissed),
        getClusterPreserveDowngradeOptionOvertime(
          settingValues,
          preserveDowngradeDismissed,
        ),
        getUpgradeNotFinalizedWarning(
          settingValues,
          versionsMap,
          clusterVersion,
          upgradeNotFinalizedDismissed,
        ),
      ],
      null,
      undefined,
    );
  }, [
    nodeStatuses,
    livenessByNodeID,
    settingValues,
    versionMismatchDismissed,
    preserveDowngradeDismissed,
    upgradeNotFinalizedDismissed,
  ]);

  if (alerts.length === 0) {
    return null;
  }

  return (
    <section className="section">
      {alerts.map((a, i) => {
        const { dismiss, ...alertProps } = a;
        const boundDismiss = () => dismiss(store.dispatch, store.getState);
        return <AlertBox key={i} dismiss={boundDismiss} {...alertProps} />;
      })}
    </section>
  );
}

export default OverviewAlertListSection;
