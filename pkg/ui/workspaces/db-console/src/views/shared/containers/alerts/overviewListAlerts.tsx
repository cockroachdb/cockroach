// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useNodesSummary } from "@cockroachlabs/cluster-ui";
import map from "lodash/map";
import React, { useMemo } from "react";
import { useSelector, useDispatch } from "react-redux";
import { bindActionCreators } from "redux";

import {
  Alert,
  overviewListAlertsSelector,
  computeStaggeredVersionWarning,
  computeUpgradeNotFinalizedWarning,
  staggeredVersionDismissedSetting,
  upgradeNotFinalizedDismissedSetting,
} from "src/redux/alerts";
import {
  selectClusterSettings,
  selectClusterSettingVersion,
} from "src/redux/clusterSettings";
import {
  validateNodes,
  getNumNodesByVersionsTag,
  getNumNodesByVersions,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";

function OverviewAlertListSection(): React.ReactElement {
  const reduxAlerts = useSelector((state: AdminUIState) =>
    overviewListAlertsSelector(state),
  );
  const staggeredDismissed = useSelector(
    staggeredVersionDismissedSetting.selector,
  );
  const upgradeNotFinalizedDismissed = useSelector(
    upgradeNotFinalizedDismissedSetting.selector,
  );
  const settings = useSelector(selectClusterSettings);
  const clusterVersion = useSelector(selectClusterSettingVersion);
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();
  const dispatch = useDispatch();

  const nodeAlerts = useMemo(() => {
    const validated = validateNodes(nodeStatuses, livenessByNodeID);
    const versionsTagMap = getNumNodesByVersionsTag(validated);
    const versionsMap = getNumNodesByVersions(validated);
    const result: Alert[] = [];
    const staggered = computeStaggeredVersionWarning(
      versionsTagMap,
      staggeredDismissed,
    );
    if (staggered) {
      result.push(staggered);
    }
    const upgradeNotFinalized = computeUpgradeNotFinalizedWarning(
      settings,
      versionsMap,
      clusterVersion,
      upgradeNotFinalizedDismissed,
    );
    if (upgradeNotFinalized) {
      result.push(upgradeNotFinalized);
    }
    return result;
  }, [
    nodeStatuses,
    livenessByNodeID,
    staggeredDismissed,
    upgradeNotFinalizedDismissed,
    settings,
    clusterVersion,
  ]);

  const alerts = [...reduxAlerts, ...nodeAlerts];

  if (alerts.length === 0) {
    return null;
  }

  return (
    <section className="section">
      {map(alerts, (a, i) => {
        // Extract values we don't want.
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { dismiss, ...alertProps } = a;
        const boundDismiss = bindActionCreators(() => a.dismiss, dispatch);
        return <AlertBox key={i} dismiss={boundDismiss} {...alertProps} />;
      })}
    </section>
  );
}

export default OverviewAlertListSection;
