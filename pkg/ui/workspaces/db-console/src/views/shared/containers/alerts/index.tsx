// Copyright 2018 The Cockroach Authors.
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
  panelAlertsSelector,
  computeStaggeredVersionWarning,
  staggeredVersionDismissedSetting,
} from "src/redux/alerts";
import { validateNodes, getNumNodesByVersionsTag } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";

function AlertSection(): React.ReactElement {
  const reduxAlerts = useSelector((state: AdminUIState) =>
    panelAlertsSelector(state),
  );
  const staggeredDismissed = useSelector(
    staggeredVersionDismissedSetting.selector,
  );
  const { nodeStatuses, livenessByNodeID } = useNodesSummary();
  const dispatch = useDispatch();

  const nodeAlerts = useMemo(() => {
    const validated = validateNodes(nodeStatuses, livenessByNodeID);
    const versionsTagMap = getNumNodesByVersionsTag(validated);
    const result: Alert[] = [];
    const staggered = computeStaggeredVersionWarning(
      versionsTagMap,
      staggeredDismissed,
    );
    if (staggered) {
      result.push(staggered);
    }
    return result;
  }, [nodeStatuses, livenessByNodeID, staggeredDismissed]);

  const alerts = [...reduxAlerts, ...nodeAlerts];

  return (
    <div>
      {map(alerts, (a, i) => {
        // Extract values we don't want.
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { dismiss, ...alertProps } = a;
        const boundDismiss = bindActionCreators(() => a.dismiss, dispatch);
        return <AlertBox key={i} dismiss={boundDismiss} {...alertProps} />;
      })}
    </div>
  );
}

export default AlertSection;
