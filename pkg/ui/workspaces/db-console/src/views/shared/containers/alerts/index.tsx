// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import map from "lodash/map";
import React from "react";
import { useSelector, useDispatch } from "react-redux";
import { bindActionCreators } from "redux";

import { panelAlertsSelector } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";

function AlertSection(): React.ReactElement {
  const alerts = useSelector((state: AdminUIState) =>
    panelAlertsSelector(state),
  );
  const dispatch = useDispatch();

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
