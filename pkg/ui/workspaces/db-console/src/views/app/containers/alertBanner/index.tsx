// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useDispatch, useSelector } from "react-redux";
import { bindActionCreators } from "redux";

import { bannerAlertsSelector } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";
import { AlertMessage } from "src/views/shared/components/alertMessage";

import "./alertbanner.scss";

/**
 * AlertBanner is a React element that displays active critical alerts as a
 * banner, intended for display across the top of the screen in a way that may
 * overlap content.
 */
function AlertBanner(): React.ReactElement {
  const alerts = useSelector((state: AdminUIState) =>
    bannerAlertsSelector(state),
  );
  const dispatch = useDispatch();

  if (!alerts || alerts.length === 0) {
    return null;
  }

  // Display only the first visible component.
  const { dismiss, ...alertProps } = alerts[0];
  const boundDismiss = bindActionCreators(() => dismiss, dispatch);
  const AlertComponent = alertProps.showAsAlert ? AlertMessage : AlertBox;

  return (
    <div className="alert-banner">
      <AlertComponent dismiss={boundDismiss} {...alertProps} />
    </div>
  );
}

export default AlertBanner;
