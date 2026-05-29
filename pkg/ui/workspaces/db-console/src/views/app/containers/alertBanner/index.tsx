// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useSelector, useStore } from "react-redux";

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
  const store = useStore<AdminUIState>();

  if (!alerts || alerts.length === 0) {
    return null;
  }

  // Display only the first visible component.
  const { dismiss, ...alertProps } = alerts[0];
  // Alert dismiss handlers are written as redux-thunk style
  // (dispatch, getState) functions; invoke them directly against the store
  // since redux-thunk is no longer wired in.
  const boundDismiss = () => dismiss(store.dispatch, store.getState);
  const AlertComponent = alertProps.showAsAlert ? AlertMessage : AlertBox;

  return (
    <div className="alert-banner">
      <AlertComponent dismiss={boundDismiss} {...alertProps} />
    </div>
  );
}

export default AlertBanner;
