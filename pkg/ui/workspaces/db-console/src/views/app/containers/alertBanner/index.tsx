// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useHealth } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React, { useState } from "react";
import { useDispatch, useSelector } from "react-redux";

import { AlertLevel, AlertInfo, bannerAlertsSelector } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";
import { AlertMessage } from "src/views/shared/components/alertMessage";

import "./alertbanner.scss";

interface DisplayAlert extends AlertInfo {
  dismiss(): void;
  showAsAlert?: boolean;
  autoClose?: boolean;
  closable?: boolean;
  autoCloseTimeout?: number;
}

/**
 * AlertBanner is a React element that displays active critical alerts as a
 * banner, intended for display across the top of the screen in a way that may
 * overlap content.
 */
function AlertBanner(): React.ReactElement {
  const { error: healthError } = useHealth();
  const [dismissedAt, setDismissedAt] = useState(() => moment(0));

  const reduxAlerts = useSelector((state: AdminUIState) =>
    bannerAlertsSelector(state),
  );
  const dispatch = useDispatch();

  // Build combined alerts list.
  const alerts: DisplayAlert[] = [];

  // Health disconnected alert (driven by SWR polling).
  if (healthError) {
    const dismissedMaxTime = moment().subtract(1, "m");
    if (!dismissedAt.isAfter(dismissedMaxTime)) {
      alerts.push({
        level: AlertLevel.CRITICAL,
        title:
          "We're currently having some trouble fetching updated data. " +
          "If this persists, it might be a good idea to check your " +
          "network connection to the CockroachDB cluster.",
        dismiss: () => setDismissedAt(moment()),
      });
    }
  }

  // Redux-based alerts.
  for (const alert of reduxAlerts) {
    const { dismiss, ...rest } = alert;
    alerts.push({
      ...rest,
      dismiss: () => dispatch(dismiss as any),
    });
  }

  if (alerts.length === 0) {
    return null;
  }

  // Display only the first visible component.
  const { dismiss, ...alertProps } = alerts[0];
  const AlertComponent = alertProps.showAsAlert ? AlertMessage : AlertBox;

  return (
    <div className="alert-banner">
      <AlertComponent dismiss={dismiss} {...alertProps} />
    </div>
  );
}

export default AlertBanner;
