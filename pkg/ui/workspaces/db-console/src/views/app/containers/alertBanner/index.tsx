// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { connect } from "react-redux";
import { Action, Dispatch, bindActionCreators } from "redux";

import { Alert, bannerAlertsSelector } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { AlertBox } from "src/views/shared/components/alertBox";
import { AlertMessage } from "src/views/shared/components/alertMessage";

import "./alertbanner.styl";

interface AlertBannerProps {
  /**
   * List of alerts to display in the alert banner.
   */
  alerts: Alert[];
  /**
   * Raw dispatch method for the current store, will be used to dispatch
   * alert dismissal callbacks.
   */
  dispatch: Dispatch<Action>;
}

/**
 * AlertBanner is a React element that displays active critical alerts as a
 * banner, intended for display across the top of the screen in a way that may
 * overlap content.
 */
class AlertBanner extends React.Component<AlertBannerProps, {}> {
  render() {
    const { alerts, dispatch } = this.props;
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
}

const alertBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      alerts: bannerAlertsSelector(state),
    };
  },
  dispatch => {
    return {
      dispatch,
    };
  },
)(AlertBanner);

export default alertBannerConnected;
