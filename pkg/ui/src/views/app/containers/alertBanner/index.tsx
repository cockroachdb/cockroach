// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import React from "react";
import { Dispatch, bindActionCreators } from "redux";
import { connect } from "react-redux";

import "./alertbanner.styl";

import { AlertBox } from "src/views/shared/components/alertBox";
import { Alert, bannerAlertsSelector } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";

interface AlertBannerProps {
  /**
   * List of alerts to display in the alert banner.
   */
  alerts: Alert[];
  /**
   * Raw dispatch method for the current store, will be used to dispatch
   * alert dismissal callbacks.
   */
  dispatch: Dispatch<AdminUIState>;
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
    return  <div className="alert-banner">
      <AlertBox dismiss={boundDismiss} {...alertProps} />
    </div>;
  }
}

const alertBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      alerts: bannerAlertsSelector(state),
    };
  },
  (dispatch) => {
    return {
      dispatch,
    };
  },
)(AlertBanner);

export default alertBannerConnected;
