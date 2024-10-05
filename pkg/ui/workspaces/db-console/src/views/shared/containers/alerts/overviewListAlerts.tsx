// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import _ from "lodash";
import { Dispatch, Action, bindActionCreators } from "redux";
import { connect } from "react-redux";

import { AlertBox } from "src/views/shared/components/alertBox";
import { AdminUIState } from "src/redux/state";
import { Alert, overviewListAlertsSelector } from "src/redux/alerts";

interface AlertSectionProps {
  /**
   * List of alerts to display in the alert section.
   */
  alerts: Alert[];
  /**
   * Raw dispatch method for the current store, will be used to dispatch
   * alert dismissal callbacks.
   */
  dispatch: Dispatch<Action>;
}

class OverviewAlertListSection extends React.Component<AlertSectionProps, {}> {
  render() {
    const { alerts, dispatch } = this.props;
    if (alerts.length === 0) {
      return null;
    }
    return (
      <section className="section">
        {_.map(alerts, (a, i) => {
          // Extract values we don't want.
          // eslint-disable-next-line @typescript-eslint/no-unused-vars
          const { dismiss, ...alertProps } = a;
          const boundDismiss = bindActionCreators(() => a.dismiss, dispatch);
          return <AlertBox key={i} dismiss={boundDismiss} {...alertProps} />;
        })}
      </section>
    );
  }
}

const overviewAlertListSectionConnected = connect(
  (state: AdminUIState) => {
    return {
      alerts: overviewListAlertsSelector(state),
    };
  },
  dispatch => {
    return {
      dispatch: dispatch,
    };
  },
)(OverviewAlertListSection);

export default overviewAlertListSectionConnected;
