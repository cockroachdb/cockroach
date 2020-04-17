// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import _ from "lodash";
import { Dispatch, Action, bindActionCreators } from "redux";
import { connect } from "react-redux";

import { AlertBox } from "src/views/shared/components/alertBox";
import { AdminUIState } from "src/redux/state";
import { Alert, panelAlertsSelector } from "src/redux/alerts";

interface AlertSectionProps {
  /**
   * List of alerts to display in the alert secion.
   */
  alerts: Alert[];
  /**
   * Raw dispatch method for the current store, will be used to dispatch
   * alert dismissal callbacks.
   */
  dispatch: Dispatch<Action, AdminUIState>;
}

class AlertSection extends React.Component<AlertSectionProps, {}> {
  dismiss = (val: any) => () => {
    const { dispatch } = this.props;
    bindActionCreators(() => val, dispatch);
  };

  render() {
    const { alerts } = this.props;
    return (
      <div>
        {_.map(alerts, (alert, i) => (
          <AlertBox {...alert} key={i} dismiss={this.dismiss(alert.dismiss)} />
        ))}
      </div>
    );
  }
}

const alertSectionConnected = connect(
  (state: AdminUIState) => {
    return {
      alerts: panelAlertsSelector(state),
    };
  },
  (dispatch) => {
    return {
      dispatch: dispatch,
    };
  },
)(AlertSection);

export default alertSectionConnected;
