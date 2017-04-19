import * as React from "react";
import { Dispatch, bindActionCreators } from "redux";
import { connect } from "react-redux";

import { AlertBox } from "../components/alertBox";
import { Alert, bannerAlertsSelector } from "../redux/alerts";
import { AdminUIState } from "../redux/state";

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
