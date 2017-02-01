import * as React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import { AdminUIState } from "../redux/state";
import { Alert, AlertIcon, alertsSelector, setDismissed } from "../services/alertService";
import { warningIcon } from "../components/icons";
import { trustIcon } from "../util/trust";

interface AlertBoxState {
  alerts: Alert[];
};

function alertIcon (icon: AlertIcon) {
  switch (icon) {
    case AlertIcon.WARNING:
      return trustIcon(warningIcon);
    default:
      return null;
  }
}

class AlertBox extends React.Component<AlertBoxState, {}> {

  dismiss = (id: string) => {
    // TODO: USE UI REDUCER INSTEAD
    setDismissed(id, true);
  }

  render() {
    let visibleAlert = _.find(this.props.alerts, (alert) => !alert.dismissed);
    let dismiss = () => this.dismiss(visibleAlert.id);
    if (visibleAlert) {
      return <div className="alert summary-section">
        <div className="icon" dangerouslySetInnerHTML={alertIcon(visibleAlert.icon)} />
        <div className="title">{visibleAlert.title}</div>
        <div className="close" onClick={dismiss}>
          ✕
        </div>
        {visibleAlert.content}
      </div>;
    }
    return null;
  }
}

// Connect the OutdatedAlert class with our redux store.
let alertBoxConnected = connect(
  (state: AdminUIState) => {
    return {
      alerts: alertsSelector(state),
    };
  },
  {
  },
)(AlertBox);

export default alertBoxConnected;
