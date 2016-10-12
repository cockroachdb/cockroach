import * as React from "react";
import { connect } from "react-redux";
import Banner from "./banner";

import { AdminUIState } from "../../redux/state";
import { refreshHealth, HealthState } from "../../redux/apiReducers";

class DisconnectedBannerProps {
  health: HealthState;
  refreshHealth: typeof refreshHealth;
}

class DisconnectedBannerState {
  dismissed: boolean = false;
}

class DisconnectedBanner extends React.Component<DisconnectedBannerProps, DisconnectedBannerState> {
  state = new DisconnectedBannerState();

  componentWillMount() {
    this.props.refreshHealth();
  }

  componentWillReceiveProps(props: DisconnectedBannerProps) {
    props.refreshHealth();
    if (props.health.valid && !props.health.lastError) {
      this.setState({ dismissed: false });
    }
  }

  render() {
    let visible: boolean = this.props.health && !!this.props.health.lastError && !this.state.dismissed;
    return <Banner className="disconnected" visible={visible} onclose={() => this.setState({dismissed: true}) }>
        <span className="icon-warning" />
        Connection to Cockroach node lost.
    </Banner>;
  }
}

let health = (state: AdminUIState): HealthState => state.cachedData.health;

// Connect the DisconnectedBanner class with our redux store.
let disconnectedBannerConnected = connect(
  (state: AdminUIState) => {
    return {
      health: health(state),
    };
  },
  {
    refreshHealth,
  }
)(DisconnectedBanner);

export default disconnectedBannerConnected;
