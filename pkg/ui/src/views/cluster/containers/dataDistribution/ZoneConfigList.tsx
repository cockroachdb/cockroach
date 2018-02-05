import React from "react";
import { AdminUIState } from "oss/src/redux/state";
import { refreshNodes, refreshReplicaMatrix } from "oss/src/redux/apiReducers";
import { cockroach } from "oss/src/js/protos";
import { connect } from "react-redux";

interface ZoneConfigListProps {
  zoneConfigs: cockroach.server.serverpb.ReplicaMatrixResponse.ZoneConfig[];
}

class ZoneConfigList extends React.Component<ZoneConfigListProps, {}> {
  render() {
    if (!this.props.zoneConfigs) {
      return (
        <p>Loading...</p>
      );
    }

    return (
      <div className="zone-config-list">
        <ul>
          {this.props.zoneConfigs.map((zoneConfig) => (
            <li key={zoneConfig.cli_specifier} style={{ paddingTop: 10 }}>
              <h3>{zoneConfig.cli_specifier}</h3>
              <pre style={{ paddingTop: 5 }}>
                {zoneConfig.config_yaml}
              </pre>
            </li>
          ))}
        </ul>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const ZoneConfigListConnected = connect(
  (state: AdminUIState) => {
    return {
      zoneConfigs: state.cachedData.replicaMatrix
        ? state.cachedData.replicaMatrix.data.zone_configs
        : null,
    };
  },
  {
    refreshReplicaMatrix,
    refreshNodes,
  },
)(ZoneConfigList);

export default ZoneConfigListConnected;
