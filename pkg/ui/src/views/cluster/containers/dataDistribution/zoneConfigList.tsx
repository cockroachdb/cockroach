import React from "react";
import { connect } from "react-redux";
import _ from "lodash";

import { AdminUIState } from "src/redux/state";
import { refreshReplicaMatrix } from "src/redux/apiReducers";
import { cockroach } from "src/js/protos";
import spinner from "assets/spinner.gif";
import Loading from "src/views/shared/components/loading";

interface ZoneConfigListProps {
  refreshReplicaMatrix: typeof refreshReplicaMatrix;
  zoneConfigs: { [id: string]: cockroach.server.serverpb.ReplicaMatrixResponse.ZoneConfig };
}

class ZoneConfigList extends React.Component<ZoneConfigListProps, {}> {
  componentDidMount() {
    this.props.refreshReplicaMatrix();
  }

  componentDidUpdate() {
    this.props.refreshReplicaMatrix();
  }

  render() {
    const sortedIDs = _.keys(this.props.zoneConfigs);
    sortedIDs.sort();

    return (
      <div className="zone-config-list">
        <Loading loading={!this.props.zoneConfigs} className={"loading-image"} image={spinner}>
          <ul>
            {sortedIDs.map((zcId) => {
              const zoneConfig = this.props.zoneConfigs[zcId];
              return (
                <li key={zcId} style={{ paddingTop: 10 }}>
                  <h3>{zoneConfig.cli_specifier}</h3>
                  <pre style={{ paddingTop: 5 }}>
                    {zoneConfig.config_yaml}
                  </pre>
                </li>
              );
            })}
          </ul>
        </Loading>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const ZoneConfigListConnected = connect(
  (state: AdminUIState) => {
    return {
      zoneConfigs: state.cachedData.replicaMatrix.data
        ? state.cachedData.replicaMatrix.data.zone_configs
        : null,
    };
  },
  {
    refreshReplicaMatrix,
  },
)(ZoneConfigList);

export default ZoneConfigListConnected;
