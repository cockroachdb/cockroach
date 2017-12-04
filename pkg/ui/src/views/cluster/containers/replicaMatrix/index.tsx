import _ from "lodash";
import React from "react";
import Link from "react-router/lib/Link";
import {connect} from "react-redux";
import {AdminUIState} from "oss/src/redux/state";
import { refreshReplicaMatrix, refreshNodes } from "src/redux/apiReducers";
import {cockroach} from "oss/src/js/protos";
import { NodeStatus$Properties } from "src/util/proto";
import "./index.styl";
import ReplicaMatrixResponse = cockroach.server.serverpb.ReplicaMatrixResponse;

interface ReplicaMatrixProps {
  replicaMatrix: ReplicaMatrixResponse;
  nodes: NodeStatus$Properties[];
  refreshReplicaMatrix: typeof refreshReplicaMatrix;
  refreshNodes: typeof refreshNodes;
}

class ReplicaMatrix extends React.Component<ReplicaMatrixProps, {}> {
  render() {
    if (!this.props.replicaMatrix || !this.props.nodes) {
      return (<p>Loading...</p>);
    }

    const byDbByTableByNode: {[db: string]: { [table: string]: {[node: string]: Long} }} = {};
    this.props.replicaMatrix.cells.forEach((cell) => {
      _.set(byDbByTableByNode, [cell.database_name, cell.table_name, cell.node_id], cell.count);
    });

    function localityToString(locality: cockroach.roachpb.Locality$Properties) {
      return locality.tiers.map((tier) => (`${tier.key}=${tier.value}`)).join(", ");
    }

    return (
      <table className="replica-matrix">
        <thead>
          <tr>
            <th colSpan={2} />
            <th colSpan={this.props.nodes.length}>Node ID</th>
          </tr>
          <tr>
            <th colSpan={2} />
            {this.props.nodes.map((node) => (
              <th title={localityToString(node.desc.locality)}>
                {node.desc.node_id.toString()}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {_.map(byDbByTableByNode, (byTableByNode, dbName) => (
            _.map(byTableByNode, (byNode, tableName) => (
              <tr key={`${dbName}-${tableName}`}>
                <th>{dbName}</th>
                <th className="table-name">{tableName}</th>
                {this.props.nodes.map((node) => (
                  <td className="value">{byNode[node.desc.node_id].toString()}</td>
                ))}
              </tr>
            ))
          ))}
        </tbody>
      </table>
    );
  }
}

class ReplicaMatrixMain extends React.Component<ReplicaMatrixProps, {}> {
  componentDidMount() {
    this.props.refreshReplicaMatrix();
    this.props.refreshNodes();
  }

  render() {
    return (
      <div>
        <div className="section">
          <h1>Replica Matrix</h1>
        </div>
        <div className="section">
          <ReplicaMatrix {...this.props} />
        </div>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const ReplicaMatrixMainConnected = connect(
  (state: AdminUIState) => {
    return {
      replicaMatrix: state.cachedData.replicaMatrix.data,
      nodes: state.cachedData.nodes.data,
    };
  },
  {
    refreshReplicaMatrix,
    refreshNodes,
  },
)(ReplicaMatrixMain);

function ReplicaMatrixPage() {
  return (
    <div>
      <section className="section parent-link">
        <Link to="/cluster">&lt; Back to Cluster</Link>
      </section>
      <ReplicaMatrixMainConnected />
    </div>
  );
}

export default ReplicaMatrixPage;
