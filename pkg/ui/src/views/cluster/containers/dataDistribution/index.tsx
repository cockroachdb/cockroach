import _ from "lodash";
import React from "react";
import { Link } from "react-router";
import { connect } from "react-redux";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { cockroach } from "src/js/protos";
import { NodeStatus$Properties } from "src/util/proto";
import { AdminUIState } from "src/redux/state";
import docsURL from "src/util/docs";
import { refreshReplicaMatrix, refreshNodes } from "src/redux/apiReducers";
import Matrix from "./matrix";
import ZoneConfigList from "./zoneConfigList";
import { TreeNode, setAtPath, TreePath } from "./tree";
import "./index.styl";

type ReplicaMatrixResponse = cockroach.server.serverpb.ReplicaMatrixResponse;
type NodeDescriptor = cockroach.roachpb.NodeDescriptor$Properties;

interface TableDesc {
  dbName: string;
  tableName?: string;
}

function makeNodeTree(nodes: NodeDescriptor[]): TreeNode<NodeDescriptor> {
  const root: TreeNode<NodeDescriptor> = {
    name: "Cluster",
    data: {},
    children: [],
  };

  nodes.forEach((node) => {
    const path = node.locality.tiers.map((tier) => `${tier.key}=${tier.value}`);
    setAtPath(root, path, {
      name: `n${node.node_id.toString()}`,
      data: node,
    });
  });
  return root;
}

const ZONE_CONFIGS_DOCS_URL = docsURL("configure-replication-zones.html");

class ReplicaMatrix extends Matrix<TableDesc, NodeDescriptor> {}

interface DataDistributionProps {
  replicaMatrix: ReplicaMatrixResponse;
  nodes: NodeStatus$Properties[];
  refreshReplicaMatrix: typeof refreshReplicaMatrix;
  refreshNodes: typeof refreshNodes;
}

class DataDistribution extends React.Component<DataDistributionProps> {

  render() {
    // TODO(vilterp): use locality tree selector
    const nodeTree = makeNodeTree(this.props.nodes.map((n) => n.desc));
    const databaseInfo = this.props.replicaMatrix.database_info;

    const dbTree: TreeNode<TableDesc> = {
      name: "Cluster",
      data: null,
      children: _.map(databaseInfo, (dbInfo, dbName) => ({
        name: dbName,
        data: { dbName },
        children: _.map(dbInfo.table_info, (_, tableName) => ({
          name: tableName,
          data: { dbName, tableName },
        })),
      })),
    };

    function getValue(dbPath: TreePath, nodePath: TreePath): number {
      const [dbName, tableName] = dbPath;
      // TODO(vilterp): substring is to get rid of the "n" prefix; find a different way
      const nodeID = nodePath[nodePath.length - 1].substr(1);

      const res = databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[nodeID];
      if (!res) {
        return 0;
      }
      return res.toInt();
    }

    return (
      <table>
        <tbody>
          <tr>
            <td style={{verticalAlign: "top", paddingRight: 20}}>
              <h2>
                Zone Configs{" "}
                <a href={ZONE_CONFIGS_DOCS_URL}>
                  <small>(?)</small>
                </a>
              </h2>
              <ZoneConfigList/>
            </td>
            <td style={{verticalAlign: "top"}}>
              <ReplicaMatrix
                label={<em># Replicas</em>}
                cols={nodeTree}
                rows={dbTree}
                colNodeLabel={(_, path, isPlaceholder) => (
                  isPlaceholder ? "" : path[path.length - 1]
                )}
                colLeafLabel={(node, path, isPlaceholder) => (
                  isPlaceholder
                    ? ""
                    : node === null
                    ? path[path.length - 1]
                    : `n${node.node_id.toString()}`
                )}
                rowNodeLabel={(row: TableDesc) => (`DB: ${row.dbName}`)}
                rowLeafLabel={(row: TableDesc) => (row.tableName)}
                getValue={getValue}
              />
            </td>
          </tr>
        </tbody>
      </table>
    );
  }
}

class DataDistributionPage extends React.Component<DataDistributionProps> {

  componentDidMount() {
    this.props.refreshReplicaMatrix();
    this.props.refreshNodes();
  }

  componentDidUpdate() {
    this.props.refreshReplicaMatrix();
    this.props.refreshNodes();
  }

  render() {
    return (
      <div>
        <section className="section parent-link">
          <Link to="/debug">&lt; Back to Debug</Link>
        </section>
        <section className="section">
          <h1>Data Distribution</h1>
        </section>
        <section className="section">
          <Loading
            className="loading-image loading-image__spinner-left"
            loading={!this.props.replicaMatrix || !this.props.nodes}
            image={spinner}
          >
            <DataDistribution
              nodes={this.props.nodes}
              refreshNodes={this.props.refreshNodes}
              replicaMatrix={this.props.replicaMatrix}
              refreshReplicaMatrix={this.props.refreshReplicaMatrix}
            />
          </Loading>
        </section>
      </div>
    );
  }
}

// tslint:disable-next-line:variable-name
const DataDistributionPageConnected = connect(
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
)(DataDistributionPage);

export default DataDistributionPageConnected;
