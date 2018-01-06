import _ from "lodash";
import React from "react";
import Link from "react-router/lib/Link";
import { connect } from "react-redux";
import { AdminUIState } from "oss/src/redux/state";
import { refreshReplicaMatrix, refreshNodes } from "src/redux/apiReducers";
import { cockroach } from "oss/src/js/protos";
import { NodeStatus$Properties } from "src/util/proto";
import Matrix from "./Matrix";
import ZoneConfigList from "./ZoneConfigList";
import { TreeNode, setAtPath, TreePath } from "./tree";
import "./index.styl";

type ReplicaMatrixResponse = cockroach.server.serverpb.ReplicaMatrixResponse;
type NodeDescriptor = cockroach.roachpb.NodeDescriptor$Properties;

interface DataDistributionProps {
  replicaMatrix: ReplicaMatrixResponse;
  nodes: NodeStatus$Properties[];
  refreshReplicaMatrix: typeof refreshReplicaMatrix;
  refreshNodes: typeof refreshNodes;
}

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

const ZONE_CONFIGS_DOCS_URL =
  "https://www.cockroachlabs.com/docs/stable/configure-replication-zones.html";

class DataDistribution extends React.Component<DataDistributionProps, {}> {
  render() {
    if (!this.props.replicaMatrix || !this.props.nodes) {
      return (<p>Loading...</p>);
    }

    const byDbByTableByNode: {[db: string]: { [table: string]: {[node: string]: Long} }} = {};
    this.props.replicaMatrix.cells.forEach((cell) => {
      _.set(
        byDbByTableByNode,
        [cell.database_name, cell.table_name, `n${cell.node_id}`],
        cell.count,
      );
    });

    const nodeTree = makeNodeTree(this.props.nodes.map((n) => n.desc));

    const dbTree: TreeNode<TableDesc> = {
      name: "Cluster",
      data: null,
      children: _.map(byDbByTableByNode, (byTable, dbName) => ({
        name: dbName,
        data: { dbName: dbName },
        children: _.map(byTable, (_, tableName) => ({
          name: tableName,
          data: { dbName, tableName },
        })),
      })),
    };

    interface IReplicaMatrix { new(): Matrix<TableDesc, NodeDescriptor>; }
    // tslint:disable-next-line:variable-name
    const ReplicaMatrix = Matrix as IReplicaMatrix;

    function getValue(dbPath: TreePath, nodePath: TreePath): number {
      const [dbName, tableName] = dbPath;
      const nodeID = nodePath[nodePath.length - 1];

      const res = byDbByTableByNode[dbName][tableName][nodeID];
      if (!res) {
        return 0;
      }
      return res.toInt();
    }

    return (
      <table>
        <tbody>
          <tr>
            <td style={{ verticalAlign: "top", paddingRight: 20 }}>
              <h2>
                Zone Configs{" "}
                <a href={ZONE_CONFIGS_DOCS_URL}>
                  <small>(?)</small>
                </a>
              </h2>
              <ZoneConfigList />
            </td>
            <td style={{ verticalAlign: "top" }}>
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

class DataDistributionPage extends React.Component<DataDistributionProps, {}> {
  componentDidMount() {
    this.props.refreshReplicaMatrix();
    this.props.refreshNodes();
  }

  render() {
    return (
      <div>
        <section className="section parent-link">
          <Link to="/cluster">&lt; Back to Cluster</Link>
        </section>
        <section className="section">
          <h1>Data Distribution</h1>
        </section>
        <section className="section">
          <DataDistribution {...this.props} />
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
