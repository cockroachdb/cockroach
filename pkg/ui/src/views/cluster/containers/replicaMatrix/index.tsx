import _ from "lodash";
import React from "react";
import Link from "react-router/lib/Link";
import {connect} from "react-redux";
import { AdminUIState } from "oss/src/redux/state";
import { refreshReplicaMatrix, refreshNodes } from "src/redux/apiReducers";
import { cockroach } from "oss/src/js/protos";
import { NodeStatus$Properties } from "src/util/proto";
import Matrix from "./Matrix";
import {TreeNode, setAtPath, TreePath} from "./tree";
import "./index.styl";
import ReplicaMatrixResponse = cockroach.server.serverpb.ReplicaMatrixResponse;
import NodeDescriptor = cockroach.roachpb.NodeDescriptor$Properties;

interface ReplicaMatrixProps {
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
    console.log('SAP', path);
    setAtPath(root, path, {
      name: `n${node.node_id.toString()}`,
      data: node,
    });
  });
  return root;
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

    function renderCell(tableDesc: TableDesc, nodeDesc: NodeDescriptor): JSX.Element | null {
      if (!_.has(tableDesc, "tableName")) {
        return null;
      }

      const val = byDbByTableByNode[tableDesc.dbName][tableDesc.tableName][nodeDesc.node_id.toString()];
      return (<span>{val ? val.toString() : ""}</span>);
    }

    return (
      <ReplicaMatrix
        label={<em># Replicas</em>}
        cols={nodeTree}
        rows={dbTree}
        colNodeLabel={(_, path: TreePath) => (path.length === 0 ? "Cluster" : path[path.length - 1])}
        colLeafLabel={(node: NodeDescriptor) => `n${node.node_id.toString()}`}
        rowNodeLabel={(row: TableDesc) => (row === null ? "Cluster" : `DB: ${row.dbName}`)}
        rowLeafLabel={(row: TableDesc) => (row.tableName)}
        renderCell={renderCell} />
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
