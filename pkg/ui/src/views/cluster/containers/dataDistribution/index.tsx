import _ from "lodash";
import React from "react";
import { createSelector } from "reselect";
import { connect } from "react-redux";
import Helmet from "react-helmet";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import * as docsURL from "src/util/docs";
import { FixLong } from "src/util/fixLong";
import { cockroach } from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import {
  refreshDataDistribution,
  refreshNodes,
  refreshLiveness,
  refreshLeaseholdersAndQPS,
} from "src/redux/apiReducers";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import ReplicaMatrix, {
  METRIC_LEASEHOLDERS,
  METRIC_QPS,
  METRIC_REPLICAS,
  SchemaObject,
} from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.styl";

type DataDistributionResponse = cockroach.server.serverpb.DataDistributionResponse;
type LeaseholdersAndQPSResponse = cockroach.server.serverpb.LeaseholdersAndQPSResponse;
type NodeDescriptor = cockroach.roachpb.INodeDescriptor;
type ZoneConfig$Properties = cockroach.server.serverpb.DataDistributionResponse.IZoneConfig;

const ZONE_CONFIG_TEXT = (
  <span>
    Zone configurations
    (<a href={docsURL.configureReplicationZones} target="_blank">see documentation</a>)
    control how CockroachDB distributes data across nodes.
  </span>
);

interface DataDistributionProps {
  dataDistribution: DataDistributionResponse;
  leaseholdersAndQPS: LeaseholdersAndQPSResponse;
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
}

class DataDistribution extends React.Component<DataDistributionProps> {

  renderZoneConfigs() {
    return (
      <div className="zone-config-list">
        <ul>
          {this.props.sortedZoneConfigs.map((zoneConfig) => (
            <li key={zoneConfig.cli_specifier} className="zone-config">
              <h3>{zoneConfig.cli_specifier}</h3>
              <pre className="zone-config__raw-yaml">
                {zoneConfig.config_yaml}
              </pre>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  getCellValue = (metric: string) => {
    return (dbPath: TreePath, nodePath: TreePath): number => {
      switch (metric) {
        case METRIC_QPS:
          return this.getQPS(dbPath, nodePath);
        case METRIC_LEASEHOLDERS:
          return this.getLeaseholderCount(dbPath, nodePath);
        case METRIC_REPLICAS:
          return this.getReplicaCount(dbPath, nodePath);
        default:
          return 0;
      }
    };
  }

  getLeaseholderCount(dbPath: TreePath, nodePath: TreePath): number {
    const tableName = dbPath[1];
    const nodeID = nodePath[nodePath.length - 1];

    const tableID = this.tableIDForName(tableName);

    // gah this is stupid. use int32s
    // TODO(vilterp): remove this keyBy...
    const tableInfo = this.props.leaseholdersAndQPS.table_infos[tableID];

    if (tableInfo) {
      return _.sumBy(tableInfo.range_infos, (rangeInfo) => {
        return rangeInfo.leaseholder_node_id.toString() === nodeID ? 1 : 0;
      });
    } else {
      return 0;
    }
  }

  getQPS(dbPath: TreePath, nodePath: TreePath): number {
    const tableName = dbPath[1];
    const nodeID = nodePath[nodePath.length - 1];

    const tableID = this.tableIDForName(tableName);

    // gah this is stupid. use int32s
    // TODO(vilterp): remove this keyBy...
    const tableInfo = this.props.leaseholdersAndQPS.table_infos[tableID];

    if (tableInfo) {
      return _.sumBy(tableInfo.range_infos, (rangeInfo) => {
        const replicaOnThisNode = rangeInfo.replica_info[nodeID];
        if (replicaOnThisNode) {
          return Math.round(replicaOnThisNode.stats.queries_per_second);
        }
        return 0;
      });
    } else {
      return 0;
    }
  }

  getReplicaCount(dbPath: TreePath, nodePath: TreePath): number {
    const [dbName, tableName] = dbPath;
    const nodeID = nodePath[nodePath.length - 1];
    const databaseInfo = this.props.dataDistribution.database_info;

    const res = databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[nodeID];
    if (!res) {
      return 0;
    }
    return FixLong(res).toInt();
  }

  tableIDForName(name: string) {
    let id = 0;
    _.forEach(this.props.dataDistribution.database_info, (dbInfo) => {
      _.forEach(dbInfo.table_info, (tableInfo, tableName) => {
        if (tableName === name) {
          id = tableInfo.id.toInt();
          // TODO(vilterp): how do you bail out of this early?
        }
      });
    });
    return id;
  }

  render() {
    const nodeTree = nodeTreeFromLocalityTree("Cluster", this.props.localityTree);

    const databaseInfo = this.props.dataDistribution.database_info;
    const dbTree: TreeNode<SchemaObject> = {
      name: "Cluster",
      data: { dbName: null, tableName: null },
      children: _.map(databaseInfo, (dbInfo, dbName) => ({
        name: dbName,
        data: { dbName },
        children: _.map(dbInfo.table_info, (tableInfo, tableName) => ({
          name: tableName,
          data: {
            dbName,
            tableName,
            tableID: tableInfo.id.toInt(),
          },
        })),
      })),
    };

    return (
      <div className="data-distribution">
        <div className="data-distribution__zone-config-sidebar">
          <h2>
            Zone Configs{" "}
            <div className="section-heading__tooltip">
              <ToolTipWrapper text={ZONE_CONFIG_TEXT}>
                <div className="section-heading__tooltip-hover-area">
                  <div className="section-heading__info-icon">i</div>
                </div>
              </ToolTipWrapper>
            </div>
          </h2>
          {this.renderZoneConfigs()}
        </div>
        <div>
          <ReplicaMatrix
            cols={nodeTree}
            rows={dbTree}
            getValue={this.getCellValue}
          />
        </div>
      </div>
    );
  }
}

interface DataDistributionPageProps {
  dataDistribution: DataDistributionResponse;
  leaseholdersAndQPS: LeaseholdersAndQPSResponse;
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
  refreshDataDistribution: typeof refreshDataDistribution;
  refreshLeaseholdersAndQPS: typeof refreshLeaseholdersAndQPS;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

class DataDistributionPage extends React.Component<DataDistributionPageProps> {

  componentDidMount() {
    this.props.refreshDataDistribution();
    this.props.refreshLeaseholdersAndQPS();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps() {
    this.props.refreshDataDistribution();
    this.props.refreshLeaseholdersAndQPS();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  render() {
    return (
      <div>
        <Helmet>
          <title>Data Distribution</title>
        </Helmet>
        <section className="section">
          <h1>Data Distribution</h1>
        </section>
        <section style={{ paddingTop: 10, paddingLeft: 30, display: "block" }}>
          <Loading
            className="loading-image loading-image__spinner-left"
            loading={!this.props.dataDistribution || !this.props.localityTree}
            image={spinner}
          >
            <DataDistribution
              localityTree={this.props.localityTree}
              dataDistribution={this.props.dataDistribution}
              leaseholdersAndQPS={this.props.leaseholdersAndQPS}
              sortedZoneConfigs={this.props.sortedZoneConfigs}
            />
          </Loading>
        </section>
      </div>
    );
  }
}

const sortedZoneConfigs = createSelector(
  (state: AdminUIState) => state.cachedData.dataDistribution,
  (dataDistributionState) => {
    if (!dataDistributionState.data) {
      return null;
    }
    return _.sortBy(dataDistributionState.data.zone_configs, (zc) => zc.cli_specifier);
  },
);

// tslint:disable-next-line:variable-name
const DataDistributionPageConnected = connect(
  (state: AdminUIState) => ({
    dataDistribution: state.cachedData.dataDistribution.data,
    leaseholdersAndQPS: state.cachedData.leaseholdersAndQPS.data,
    sortedZoneConfigs: sortedZoneConfigs(state),
    localityTree: selectLocalityTree(state),
  }),
  {
    refreshDataDistribution,
    refreshLeaseholdersAndQPS,
    refreshNodes,
    refreshLiveness,
  },
)(DataDistributionPage);

export default DataDistributionPageConnected;

// Helpers

function nodeTreeFromLocalityTree(
  rootName: string,
  localityTree: LocalityTree,
): TreeNode<NodeDescriptor> {
  const children: TreeNode<any>[] = [];

  // Add child localities.
  _.forEach(localityTree.localities, (valuesForKey, key) => {
    _.forEach(valuesForKey, (subLocalityTree, value) => {
      children.push(nodeTreeFromLocalityTree(`${key}=${value}`, subLocalityTree));
    });
  });

  // Add child nodes.
  _.forEach(localityTree.nodes, (node) => {
    children.push({
      name: node.desc.node_id.toString(),
      data: node.desc,
    });
  });

  return {
    name: rootName,
    children: children,
  };
}
