import _ from "lodash";
import React from "react";
import { createSelector } from "reselect";
import { connect } from "react-redux";
import Helmet from "react-helmet";

import Loading from "src/views/shared/components/loading";
import spinner from "assets/spinner.gif";
import { ToolTipWrapper } from "src/views/shared/components/toolTip";
import * as docsURL from "src/util/docs";
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

import IReplicaInfo = cockroach.server.serverpb.LeaseholdersAndQPSResponse.IReplicaInfo;
type DataDistributionResponse = cockroach.server.serverpb.DataDistributionResponse;
type LeaseholdersAndQPSResponse = cockroach.server.serverpb.LeaseholdersAndQPSResponse;
type INodeDescriptor = cockroach.roachpb.INodeDescriptor;
type IZoneConfig = cockroach.server.serverpb.DataDistributionResponse.IZoneConfig;
type IRangeInfo = cockroach.server.serverpb.LeaseholdersAndQPSResponse.IRangeInfo;

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
  sortedZoneConfigs: IZoneConfig[];
  tablesByName: { [name: string]: number };
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
    const nodeID = nodePath[nodePath.length - 1];
    const range = this.getRangeAtPath(dbPath);

    if (!range) {
      return 0;
    }

    const isLeaseholder = range.leaseholder_node_id.toString() === nodeID;
    return isLeaseholder ? 1 : 0;
  }

  getQPS(dbPath: TreePath, nodePath: TreePath): number {
    const replica = this.getReplicaAtPaths(dbPath, nodePath);

    return replica ? Math.round(replica.stats.queries_per_second) : 0;
  }

  getReplicaCount(dbPath: TreePath, nodePath: TreePath): number {
    const replica = this.getReplicaAtPaths(dbPath, nodePath);

    return replica ? 1 : 0;
  }

  tableIDForName(name: string): number {
    return this.props.tablesByName[name];
  }

  getRangeAtPath(dbPath: TreePath): IRangeInfo {
    const tableName = dbPath[1];
    const rangeID = dbPath[2];
    const tableID = this.tableIDForName(tableName);
    const ranges = getRangesForTableID(this.props.leaseholdersAndQPS, tableID);
    return ranges[rangeID];
  }

  getReplicaAtPaths(dbPath: TreePath, nodePath: TreePath): IReplicaInfo {
    const nodeID = nodePath[nodePath.length - 1];

    const range = this.getRangeAtPath(dbPath);
    if (!range) {
      return null; // TODO(vilterp) under what circumstances does this happen?
    }

    return range.replica_info[nodeID];
  }

  getRangesForTableID(tableID: number): { [K: string]: IRangeInfo } {
    const maybeTableInfo = this.props.leaseholdersAndQPS.table_infos[tableID.toString()];
    if (maybeTableInfo) {
      return maybeTableInfo.range_infos;
    }
    return {};
  }

  render() {
    const nodeTree = nodeTreeFromLocalityTree("Cluster", this.props.localityTree);

    const schemaTree: TreeNode<SchemaObject> = selectSchemaTree(this.props);

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
            rows={schemaTree}
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
  sortedZoneConfigs: IZoneConfig[];
  tablesByName: { [name: string]: number };
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
    const isLoading = (
      !this.props.dataDistribution ||
      !this.props.localityTree ||
      !this.props.leaseholdersAndQPS
    );

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
            loading={isLoading}
            image={spinner}
          >
            <DataDistribution
              localityTree={this.props.localityTree}
              dataDistribution={this.props.dataDistribution}
              tablesByName={this.props.tablesByName}
              leaseholdersAndQPS={this.props.leaseholdersAndQPS}
              sortedZoneConfigs={this.props.sortedZoneConfigs}
            />
          </Loading>
        </section>
      </div>
    );
  }
}

// Selectors

const sortedZoneConfigs = createSelector(
  (state: AdminUIState) => state.cachedData.dataDistribution,
  (dataDistributionState) => {
    if (!dataDistributionState.data) {
      return null;
    }
    return _.sortBy(dataDistributionState.data.zone_configs, (zc) => zc.cli_specifier);
  },
);

const tablesByName = createSelector(
  (state: AdminUIState) => state.cachedData.dataDistribution,
  (dataDistributionState) => {
    if (!dataDistributionState.data) {
      return {};
    }

    const tables: { [name: string]: number } = {};
    _.forEach(dataDistributionState.data.database_info, (dbInfo) => {
      _.forEach(dbInfo.table_info, (tableInfo, tableName) => {
        tables[tableName] = tableInfo.id.toNumber();
      });
    });
    return tables;
  },
);

function getRangesForTableID(
  leaseholdersAndQPS: LeaseholdersAndQPSResponse,
  tableID: number,
): { [rangeId: string]: IRangeInfo } {
  const maybeTableInfo = leaseholdersAndQPS.table_infos[tableID.toString()];
  if (maybeTableInfo) {
    return maybeTableInfo.range_infos;
  }
  return {};
}

// TODO(vilterp): do I need the two funcs?
const selectSchemaTree = createSelector(
  (props: DataDistributionProps) => props.dataDistribution,
  (props: DataDistributionProps) => props.leaseholdersAndQPS,
  (dataDistribution: DataDistributionResponse, leaseholdersAndQPS: LeaseholdersAndQPSResponse) => {
    console.log("computing schema tree");
    return {
      name: "Cluster",
      data: { dbName: null, tableName: null },
      children: _.map(dataDistribution.database_info, (dbInfo, dbName) => ({
        name: dbName,
        data: { dbName },
        children: _.map(dbInfo.table_info, (tableInfo, tableName) => ({
          name: tableName,
          data: {
            dbName,
            tableName,
            tableID: tableInfo.id.toInt(),
          },
          children: _.map(getRangesForTableID(leaseholdersAndQPS, tableInfo.id.toNumber()), (rangeInfo) => ({
            name: rangeInfo.id.toString(),
            data: { dbName, tableName, rangeID: rangeInfo.id.toString() },
          })),
        })),
      })),
    };
  },
);

// Connected Component

// tslint:disable-next-line:variable-name
const DataDistributionPageConnected = connect(
  (state: AdminUIState) => ({
    dataDistribution: state.cachedData.dataDistribution.data,
    leaseholdersAndQPS: state.cachedData.leaseholdersAndQPS.data,
    sortedZoneConfigs: sortedZoneConfigs(state),
    localityTree: selectLocalityTree(state),
    tablesByName: tablesByName(state),
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
): TreeNode<INodeDescriptor> {
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
