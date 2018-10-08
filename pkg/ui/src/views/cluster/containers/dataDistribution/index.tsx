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
import { refreshDataDistribution, refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import ReplicaMatrix, { SchemaObject } from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.styl";

type DataDistributionResponse = cockroach.server.serverpb.DataDistributionResponse;
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
              <pre className="zone-config__raw-sql">
                {zoneConfig.config_sql}
              </pre>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  getCellValue = (dbPath: TreePath, nodePath: TreePath): number => {
    const [dbName, tableName] = dbPath;
    const nodeID = nodePath[nodePath.length - 1];
    const databaseInfo = this.props.dataDistribution.database_info;

    const res = databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[nodeID];
    if (!res) {
      return 0;
    }
    return FixLong(res).toInt();
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
        children: _.map(dbInfo.table_info, (_tableInfo, tableName) => ({
          name: tableName,
          data: { dbName, tableName },
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
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
  refreshDataDistribution: typeof refreshDataDistribution;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

class DataDistributionPage extends React.Component<DataDistributionPageProps> {

  componentDidMount() {
    this.props.refreshDataDistribution();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps() {
    this.props.refreshDataDistribution();
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
        <section className="section">
          <Loading
            className="loading-image loading-image__spinner-left"
            loading={!this.props.dataDistribution || !this.props.localityTree}
            image={spinner}
            render={() => (
              <DataDistribution
                localityTree={this.props.localityTree}
                dataDistribution={this.props.dataDistribution}
                sortedZoneConfigs={this.props.sortedZoneConfigs}
              />
            )}
          />
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
    sortedZoneConfigs: sortedZoneConfigs(state),
    localityTree: selectLocalityTree(state),
  }),
  {
    refreshDataDistribution,
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
