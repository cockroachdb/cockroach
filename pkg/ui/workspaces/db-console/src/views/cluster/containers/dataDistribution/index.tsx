// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Loading } from "@cockroachlabs/cluster-ui";
import forEach from "lodash/forEach";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React from "react";
import Helmet from "react-helmet";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { createSelector } from "reselect";

import { InfoTooltip } from "src/components/infoTooltip";
import { cockroach } from "src/js/protos";
import {
  refreshDataDistribution,
  refreshNodes,
  refreshLiveness,
  CachedDataReducerState,
} from "src/redux/apiReducers";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import {
  selectLivenessRequestStatus,
  selectNodeRequestStatus,
} from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import * as docsURL from "src/util/docs";
import { FixLong } from "src/util/fixLong";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";

import ReplicaMatrix, { SchemaObject } from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.styl";

type DataDistributionResponse =
  cockroach.server.serverpb.DataDistributionResponse;
type NodeDescriptor = cockroach.roachpb.INodeDescriptor;
type ZoneConfig$Properties =
  cockroach.server.serverpb.DataDistributionResponse.IZoneConfig;

const ZONE_CONFIG_TEXT = (
  <span>
    Zone configurations (
    <a
      href={docsURL.configureReplicationZones}
      target="_blank"
      rel="noreferrer"
    >
      see documentation
    </a>
    ) control how CockroachDB distributes data across nodes.
  </span>
);

const RANGE_COALESCING_ADVICE = (
  <div>
    <pre>
      SET CLUSTER SETTING spanconfig.range_coalescing.system.enabled = false;
    </pre>
    <pre>
      SET CLUSTER SETTING spanconfig.range_coalescing.application.enabled =
      false;
    </pre>
  </div>
);

interface DataDistributionProps {
  dataDistribution: CachedDataReducerState<DataDistributionResponse>;
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
}

class DataDistribution extends React.Component<DataDistributionProps> {
  renderZoneConfigs() {
    return (
      <div className="zone-config-list">
        <ul>
          {this.props.sortedZoneConfigs.map(zoneConfig => (
            <li key={zoneConfig.target} className="zone-config">
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
    const databaseInfo = this.props.dataDistribution.data.database_info;

    const res =
      databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[
        nodeID
      ];
    if (!res) {
      return 0;
    }
    return FixLong(res).toInt();
  };

  render() {
    const nodeTree = nodeTreeFromLocalityTree(
      "Cluster",
      this.props.localityTree,
    );

    const databaseInfo = this.props.dataDistribution.data.database_info;
    const dbTree: TreeNode<SchemaObject> = {
      name: "Cluster",
      data: {
        dbName: null,
        tableName: null,
      },
      children: map(databaseInfo, (dbInfo, dbName) => ({
        name: dbName,
        data: {
          dbName,
        },
        children: map(dbInfo.table_info, (tableInfo, tableName) => ({
          name: tableName,
          data: {
            dbName,
            tableName,
            droppedAt: tableInfo.dropped_at,
          },
        })),
      })),
    };

    return (
      <div className="data-distribution">
        <div className="data-distribution__zone-config-sidebar">
          <h2 className="base-heading">
            Zone Configs <InfoTooltip text={ZONE_CONFIG_TEXT} />
          </h2>
          {this.renderZoneConfigs()}
          <p style={{ maxWidth: 300, paddingTop: 10 }}>
            Dropped tables appear{" "}
            <span className="table-label--dropped">greyed out</span>. Their
            replicas will be garbage collected according to the{" "}
            <code>gc.ttlseconds</code> setting in their zone configs.
          </p>
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
  dataDistribution: CachedDataReducerState<DataDistributionResponse>;
  localityTree: LocalityTree;
  localityTreeErrors: Error[];
  sortedZoneConfigs: ZoneConfig$Properties[];
  refreshDataDistribution: typeof refreshDataDistribution;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

export class DataDistributionPage extends React.Component<
  DataDistributionPageProps & RouteComponentProps
> {
  componentDidMount() {
    this.props.refreshDataDistribution();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentDidUpdate() {
    this.props.refreshDataDistribution();
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  render() {
    return (
      <div>
        <Helmet title="Data Distribution" />
        <BackToAdvanceDebug history={this.props.history} />
        <section className="section">
          <h1 className="base-heading">Data Distribution</h1>
          <p style={{ maxWidth: 300, paddingTop: 10 }}>
            Note: the data distribution render does not work with coalesced
            ranges. To disable coalesced ranges (would increase range count),
            run the following SQL statement:
          </p>
          {RANGE_COALESCING_ADVICE}
        </section>
        <section className="section">
          <Loading
            loading={
              !this.props.dataDistribution.data || !this.props.localityTree
            }
            page={"data distribution"}
            error={[
              this.props.dataDistribution.lastError,
              ...this.props.localityTreeErrors,
            ]}
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
  dataDistributionState => {
    if (!dataDistributionState.data) {
      return null;
    }
    return sortBy(dataDistributionState.data.zone_configs, zc => zc.target);
  },
);

const localityTreeErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

const DataDistributionPageConnected = withRouter(
  connect(
    (state: AdminUIState, _: RouteComponentProps) => ({
      dataDistribution: state.cachedData.dataDistribution,
      sortedZoneConfigs: sortedZoneConfigs(state),
      localityTree: selectLocalityTree(state),
      localityTreeErrors: localityTreeErrors(state),
    }),
    {
      refreshDataDistribution,
      refreshNodes,
      refreshLiveness,
    },
  )(DataDistributionPage),
);

export default DataDistributionPageConnected;

// Helpers

function nodeTreeFromLocalityTree(
  rootName: string,
  localityTree: LocalityTree,
): TreeNode<NodeDescriptor> {
  const children: TreeNode<any>[] = [];

  // Add child localities.
  forEach(localityTree.localities, (valuesForKey, key) => {
    forEach(valuesForKey, (subLocalityTree, value) => {
      children.push(
        nodeTreeFromLocalityTree(`${key}=${value}`, subLocalityTree),
      );
    });
  });

  // Add child nodes.
  forEach(localityTree.nodes, node => {
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
