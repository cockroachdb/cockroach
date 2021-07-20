// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";
import { createSelector } from "reselect";
import { connect } from "react-redux";
import Helmet from "react-helmet";
import { withRouter } from "react-router-dom";

import { Loading } from "@cockroachlabs/cluster-ui";
import { InfoTooltip } from "src/components/infoTooltip";
import * as docsURL from "src/util/docs";
import { FixLong } from "src/util/fixLong";
import { cockroach } from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import {
  refreshDataDistribution,
  refreshNodes,
  refreshLiveness,
  CachedDataReducerState,
} from "src/redux/apiReducers";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import ReplicaMatrix, { SchemaObject } from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.styl";
import {
  selectLivenessRequestStatus,
  selectNodeRequestStatus,
} from "src/redux/nodes";

type DataDistributionResponse = cockroach.server.serverpb.DataDistributionResponse;
type NodeDescriptor = cockroach.roachpb.INodeDescriptor;
type ZoneConfig$Properties = cockroach.server.serverpb.DataDistributionResponse.IZoneConfig;

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
          {this.props.sortedZoneConfigs.map((zoneConfig) => (
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
      children: _.map(databaseInfo, (dbInfo, dbName) => ({
        name: dbName,
        data: {
          dbName,
        },
        children: _.map(dbInfo.table_info, (tableInfo, tableName) => ({
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

export class DataDistributionPage extends React.Component<DataDistributionPageProps> {
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
        <section className="section">
          <h1 className="base-heading">Data Distribution</h1>
        </section>
        <section className="section">
          <Loading
            loading={
              !this.props.dataDistribution.data || !this.props.localityTree
            }
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
  (dataDistributionState) => {
    if (!dataDistributionState.data) {
      return null;
    }
    return _.sortBy(dataDistributionState.data.zone_configs, (zc) => zc.target);
  },
);

const localityTreeErrors = createSelector(
  selectNodeRequestStatus,
  selectLivenessRequestStatus,
  (nodes, liveness) => [nodes.lastError, liveness.lastError],
);

const DataDistributionPageConnected = withRouter(
  connect(
    (state: AdminUIState) => ({
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
  _.forEach(localityTree.localities, (valuesForKey, key) => {
    _.forEach(valuesForKey, (subLocalityTree, value) => {
      children.push(
        nodeTreeFromLocalityTree(`${key}=${value}`, subLocalityTree),
      );
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
