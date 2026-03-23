// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  api as clusterUiApi,
  useNodesSummary,
} from "@cockroachlabs/cluster-ui";
import filter from "lodash/filter";
import forEach from "lodash/forEach";
import isNil from "lodash/isNil";
import map from "lodash/map";
import sortBy from "lodash/sortBy";
import React, { useMemo } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { InfoTooltip } from "src/components/infoTooltip";
import { cockroach } from "src/js/protos";
import { LocalityTree, selectLocalityTree } from "src/redux/localities";
import { LivenessStatus } from "src/redux/nodes";
import * as docsURL from "src/util/docs";
import { FixLong } from "src/util/fixLong";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";

import ReplicaMatrix, { SchemaObject } from "./replicaMatrix";
import { TreeNode, TreePath } from "./tree";
import "./index.scss";

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
  dataDistribution: DataDistributionResponse;
  localityTree: LocalityTree;
  sortedZoneConfigs: ZoneConfig$Properties[];
}

function DataDistribution({
  dataDistribution,
  localityTree,
  sortedZoneConfigs,
}: DataDistributionProps): React.ReactElement {
  const getCellValue = (dbPath: TreePath, nodePath: TreePath): number => {
    const [dbName, tableName] = dbPath;
    const nodeID = nodePath[nodePath.length - 1];
    const databaseInfo = dataDistribution.database_info;

    const res =
      databaseInfo[dbName].table_info[tableName].replica_count_by_node_id[
        nodeID
      ];
    if (!res) {
      return 0;
    }
    return FixLong(res).toInt();
  };

  const nodeTree = nodeTreeFromLocalityTree("Cluster", localityTree);

  const databaseInfo = dataDistribution.database_info;
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
        <div className="zone-config-list">
          <ul>
            {sortedZoneConfigs.map(zoneConfig => (
              <li key={zoneConfig.target} className="zone-config">
                <pre className="zone-config__raw-sql">
                  {zoneConfig.config_sql}
                </pre>
              </li>
            ))}
          </ul>
        </div>
        <p style={{ maxWidth: 300, paddingTop: 10 }}>
          Dropped tables appear{" "}
          <span className="table-label--dropped">greyed out</span>. Their
          replicas will be garbage collected according to the{" "}
          <code>gc.ttlseconds</code> setting in their zone configs.
        </p>
      </div>
      <div>
        <ReplicaMatrix cols={nodeTree} rows={dbTree} getValue={getCellValue} />
      </div>
    </div>
  );
}

export function DataDistributionPage({
  history,
}: RouteComponentProps): React.ReactElement {
  const {
    nodeStatuses,
    livenessStatusByNodeID,
    isLoading: nodesLoading,
    error: nodesError,
  } = useNodesSummary();

  const {
    data: dataDistribution,
    isLoading: ddLoading,
    error: ddError,
  } = clusterUiApi.useDataDistribution();

  const commissioned = useMemo(
    () =>
      filter(nodeStatuses, node => {
        const livenessStatus = livenessStatusByNodeID[`${node.desc.node_id}`];
        return (
          isNil(livenessStatus) ||
          livenessStatus !== LivenessStatus.NODE_STATUS_DECOMMISSIONED
        );
      }),
    [nodeStatuses, livenessStatusByNodeID],
  );

  const localityTree = useMemo(
    () => selectLocalityTree.resultFunc(commissioned),
    [commissioned],
  );

  const sortedZoneConfigs = useMemo(() => {
    if (!dataDistribution) {
      return null;
    }
    return sortBy(dataDistribution.zone_configs, zc => zc.target);
  }, [dataDistribution]);

  const isLoading = nodesLoading || ddLoading;

  return (
    <div>
      <Helmet title="Data Distribution" />
      <BackToAdvanceDebug history={history} />
      <section className="section">
        <h1 className="base-heading">Data Distribution</h1>
        <p style={{ maxWidth: 300, paddingTop: 10 }}>
          Note: the data distribution render does not work with coalesced
          ranges. To disable coalesced ranges (would increase range count), run
          the following SQL statement:
        </p>
        {RANGE_COALESCING_ADVICE}
      </section>
      <section className="section">
        <Loading
          loading={isLoading || !dataDistribution || !localityTree}
          page={"data distribution"}
          error={[ddError, nodesError]}
          render={() => (
            <DataDistribution
              localityTree={localityTree}
              dataDistribution={dataDistribution}
              sortedZoneConfigs={sortedZoneConfigs}
            />
          )}
        />
      </section>
    </div>
  );
}

export default withRouter(DataDistributionPage);

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
