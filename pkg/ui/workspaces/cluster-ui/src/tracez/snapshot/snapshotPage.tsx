// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useMemo } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import {
  ListTracingSnapshotsResponse,
  GetTracingSnapshotResponse,
  TakeTracingSnapshotResponse,
} from "src/api/tracezApi";
import { Button, Icon } from "@cockroachlabs/ui-components";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { join } from "path";
import {
  getDataFromServer,
  getMatchParamByName,
  TimestampToMoment,
} from "src/util";
import { syncHistory } from "src/util";

import { SpanTable } from "./spanTable";

import { commonStyles } from "src/common";
import styles from "../snapshot.module.scss";
import classNames from "classnames/bind";

const cx = classNames.bind(styles);

export interface SnapshotPageStateProps {
  sort: SortSetting;
  snapshotsError?: Error;
  snapshotsLoading: boolean;
  snapshots?: ListTracingSnapshotsResponse;
  snapshotsValid: boolean;
  snapshot: GetTracingSnapshotResponse;
  snapshotError?: Error;
  snapshotLoading: boolean;
  nodes?: cockroach.server.status.statuspb.INodeStatus[];
  takeSnapshot: (nodeID: string) => Promise<TakeTracingSnapshotResponse>;
}

export interface SnapshotPageDispatchProps {
  setSort: (value: SortSetting) => void;
  refreshSnapshots: (id: string) => void;
  refreshSnapshot: (req: { nodeID: string; snapshotID: number }) => void;
  refreshNodes: () => void;
}

type UrlParams = Partial<
  Record<"nodeID" | "snapshotID" | "ascending" | "columnTitle", string>
>;
export type SnapshotPageProps = SnapshotPageStateProps &
  SnapshotPageDispatchProps &
  RouteComponentProps<UrlParams>;

export const SnapshotPage: React.FC<SnapshotPageProps> = props => {
  const {
    history,
    match,
    refreshNodes,
    refreshSnapshots,
    refreshSnapshot,
    nodes,
    snapshots,
    snapshot,
    snapshotsValid,
    sort,
    setSort,
    takeSnapshot,
  } = props;

  // Sort Settings.
  const ascending = match.params.ascending === "true";
  const columnTitle = match.params.columnTitle || "";

  // Always an integer ID.
  const snapshotID = parseInt(getMatchParamByName(match, "snapshotID"));
  // Usually a string-wrapped integer ID, but also supports alias "local."
  const nodeID = getMatchParamByName(match, "nodeID");

  // Load initial data.
  useEffect(() => {
    refreshNodes();
  }, [refreshNodes]);

  useEffect(() => {
    if (!nodeID) {
      return;
    }
    refreshSnapshots(nodeID);
  }, [nodeID, refreshSnapshots]);

  useEffect(() => {
    if (!snapshotID) {
      return;
    }
    refreshSnapshot({
      nodeID: nodeID,
      snapshotID: snapshotID,
    });
  }, [nodeID, snapshotID, refreshSnapshot]);

  const snapArray = snapshots?.snapshots;
  const snapArrayAsJson = JSON.stringify(snapArray);

  // If no node was provided, navigate explicitly to the local node.
  // If no snapshot was provided, navigate to the most recent.
  useEffect(() => {
    if (nodeID && snapshotID) {
      return;
    }
    if (!nodeID) {
      let targetNodeID = getDataFromServer().NodeID;
      if (!targetNodeID) {
        targetNodeID = "local";
      }
      history.location.pathname = join("/debug/tracez_v2/node/", targetNodeID);
    }

    if (!snapArray?.length || !snapshotsValid) {
      // If we have no snapshots, or the record is stale, don't navigate.
      history.replace(history.location);
      return;
    }
    const lastSnapshotID = snapArray[snapArray.length - 1].snapshot_id;
    history.location.pathname = join(
      history.location.pathname,
      "snapshot",
      lastSnapshotID.toString(),
    );
    history.replace(history.location);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snapArrayAsJson, snapshotID, nodeID, history, snapshotsValid]);

  // Update sort based on URL.
  useEffect(() => {
    if (!columnTitle) {
      return;
    }
    setSort({ columnTitle, ascending });
  }, [setSort, columnTitle, ascending]);

  const onSnapshotSelected = (item: number) => {
    history.location.pathname = join(
      "/debug/tracez_v2/node",
      nodeID,
      "snapshot",
      item.toString(),
    );
    history.push(history.location);
  };

  const onNodeSelected = (item: string) => {
    history.location.pathname = join("/debug/tracez_v2/node/", item);
    history.push(history.location);
  };

  const changeSortSetting = (ss: SortSetting): void => {
    setSort(ss);
    syncHistory(
      {
        ascending: ss.ascending.toString(),
        columnTitle: ss.columnTitle,
      },
      history,
    );
  };

  const takeAndLoadSnapshot = () => {
    takeSnapshot(nodeID).then(resp => {
      refreshSnapshots(nodeID);
      // Load the new snapshot.
      history.location.pathname = join(
        "/debug/tracez_v2/node",
        nodeID,
        "/snapshot/",
        resp.snapshot.snapshot_id.toString(),
      );
      history.push(history.location);
    });
  };

  const [snapshotItems, snapshotName] = useMemo(() => {
    if (!snapArray) {
      return [[], ""];
    }
    let selectedName = "";
    const items = snapArray.map(snapshotInfo => {
      const id = snapshotInfo.snapshot_id.toNumber();
      const time = TimestampToMoment(snapshotInfo.captured_at).format(
        "MMM D, YYYY [at] HH:mm:ss",
      );
      const out = {
        name: "Snapshot " + id + ": " + time,
        value: id,
      };
      if (id === snapshotID) {
        selectedName = out.name;
      }
      return out;
    });
    return [items, selectedName];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snapArrayAsJson, snapshotID]);

  const [nodeItems, nodeName] = useMemo(() => {
    if (!nodes) {
      return [[], ""];
    }
    let selectedName = "";
    const items = nodes.map(node => {
      const id = node.desc.node_id.toString();
      const out = {
        name: "Node " + id,
        value: id,
      };
      if (id === nodeID) {
        selectedName = out.name;
      }
      return out;
    });
    return [items, selectedName];
  }, [nodes, nodeID]);

  const isLoading = props.snapshotsLoading || props.snapshotLoading;
  const error = props.snapshotsError || props.snapshotError;
  return (
    <div className={cx("snapshots-page")}>
      <Helmet title="Snapshots" />
      <h3 className={commonStyles("base-heading")}>Snapshots</h3>
      <div>
        <PageConfig>
          <PageConfigItem>
            <Button onClick={takeAndLoadSnapshot} intent="secondary">
              <Icon iconName="Download" /> Take snapshot
            </Button>
          </PageConfigItem>
          <PageConfigItem>
            <Dropdown items={nodeItems} onChange={onNodeSelected}>
              {nodeName}
            </Dropdown>
          </PageConfigItem>
          {snapshotItems.length > 0 && (
            <PageConfigItem>
              <Dropdown<number>
                items={snapshotItems}
                onChange={onSnapshotSelected}
              >
                {snapshotName}
              </Dropdown>
            </PageConfigItem>
          )}
        </PageConfig>
      </div>
      <section className={cx("section")}>
        {snapshotID ? (
          <Loading
            loading={isLoading}
            page={"snapshots"}
            error={error}
            render={() => (
              <SpanTable
                snapshot={snapshot?.snapshot}
                setSort={changeSortSetting}
                sort={sort}
              />
            )}
          />
        ) : (
          "No snapshots found on this node."
        )}
      </section>
    </div>
  );
};
