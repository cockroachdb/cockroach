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
  ListTracingSnapshotsResponseMessage,
  GetTracingSnapshotResponseMessage,
  TakeTracingSnapshotResponseMessage,
} from "src/api/tracezApi";
import { Button, Icon } from "@cockroachlabs/ui-components";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { SortSetting } from "src/sortedtable";
import { getMatchParamByName, TimestampToMoment } from "src/util";
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
  snapshots?: ListTracingSnapshotsResponseMessage;
  snapshot: GetTracingSnapshotResponseMessage;
  snapshotError?: Error;
  snapshotLoading: boolean;
  nodes?: cockroach.server.status.statuspb.INodeStatus[];
  nodesLoading: boolean;
  nodesError?: Error;
  defaultNodeID: number;
  takeSnapshot: (nodeID: number) => Promise<TakeTracingSnapshotResponseMessage>;
}

export interface SnapshotPageDispatchProps {
  setSort: (value: SortSetting) => void;
  refreshSnapshots: (id: number) => void;
  refreshSnapshot: (req: { nodeID: number; snapshotID: number }) => void;
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
    sort,
    setSort,
    takeSnapshot,
    defaultNodeID,
  } = props;

  // Sort Settings.
  const ascending = match.params.ascending === "true";
  const columnTitle = match.params.columnTitle || "";

  const snapshotIDStr = getMatchParamByName(match, "snapshotID");
  const nodeIDStr = getMatchParamByName(match, "nodeID");

  // Load iniital data.
  useEffect(() => {
    refreshNodes();
  }, [refreshNodes]);

  useEffect(() => {
    if (!nodeIDStr) {
      return;
    }
    refreshSnapshots(parseInt(nodeIDStr));
  }, [nodeIDStr, refreshSnapshots]);

  useEffect(() => {
    if (!snapshotIDStr) {
      return;
    }
    refreshSnapshot({
      nodeID: parseInt(nodeIDStr),
      snapshotID: parseInt(snapshotIDStr),
    });
  }, [nodeIDStr, snapshotIDStr, refreshSnapshot]);

  const snapArray = snapshots?.snapshots;

  // If no node was provided, navigate to the default if provided, or the first.
  useEffect(() => {
    if (nodeIDStr || !nodes?.length) {
      return;
    }

    const nodeIDs = nodes.map(node => node.desc.node_id);
    let targetNodeID;
    if (defaultNodeID && nodeIDs.includes(defaultNodeID)) {
      targetNodeID = defaultNodeID;
    } else {
      targetNodeID = nodeIDs[0];
    }
    history.location.pathname = "/debug/tracez_v2/node/" + targetNodeID;
    history.replace(history.location);
  }, [nodes, nodeIDStr, defaultNodeID, history]);

  // If a node was provided, but no snapshot was provided, navigate to the most recent.
  useEffect(() => {
    if (!nodeIDStr || snapshotIDStr || !snapArray?.length) {
      return;
    }
    const lastSnapshotID = snapArray[snapArray.length - 1].snapshot_id;
    history.location.pathname =
      "/debug/tracez_v2/node/" + nodeIDStr + "/snapshot/" + lastSnapshotID;
    history.replace(history.location);
  }, [snapArray, snapshotIDStr, nodeIDStr, history]);

  // Update sort based on URL.
  useEffect(() => {
    if (!columnTitle) {
      return;
    }
    setSort({ columnTitle, ascending });
  }, [setSort, columnTitle, ascending]);

  const onSnapshotSelected = (item: string) => {
    history.location.pathname =
      "/debug/tracez_v2/node/" + nodeIDStr + "/snapshot/" + item;
    history.push(history.location);
  };

  const onNodeSelected = (item: string) => {
    history.location.pathname = "/debug/tracez_v2/node/" + item;
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
    takeSnapshot(parseInt(nodeIDStr)).then(resp => {
      refreshSnapshots(parseInt(nodeIDStr));
      // Load the new snapshot.
      history.location.pathname =
        "/debug/tracez_v2/node/" +
        nodeIDStr +
        "/snapshot/" +
        resp.snapshot.snapshot_id.toString();
      history.push(history.location);
    });
  };

  const [snapshotItems, snapshotName] = useMemo(() => {
    if (!snapArray) {
      return [[], ""];
    }
    let selectedName = "";
    const items = snapArray.map(snapshotInfo => {
      const id = snapshotInfo.snapshot_id.toString();
      const time = TimestampToMoment(snapshotInfo.captured_at).format(
        "MMM D, YYYY [at] HH:mm:ss",
      );
      const out = {
        name: "Snapshot " + id + ": " + time,
        value: id,
      };
      if (id === snapshotIDStr) {
        selectedName = out.name;
      }
      return out;
    });
    return [items, selectedName];
  }, [snapArray, snapshotIDStr]);

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
      if (id === nodeIDStr) {
        selectedName = out.name;
      }
      return out;
    });
    return [items, selectedName];
  }, [nodes, nodeIDStr]);

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
              <Dropdown items={snapshotItems} onChange={onSnapshotSelected}>
                {snapshotName}
              </Dropdown>
            </PageConfigItem>
          )}
        </PageConfig>
      </div>
      <section className={cx("section")}>
        {snapshotIDStr ? (
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
