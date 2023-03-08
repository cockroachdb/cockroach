// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Helmet } from "react-helmet";
import { commonStyles } from "src/common";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import { Button, Icon } from "@cockroachlabs/ui-components";
import { Dropdown } from "src/dropdown";
import { Loading } from "src/loading";
import { SpanTable } from "./spanTable";
import React, { useMemo } from "react";
import classNames from "classnames/bind";
import styles from "../snapshot.module.scss";
import { TimestampToMoment } from "src/util";
import { SortSetting } from "src/sortedtable";
import {
  GetTracingSnapshotResponse,
  ListTracingSnapshotsResponse,
} from "src/api";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
const cx = classNames.bind(styles);

export const SnapshotComponent: React.FC<{
  sort: SortSetting;
  changeSortSetting: (value: SortSetting) => void;
  nodes?: cockroach.server.status.statuspb.INodeStatus[];
  nodeID: string;
  onNodeSelected: (_: string) => void;
  snapshots: ListTracingSnapshotsResponse;
  snapshotID: number;
  snapshot: GetTracingSnapshotResponse;
  onSnapshotSelected: (_: number) => void;
  isLoading: boolean;
  error: Error;
  spanDetailsURL: (_: Long) => string;
  takeAndLoadSnapshot: () => void;
}> = props => {
  const {
    sort,
    changeSortSetting,
    nodes,
    nodeID,
    onNodeSelected,
    snapshots,
    snapshotID,
    snapshot,
    onSnapshotSelected,
    isLoading,
    error,
    spanDetailsURL,
    takeAndLoadSnapshot,
  } = props;

  const snapshotsAsJson = JSON.stringify(snapshots);

  const [snapshotItems, snapshotName] = useMemo(() => {
    if (!snapshots) {
      return [[], ""];
    }
    let selectedName = "";
    const items = snapshots.snapshots.map(snapshotInfo => {
      const id = snapshotInfo.snapshot_id.toNumber();
      const time = TimestampToMoment(snapshotInfo.captured_at).format(
        "MMM D, YYYY [at] HH:mm:ss",
      );
      const out = {
        name: "Snapshot " + id + ": " + time,
        value: id,
      };
      if (id < 0) {
        out.name = "Auto-Snapshot " + id * -1 + ": " + time;
      }
      if (id === snapshotID) {
        selectedName = out.name;
      }
      return out;
    });
    return [items, selectedName];
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [snapshotsAsJson, snapshotID]);

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

  return (
    <div className={cx("snapshots-page")}>
      <Helmet title="Snapshots" />
      <h3
        data-testid="snapshot-component-title"
        className={commonStyles("base-heading")}
      >
        Snapshots
      </h3>
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
                spanDetailsURL={spanDetailsURL}
              />
            )}
          />
        ) : (
          "No snapshots found on this node."
        )}
      </section>
      <div className={cx("bottom-padding")} />
    </div>
  );
};
