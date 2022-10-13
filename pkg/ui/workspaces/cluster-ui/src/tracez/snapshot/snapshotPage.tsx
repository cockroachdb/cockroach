// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect } from "react";
import { Helmet } from "react-helmet";
import { RouteComponentProps } from "react-router-dom";
import {
  ListTracingSnapshotsResponseMessage,
  GetTracingSnapshotResponseMessage,
} from "src/api/tracezApi";
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
  snapshotsError: Error | null;
  snapshotsLoading: boolean;
  snapshots: ListTracingSnapshotsResponseMessage;
  snapshot: GetTracingSnapshotResponseMessage;
  snapshotError: Error | null;
  snapshotLoading: boolean;
}

export interface SnapshotPageDispatchProps {
  setSort: (value: SortSetting) => void;
  refreshSnapshots: () => void;
  refreshSnapshot: (id: number) => void;
}

export type SnapshotPageProps = SnapshotPageStateProps &
  SnapshotPageDispatchProps &
  RouteComponentProps;

export const SnapshotPage: React.FC<SnapshotPageProps> = props => {
  const {
    history,
    match,
    refreshSnapshots,
    refreshSnapshot,
    snapshots,
    snapshot,
    sort,
    setSort,
  } = props;

  const searchParams = new URLSearchParams(history.location.search);

  // Sort Settings.
  const ascending = (searchParams.get("ascending") || undefined) === "true";
  const columnTitle = searchParams.get("columnTitle") || "";

  const snapshotIDStr = getMatchParamByName(match, "snapshotID");

  useEffect(() => {
    refreshSnapshots();
  });

  useEffect(() => {
    if (snapshotIDStr || !snapshots) {
      return;
    }
    const snapArray = snapshots.snapshots;
    const lastSnapshotID = snapArray[snapArray.length - 1].snapshot_id;
    history.location.pathname = "/debug/tracez_v2/snapshot/" + lastSnapshotID;
    history.replace(history.location);
  }, [snapshots, snapshotIDStr, history]);

  useEffect(() => {
    if (!columnTitle) {
      return;
    }
    setSort({ columnTitle, ascending });
  }, [setSort, columnTitle, ascending]);

  useEffect(() => {
    if (!snapshotIDStr) {
      return;
    }
    refreshSnapshot(parseInt(snapshotIDStr));
  }, [snapshotIDStr, refreshSnapshot]);

  const onSnapshotSelected = (item: string) => {
    history.location.pathname = "/debug/tracez_v2/snapshot/" + item;
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

  const snapshotItems = !snapshots
    ? []
    : snapshots.snapshots.map(snapshotInfo => {
        const id = snapshotInfo.snapshot_id.toString();
        const time = TimestampToMoment(snapshotInfo.captured_at).format(
          "MMM D, YYYY [at] HH:mm:ss",
        );
        return {
          name: "Snapshot " + id + ": " + time,
          value: id,
        };
      });

  const isLoading = props.snapshotsLoading || props.snapshotLoading;
  const error = props.snapshotsError || props.snapshotError;
  return (
    <div className={cx("snapshots-page")}>
      <Helmet title="Snapshots" />
      <h3 className={commonStyles("base-heading")}>Snapshots</h3>
      <div>
        <PageConfig>
          <PageConfigItem>
            <Dropdown items={snapshotItems} onChange={onSnapshotSelected}>
              {snapshotIDStr &&
                snapshotItems.length &&
                snapshotItems.find(option => option["value"] === snapshotIDStr)[
                  "name"
                ]}
            </Dropdown>
          </PageConfigItem>
        </PageConfig>
      </div>
      <section className={cx("section")}>
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
      </section>
    </div>
  );
};
