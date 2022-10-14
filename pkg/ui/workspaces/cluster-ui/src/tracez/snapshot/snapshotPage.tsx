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
  snapshotsError?: Error;
  snapshotsLoading: boolean;
  snapshots?: ListTracingSnapshotsResponseMessage;
  snapshot: GetTracingSnapshotResponseMessage;
  snapshotError?: Error;
  snapshotLoading: boolean;
}

export interface SnapshotPageDispatchProps {
  setSort: (value: SortSetting) => void;
  refreshSnapshots: () => void;
  refreshSnapshot: (id: number) => void;
}

type UrlParams = Partial<Record<"ascending" | "columnTitle", string>>;
export type SnapshotPageProps = SnapshotPageStateProps &
  SnapshotPageDispatchProps &
  RouteComponentProps<UrlParams>;

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

  // Sort Settings.
  const ascending = match.params.ascending === "true";
  const columnTitle = match.params.columnTitle || "";

  const snapshotIDStr = getMatchParamByName(match, "snapshotID");

  useEffect(() => {
    refreshSnapshots();
  }, [refreshSnapshots]);

  const snapArray = snapshots?.snapshots;

  useEffect(() => {
    if (snapshotIDStr || !snapArray) {
      return;
    }
    const lastSnapshotID = snapArray[snapArray.length - 1].snapshot_id;
    history.location.pathname = "/debug/tracez_v2/snapshot/" + lastSnapshotID;
    history.replace(history.location);
  }, [snapArray, snapshotIDStr, history]);

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
              {snapshotName}
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
