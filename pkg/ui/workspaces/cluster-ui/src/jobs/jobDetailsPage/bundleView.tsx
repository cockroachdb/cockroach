// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classnames from "classnames/bind";
import { Button, Icon } from "@cockroachlabs/ui-components";
import styles from "./bundleView.module.scss";
import { ColumnDescriptor, SortedTable, SortSetting } from "src/sortedtable";
import { DATE_FORMAT_24_TZ } from "../../util";
import { Timestamp } from "../../timestamp";
import moment from "moment-timezone";
import {
  GetJobProfilerBundleResponse,
  InsertJobProfilerBundleRequest,
  JobProfilerBundle,
} from "src/api/jobProfilerBundleApi";
import { DownloadFile, DownloadFileRef } from "src/downloadFile";
import Long from "long";

export interface BundleViewStateProps {
  bundles: JobProfilerBundle[];
  downloadBundle: (
    jobID: string,
    bundleID: string,
  ) => Promise<GetJobProfilerBundleResponse>;
}

export interface BundleViewDispatchProps {
  onCollectJobProfilerBundle: (
    insertJobProfilerBundleRequest: InsertJobProfilerBundleRequest,
  ) => void;
}

export interface BundleViewOwnProps {
  jobID?: string;
}

export type BundleViewProps = BundleViewOwnProps &
  BundleViewStateProps &
  BundleViewDispatchProps;

interface BundleViewState {
  sortSetting: SortSetting;
  downloadRef: React.RefObject<DownloadFileRef>;
}

const cx = classnames.bind(styles);

export class BundleView extends React.Component<
  BundleViewProps,
  BundleViewState
> {
  constructor(props: BundleViewProps) {
    super(props);
    this.state = {
      sortSetting: {
        ascending: true,
        columnTitle: "activatedOn",
      },
      downloadRef: React.createRef<DownloadFileRef>(),
    };
  }

  columns: ColumnDescriptor<JobProfilerBundle>[] = [
    {
      name: "activatedOn",
      title: "Collected at",
      hideTitleUnderline: true,
      cell: (bundle: JobProfilerBundle) => (
        <Timestamp time={bundle.collected_at} format={DATE_FORMAT_24_TZ} />
      ),
      sort: (bundle: JobProfilerBundle) => moment(bundle.collected_at)?.unix(),
    },

    {
      name: "actions",
      title: "",
      hideTitleUnderline: true,
      className: cx("column-size-medium"),
      cell: (bundle: JobProfilerBundle) => {
        return (
          <div className={cx("crl-jobs-bundles-view__actions-column")}>
            <DownloadFile ref={this.state.downloadRef} />
            <Button
              as="a"
              size="small"
              intent="tertiary"
              className={cx("download-bundle-button")}
              onClick={() => {
                this.fetchData(bundle);
              }}
            >
              <Icon iconName="Download" />
              Bundle (.zip)
            </Button>
          </div>
        );
      },
    },
  ];

  fetchData = (bundle: JobProfilerBundle) => {
    this.props.downloadBundle(bundle.job_id, bundle.bundle_id).then(resp => {
      const bundleBytes = new Blob([resp.bundle], { type: "application/zip" });
      Promise.resolve().then(() => {
        this.state.downloadRef.current.download(
          bundle.job_id + "-" + bundle.bundle_id + "-" + "-bundle.zip",
          bundleBytes,
        );
      });
    });
  };

  render(): React.ReactElement {
    const { jobID, bundles } = this.props;

    return (
      <>
        <div className={cx("crl-jobs-bundles-view__header")}>
          {
            <Button
              onClick={() =>
                this.props.onCollectJobProfilerBundle({ jobID: jobID })
              }
              intent="secondary"
            >
              Collect bundle
            </Button>
          }
        </div>
        <SortedTable
          data={bundles}
          columns={this.columns}
          className={cx("jobs-table")}
          sortSetting={this.state.sortSetting}
          tableWrapperClassName={cx("sorted-table")}
        />
      </>
    );
  }
}
