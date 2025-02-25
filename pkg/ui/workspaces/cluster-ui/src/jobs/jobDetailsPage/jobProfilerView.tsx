// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Button, InlineAlert, Icon } from "@cockroachlabs/ui-components";
import { Space } from "antd";
import classNames from "classnames";
import classnames from "classnames/bind";
import long from "long";
import moment from "moment-timezone";
import React, { useCallback, useEffect, useMemo, useState } from "react";

import { RequestState } from "src/api";
import {
  GetJobProfilerExecutionDetailRequest,
  GetJobProfilerExecutionDetailResponse,
  ListJobProfilerExecutionDetailsRequest,
  ListJobProfilerExecutionDetailsResponse,
} from "src/api/jobProfilerApi";
import { DownloadFile, DownloadFileRef } from "src/downloadFile";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { useScheduleFunction } from "src/util/hooks";

import styles from "./jobProfilerView.module.scss";

const cardCx = classNames.bind(summaryCardStyles);
const cx = classnames.bind(styles);

export type JobProfilerStateProps = {
  jobID: long;
  executionDetailFilesResponse: RequestState<ListJobProfilerExecutionDetailsResponse>;
  lastUpdated: moment.Moment;
  isDataValid: boolean;
  onDownloadExecutionFileClicked: (
    req: GetJobProfilerExecutionDetailRequest,
  ) => Promise<GetJobProfilerExecutionDetailResponse>;
};

export type JobProfilerDispatchProps = {
  refreshExecutionDetailFiles: (
    req: ListJobProfilerExecutionDetailsRequest,
  ) => void;
  onRequestExecutionDetails: (jobID: long) => void;
};

export type JobProfilerViewProps = JobProfilerStateProps &
  JobProfilerDispatchProps;

export function extractFileExtension(filename: string): string {
  const parts = filename.split(".");
  // The extension is the last part after the last dot (if it exists).
  return parts.length > 1 ? parts[parts.length - 1] : "";
}

export function getContentTypeForFile(filename: string): string {
  const extension = extractFileExtension(filename);
  switch (extension) {
    case "txt":
      return "text/plain";
    case "zip":
      return "application/zip";
    case "html":
      return "text/html";
    default:
      return "";
  }
}

export function makeJobProfilerViewColumns(
  jobID: Long,
  onDownloadExecutionFileClicked: (
    req: GetJobProfilerExecutionDetailRequest,
  ) => Promise<GetJobProfilerExecutionDetailResponse>,
): ColumnDescriptor<string>[] {
  const downloadRef: React.RefObject<DownloadFileRef> =
    React.createRef<DownloadFileRef>();
  return [
    {
      name: "executionDetailFiles",
      title: "Execution Detail Files",
      hideTitleUnderline: true,
      cell: (executionDetails: string) => executionDetails,
    },
    {
      name: "actions",
      title: "",
      hideTitleUnderline: true,
      className: cx("column-size-medium"),
      cell: (executionDetailFile: string) => {
        return (
          <div className={cx("crl-job-profiler-view__actions-column")}>
            <DownloadFile ref={downloadRef} />
            <Button
              as="a"
              size="small"
              intent="tertiary"
              className={cx("view-execution-detail-button")}
              onClick={() => {
                const req =
                  new cockroach.server.serverpb.GetJobProfilerExecutionDetailRequest(
                    {
                      job_id: jobID,
                      filename: executionDetailFile,
                    },
                  );
                onDownloadExecutionFileClicked(req).then(resp => {
                  const type = getContentTypeForFile(executionDetailFile);
                  const executionFileBytes = new Blob([resp?.data], {
                    type: type,
                  });
                  const url = URL.createObjectURL(executionFileBytes);
                  window.open(url, "_blank");
                });
              }}
            >
              <Icon iconName="Open" />
              View
            </Button>
            <Button
              as="a"
              size="small"
              intent="tertiary"
              className={cx("download-execution-detail-button")}
              onClick={() => {
                const req =
                  new cockroach.server.serverpb.GetJobProfilerExecutionDetailRequest(
                    {
                      job_id: jobID,
                      filename: executionDetailFile,
                    },
                  );
                onDownloadExecutionFileClicked(req).then(resp => {
                  const type = getContentTypeForFile(executionDetailFile);
                  const executionFileBytes = new Blob([resp?.data], {
                    type: type,
                  });
                  Promise.resolve().then(() => {
                    downloadRef.current.download(
                      executionDetailFile,
                      executionFileBytes,
                    );
                  });
                });
              }}
            >
              <Icon iconName="Download" />
              Download
            </Button>
          </div>
        );
      },
    },
  ];
}

export const JobProfilerView: React.FC<JobProfilerViewProps> = ({
  jobID,
  executionDetailFilesResponse,
  lastUpdated,
  isDataValid,
  onDownloadExecutionFileClicked,
  refreshExecutionDetailFiles,
  onRequestExecutionDetails,
}: JobProfilerViewProps) => {
  const columns = makeJobProfilerViewColumns(
    jobID,
    onDownloadExecutionFileClicked,
  );
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "executionDetailFiles",
  });
  const req = useMemo(
    () =>
      new cockroach.server.serverpb.ListJobProfilerExecutionDetailsRequest({
        job_id: jobID,
      }),
    [jobID],
  );
  const refresh = useCallback(() => {
    refreshExecutionDetailFiles(req);
  }, [refreshExecutionDetailFiles, req]);
  const [refetch] = useScheduleFunction(
    refresh,
    true,
    10 * 1000, // 10s polling interval
    lastUpdated,
  );
  useEffect(() => {
    if (!isDataValid) refetch();
  }, [isDataValid, refetch]);

  const onChangeSortSetting = (ss: SortSetting): void => {
    setSortSetting(ss);
  };

  // This URL results in a cluster-wide CPU profile to be collected for 5
  // seconds. We set `tagfocus` (tf) to only view the samples corresponding to
  // this job's execution.
  const url = `debug/pprof/ui/cpu?node=all&seconds=5&labels=true&tf=job.*${jobID}`;
  return (
    <Space direction="vertical" size="middle" className={cx("full-width")}>
      <SummaryCard className={cardCx("summary-card")}>
        <SummaryCardItem
          label="Cluster-wide CPU Profile"
          value={<a href={url}>Profile</a>}
        />
        <InlineAlert
          intent="warning"
          title="This operation buffers profiles in memory for all the nodes in the cluster and can result in increased memory usage."
        />
      </SummaryCard>
      <Space direction="vertical" align="end" className={cx("full-width")}>
        <Button
          intent="secondary"
          onClick={() => {
            onRequestExecutionDetails(jobID);
          }}
        >
          Request Execution Details
        </Button>
      </Space>
      <SortedTable
        data={executionDetailFilesResponse.data?.files}
        columns={columns}
        tableWrapperClassName={cx("sorted-table")}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
        renderNoResult={<EmptyTable title="No execution detail files found." />}
      />
    </Space>
  );
};
