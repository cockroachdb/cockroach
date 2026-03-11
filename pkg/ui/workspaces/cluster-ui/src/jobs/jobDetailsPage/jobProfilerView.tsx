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
import React, { useCallback, useState } from "react";

import {
  collectExecutionDetails,
  getExecutionDetailFile,
  listExecutionDetailFiles,
} from "src/api/jobProfilerApi";
import { DownloadFile, DownloadFileRef } from "src/downloadFile";
import { EmptyTable } from "src/empty";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import {
  useSwrMutationWithClusterId,
  useSwrWithClusterId,
} from "src/util/hooks";

import { ExecutionDetailViewer } from "./executionDetailViewer";
import styles from "./jobProfilerView.module.scss";

const cardCx = classNames.bind(summaryCardStyles);
const cx = classnames.bind(styles);

export interface JobProfilerViewProps {
  jobID: long;
}

function extractFileExtension(filename: string): string {
  const parts = filename.split(".");
  // The extension is the last part after the last dot (if it exists).
  return parts.length > 1 ? parts[parts.length - 1] : "";
}

function getContentTypeForFile(filename: string): string {
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

// Files with these extensions can be rendered inline in an iframe.
const VIEWABLE_EXTENSIONS = new Set(["html", "txt"]);

function isViewableFile(filename: string): boolean {
  return VIEWABLE_EXTENSIONS.has(extractFileExtension(filename));
}

export function JobProfilerView({
  jobID,
}: JobProfilerViewProps): React.ReactElement {
  const { data: detailFiles, mutate: refreshFiles } = useSwrWithClusterId(
    { name: "jobProfilerExecutionFiles", jobID },
    () => listExecutionDetailFiles({ job_id: jobID }),
    {
      refreshInterval: 10 * 1000, // 10s polling interval
    },
  );
  const { trigger } = useSwrMutationWithClusterId(
    { name: "collectExecutionDetails", jobID },
    async () => {
      const resp = await collectExecutionDetails({ job_id: jobID });
      if (resp.req_resp) {
        refreshFiles();
      }
    },
  );

  const [viewingFile, setViewingFile] = useState<string | null>(null);
  const [viewingBlobUrl, setViewingBlobUrl] = useState<string | null>(null);

  const closeViewer = useCallback(() => {
    if (viewingBlobUrl) {
      URL.revokeObjectURL(viewingBlobUrl);
    }
    setViewingFile(null);
    setViewingBlobUrl(null);
  }, [viewingBlobUrl]);

  const openViewer = useCallback(
    (filename: string) => {
      setViewingFile(filename);
      const req =
        new cockroach.server.serverpb.GetJobProfilerExecutionDetailRequest({
          job_id: jobID,
          filename: filename,
        });
      getExecutionDetailFile(req)
        .then(resp => {
          const contentType = getContentTypeForFile(filename);
          const blob = new Blob([resp?.data], { type: contentType });
          setViewingBlobUrl(URL.createObjectURL(blob));
        })
        .catch(() => {
          setViewingFile(null);
        });
    },
    [jobID],
  );

  const downloadRef: React.RefObject<DownloadFileRef> =
    React.createRef<DownloadFileRef>();

  const columns: ColumnDescriptor<string>[] = [
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
        const viewable = isViewableFile(executionDetailFile);
        return (
          <div className={cx("crl-job-profiler-view__actions-column")}>
            <DownloadFile ref={downloadRef} />
            {viewable && (
              <Button
                as="a"
                size="small"
                intent="tertiary"
                className={cx("view-execution-detail-button")}
                onClick={() => openViewer(executionDetailFile)}
              >
                <Icon iconName="Open" />
                View
              </Button>
            )}
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
                getExecutionDetailFile(req).then(resp => {
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

  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "executionDetailFiles",
  });
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
        <Button intent="secondary" onClick={trigger}>
          Request Execution Details
        </Button>
      </Space>
      <SortedTable
        data={detailFiles?.files}
        columns={columns}
        tableWrapperClassName={cx("sorted-table")}
        sortSetting={sortSetting}
        onChangeSortSetting={onChangeSortSetting}
        renderNoResult={<EmptyTable title="No execution detail files found." />}
      />
      {viewingFile && viewingBlobUrl && (
        <ExecutionDetailViewer
          filename={viewingFile}
          blobUrl={viewingBlobUrl}
          onClose={closeViewer}
        />
      )}
    </Space>
  );
}
