// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";
import React, { useCallback, useEffect, useState } from "react";
import {
  RequestState,
  ListJobProfilerExecutionDetailsResponse,
  ListJobProfilerExecutionDetailsRequest,
} from "src/api";
import { InlineAlert } from "@cockroachlabs/ui-components";
import { Row, Col } from "antd";
import "antd/lib/col/style";
import "antd/lib/row/style";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import classNames from "classnames";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import long from "long";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import classnames from "classnames/bind";
import styles from "./jobProfilerView.module.scss";
import { EmptyTable } from "src/empty";
import { useScheduleFunction } from "src/util/hooks";

const cardCx = classNames.bind(summaryCardStyles);
const cx = classnames.bind(styles);

export type JobProfilerStateProps = {
  jobID: long;
  executionDetailsResponse: RequestState<ListJobProfilerExecutionDetailsResponse>;
  lastUpdated: moment.Moment;
  isDataValid: boolean;
};

export type JobProfilerDispatchProps = {
  refreshExecutionDetails: (
    req: ListJobProfilerExecutionDetailsRequest,
  ) => void;
};

export type JobProfilerViewProps = JobProfilerStateProps &
  JobProfilerDispatchProps;

export function makeJobProfilerViewColumns(): ColumnDescriptor<string>[] {
  return [
    {
      name: "executionDetailFiles",
      title: "Execution Detail Files",
      hideTitleUnderline: true,
      cell: (executionDetails: string) => executionDetails,
    },
  ];
}

export const JobProfilerView: React.FC<JobProfilerViewProps> = ({
  jobID,
  executionDetailsResponse,
  lastUpdated,
  isDataValid,
  refreshExecutionDetails,
}: JobProfilerViewProps) => {
  const columns = makeJobProfilerViewColumns();
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "executionDetailFiles",
  });
  const req =
    new cockroach.server.serverpb.ListJobProfilerExecutionDetailsRequest({
      job_id: jobID,
    });
  const refresh = useCallback(() => {
    refreshExecutionDetails(req);
  }, [refreshExecutionDetails, req]);
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
  const summaryCardStylesCx = classNames.bind(summaryCardStyles);
  return (
    <div>
      <Row gutter={24}>
        <Col className="gutter-row" span={24}>
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
        </Col>
      </Row>
      <>
        <p className={summaryCardStylesCx("summary--card__divider--large")} />
        <Row gutter={24}>
          <Col className="gutter-row" span={24}>
            <SortedTable
              data={executionDetailsResponse.data?.files}
              columns={columns}
              tableWrapperClassName={cx("sorted-table")}
              sortSetting={sortSetting}
              onChangeSortSetting={onChangeSortSetting}
              renderNoResult={
                <EmptyTable title="No execution detail files found." />
              }
            />
          </Col>
        </Row>
      </>
    </div>
  );
};
