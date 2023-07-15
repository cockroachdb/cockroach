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
import React from "react";
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

const cardCx = classNames.bind(summaryCardStyles);
const cx = classnames.bind(styles);

export interface JobProfilerStateProps {
  jobID: long;
  executionDetailsResponse: RequestState<ListJobProfilerExecutionDetailsResponse>;
}

export interface JobProfilerDispatchProps {
  refreshExecutionDetails: (
    req: ListJobProfilerExecutionDetailsRequest,
  ) => void;
}

export type JobProfilerViewProps = JobProfilerStateProps &
  JobProfilerDispatchProps;

interface JobProfilerViewState {
  sortSetting: SortSetting;
}

export class JobProfilerView extends React.Component<
  JobProfilerViewProps,
  JobProfilerViewState
> {
  refreshDataInterval: NodeJS.Timeout;

  constructor(props: JobProfilerViewProps) {
    super(props);
    this.state = {
      sortSetting: {
        ascending: true,
        columnTitle: "executionDetails",
      },
    };
  }

  scheduleFetch(jobID: long): void {
    clearTimeout(this.refreshDataInterval);
    const now = moment.utc();
    const nextRefresh =
      !this.props.executionDetailsResponse?.valid &&
      !this.props.executionDetailsResponse?.error
        ? now
        : this.props.executionDetailsResponse.lastUpdated
            ?.clone()
            .add(10, "seconds") ?? now;
    const msToNextRefresh = Math.max(0, nextRefresh.diff(now, "millisecond"));
    this.refreshDataInterval = setTimeout(() => {
      const req =
        new cockroach.server.serverpb.ListJobProfilerExecutionDetailsRequest({
          job_id: jobID,
        });
      this.props.refreshExecutionDetails(req);
    }, msToNextRefresh);
  }

  componentDidMount(): void {
    this.scheduleFetch(this.props.jobID);
  }

  componentDidUpdate(prevProps: JobProfilerViewProps): void {
    if (
      prevProps.executionDetailsResponse.lastUpdated !==
      this.props.executionDetailsResponse.lastUpdated
    ) {
      this.scheduleFetch(this.props.jobID);
    }
  }

  componentWillUnmount(): void {
    clearTimeout(this.refreshDataInterval);
  }

  onSortingChange = (ss: SortSetting): void => {
    this.setState({
      sortSetting: {
        ascending: ss.ascending,
        columnTitle: ss.columnTitle,
      },
    });
  };

  columns: ColumnDescriptor<string>[] = [
    {
      name: "executionDetails",
      title: "Execution Details",
      hideTitleUnderline: true,
      cell: (executionDetails: string) => <div>{executionDetails}</div>,
    },
  ];

  render(): React.ReactElement {
    const { jobID, executionDetailsResponse } = this.props;

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
                columns={this.columns}
                className={cx("execution-details-table")}
                tableWrapperClassName={cx("sorted-table")}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.onSortingChange}
                renderNoResult={
                  <EmptyTable title="No execution details found." />
                }
              />
            </Col>
          </Row>
        </>
      </div>
    );
  }
}
