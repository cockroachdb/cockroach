// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { ArrowLeft } from "@cockroachlabs/icons";
import { Col, Row, Tabs } from "antd";
import classNames from "classnames/bind";
import long from "long";
import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { useJobDetails } from "src/api/jobsApi";
import { useUserSQLRoles } from "src/api/userApi";
import { Button } from "src/button";
import { commonStyles } from "src/common";
import { CockroachCloudContext } from "src/contexts";
import jobStyles from "src/jobs/jobs.module.scss";
import { Loading } from "src/loading";
import { SqlBox, SqlBoxSize } from "src/sql";
import summaryCardStyles from "src/summaryCard/summaryCard.module.scss";
import { getMatchParamByName, idAttr } from "src/util";

import {
  CollectExecutionDetailsRequest,
  CollectExecutionDetailsResponse,
  GetJobProfilerExecutionDetailRequest,
  GetJobProfilerExecutionDetailResponse,
  ListJobProfilerExecutionDetailsRequest,
  ListJobProfilerExecutionDetailsResponse,
} from "../../api";
import { isTerminalState } from "../util";

import { JobProfilerView } from "./jobProfilerView";
import OverviewTabContent from "./overviewTab";

const cardCx = classNames.bind(summaryCardStyles);
const jobCx = classNames.bind(jobStyles);

enum TabKeysEnum {
  OVERVIEW = "Overview",
  PROFILER = "Advanced Debugging",
}

export interface JobDetailsPropsV2 extends RouteComponentProps {
  onFetchExecutionDetailFiles: (
    req: ListJobProfilerExecutionDetailsRequest,
  ) => Promise<ListJobProfilerExecutionDetailsResponse>;
  onCollectExecutionDetails: (
    req: CollectExecutionDetailsRequest,
  ) => Promise<CollectExecutionDetailsResponse>;
  onDownloadExecutionFile: (
    req: GetJobProfilerExecutionDetailRequest,
  ) => Promise<GetJobProfilerExecutionDetailResponse>;
}

export function JobDetailsV2({
  history,
  match,
  onFetchExecutionDetailFiles,
  onCollectExecutionDetails,
  onDownloadExecutionFile,
}: JobDetailsPropsV2): React.ReactElement {
  const ccContext = useContext(CockroachCloudContext);
  const { data: userRoles } = useUserSQLRoles();
  const hasAdminRole = userRoles?.roles?.includes("ADMIN") ?? false;
  const jobId = useMemo(
    () => long.fromString(getMatchParamByName(match, idAttr)),
    [match],
  );
  const [jobTerminal, setJobTerminal] = useState(false);
  const {
    data: job,
    error,
    isLoading,
  } = useJobDetails(jobId, {
    refreshInterval: 10 * 1000,
    keepPreviousData: true,
    isPaused: () => jobTerminal,
  });
  const searchParams = useMemo(() => {
    return new URLSearchParams(history.location.search);
  }, [history.location.search]);
  const [currentTab, setCurrentTab] = React.useState<string>(
    searchParams.get("tab") || "overview",
  );
  const onTabChange = useCallback(
    (tabId: string): void => {
      searchParams.set("tab", tabId);
      setCurrentTab(tabId);
      history.replace({
        ...history.location,
        search: searchParams.toString(),
      });
    },
    [history, searchParams],
  );
  useEffect(() => {
    if (job) {
      setJobTerminal(isTerminalState(job.status));
    }
  }, [job]);

  return (
    <div className={jobCx("job-details")}>
      <Helmet title={"Details | Job"} />
      <div className={jobCx("section page--header")}>
        <Button
          onClick={() => history.goBack()}
          type="unstyled-link"
          size="small"
          icon={<ArrowLeft fontSize={"10px"} />}
          iconPosition="left"
          className={commonStyles("small-margin")}
        >
          Jobs
        </Button>
        <h3 className={jobCx("page--header__title")}>{`Job ID: ${jobId}`}</h3>
      </div>
      <section className={jobCx("section section--container")}>
        <Loading
          loading={isLoading}
          page={"job details"}
          error={error}
          render={() => (
            <>
              <section className={cardCx("summary-card")}>
                <Row gutter={24}>
                  <Col className="gutter-row" span={24}>
                    <SqlBox
                      value={job?.description ?? "Job not found."}
                      size={SqlBoxSize.CUSTOM}
                      format={true}
                    />
                  </Col>
                </Row>
              </section>
              <Tabs
                className={commonStyles("cockroach--tabs")}
                defaultActiveKey={TabKeysEnum.OVERVIEW}
                onChange={onTabChange}
                activeKey={currentTab}
                items={[
                  {
                    key: "overview",
                    label: TabKeysEnum.OVERVIEW,
                    children: <OverviewTabContent job={job} />,
                  },
                  !ccContext &&
                    hasAdminRole && {
                      key: "profiler",
                      label: TabKeysEnum.PROFILER,
                      children: (
                        <JobProfilerView
                          jobID={jobId}
                          onFetchExecutionDetailFiles={
                            onFetchExecutionDetailFiles
                          }
                          onCollectExecutionDetails={onCollectExecutionDetails}
                          onDownloadExecutionFile={onDownloadExecutionFile}
                        />
                      ),
                    },
                ]}
              />
            </>
          )}
        />
      </section>
    </div>
  );
}
