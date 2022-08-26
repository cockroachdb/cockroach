// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// All changes made on this file, should also be done on the equivalent
// file on managed-service repo.

import React from "react";
import Helmet from "react-helmet";
import { Tabs } from "antd";
import "antd/lib/tabs/style";
import JobsPageConnected from "src/views/jobs/jobsPage";
import SchedulesPageConnected from "src/views/schedules/schedulesPage";
import { commonStyles, util } from "@cockroachlabs/cluster-ui";
import { RouteComponentProps } from "react-router-dom";
import { tabAttr } from "src/util/constants";

const { TabPane } = Tabs;

export enum JobsTabType {
  Jobs = "Jobs",
  Schedules = "Schedules",
}

export const JOBS_DEFAULT_TAB: JobsTabType = JobsTabType.Jobs;

const JobsAndSchedulesPage = (props: RouteComponentProps) => {
  const currentTab =
    util.queryByName(props.location, tabAttr) || JobsTabType.Jobs;

  const onTabChange = (tabId: string): void => {
    const params = new URLSearchParams({ tab: tabId });
    props.history.push({
      search: params.toString(),
    });
  };

  return (
    <div>
      <Helmet title={currentTab} />
      <Tabs
        defaultActiveKey={JOBS_DEFAULT_TAB}
        className={commonStyles("cockroach--tabs")}
        onChange={onTabChange}
        activeKey={currentTab}
        destroyInactiveTabPane
      >
        <TabPane tab="Jobs" key="Jobs">
          <JobsPageConnected />
        </TabPane>
        <TabPane tab="Schedules" key="Schedules">
          <SchedulesPageConnected />
        </TabPane>
      </Tabs>
    </div>
  );
};

export default JobsAndSchedulesPage;
