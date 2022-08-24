// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React from "react";
import { shallow } from "enzyme";
import { earliestRetainedTime } from "./jobsPage.fixture";
import { JobTable, JobTableProps } from "./jobTable";

describe("<JobTable>", () => {
  it("should reset page to 1 after job list prop changes", () => {
    const toJSON = () => {
      return [""];
    };
    const jobTableProps: JobTableProps = {
      sort: { columnTitle: null, ascending: true },
      setSort: () => {},
      jobs: {
        jobs: [{}, {}, {}, {}],
        earliest_retained_time: earliestRetainedTime,
        toJSON: toJSON,
      },
      current: 2,
      pageSize: 2,
      isUsedFilter: true,
    };
    const jobTable = shallow<JobTable>(
      <JobTable
        jobs={jobTableProps.jobs}
        sort={jobTableProps.sort}
        setSort={jobTableProps.setSort}
        current={jobTableProps.current}
        pageSize={jobTableProps.pageSize}
        isUsedFilter={jobTableProps.isUsedFilter}
      />,
    );
    expect(jobTable.state().pagination.current).toBe(2);
    jobTable.setProps({
      jobs: {
        jobs: [{}, {}],
        earliest_retained_time: earliestRetainedTime,
        toJSON: toJSON,
      },
    });
    expect(jobTable.state().pagination.current).toBe(1);
  });
});
