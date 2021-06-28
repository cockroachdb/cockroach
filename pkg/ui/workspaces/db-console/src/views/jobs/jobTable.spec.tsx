// Copyright 2018 The Cockroach Authors.
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
import { assert } from "chai";
import { JobTable, JobTableProps } from "src/views/jobs/jobTable";

import "src/enzymeInit";

describe("<JobTable>", () => {
  it("should reset page to 1 after job list prop changes", () => {
    const toJSON = () => {
      return [""];
    };
    const jobTableProps: JobTableProps = {
      sort: { sortKey: null, ascending: true },
      setSort: () => {},
      jobs: {
        data: { jobs: [{}, {}, {}, {}], toJSON },
        inFlight: false,
        valid: true,
      },
      current: 2,
      pageSize: 2,
      isUsedFilter: true,
    };
    const jobTable = shallow(
      <JobTable
        jobs={jobTableProps.jobs}
        sort={jobTableProps.sort}
        setSort={jobTableProps.setSort}
        current={jobTableProps.current}
        pageSize={jobTableProps.pageSize}
        isUsedFilter={jobTableProps.isUsedFilter}
      />,
    );
    assert.equal(jobTable.state().pagination.current, 2);
    jobTable.setProps({ jobs: { data: { jobs: [{}, {}], toJSON } } });
    assert.equal(jobTable.state().pagination.current, 1);
  });
});
