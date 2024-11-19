// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { shallow } from "enzyme";
import React from "react";

import { allSchedulesFixture } from "./schedulesPage.fixture";
import { ScheduleTable, ScheduleTableProps } from "./scheduleTable";

describe("<ScheduleTable>", () => {
  it("should reset page to 1 after schedule list prop changes", () => {
    const scheduleTableProps: ScheduleTableProps = {
      sort: { columnTitle: null, ascending: true },
      setSort: () => {},
      schedules: allSchedulesFixture,
      current: 2,
      pageSize: 2,
      isUsedFilter: true,
    };
    const scheduleTable = shallow<ScheduleTable>(
      <ScheduleTable
        schedules={scheduleTableProps.schedules}
        sort={scheduleTableProps.sort}
        setSort={scheduleTableProps.setSort}
        current={scheduleTableProps.current}
        pageSize={scheduleTableProps.pageSize}
        isUsedFilter={scheduleTableProps.isUsedFilter}
      />,
    );
    expect(scheduleTable.state().pagination.current).toBe(2);
    scheduleTable.setProps({
      ...scheduleTableProps,
      schedules: [allSchedulesFixture[0]],
    });
    expect(scheduleTable.state().pagination.current).toBe(1);
  });
});
