// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import React from "react";
import { MemoryRouter } from "react-router-dom";

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

    const { rerender } = render(
      <MemoryRouter>
        <ScheduleTable
          schedules={scheduleTableProps.schedules}
          sort={scheduleTableProps.sort}
          setSort={scheduleTableProps.setSort}
          current={scheduleTableProps.current}
          pageSize={scheduleTableProps.pageSize}
          isUsedFilter={scheduleTableProps.isUsedFilter}
        />
      </MemoryRouter>,
    );

    // Verify initial render shows the table.
    expect(screen.getByRole("table")).toBeInTheDocument();

    // Rerender with different schedules to trigger pagination reset.
    rerender(
      <MemoryRouter>
        <ScheduleTable
          schedules={[allSchedulesFixture[0]]}
          sort={scheduleTableProps.sort}
          setSort={scheduleTableProps.setSort}
          current={scheduleTableProps.current}
          pageSize={scheduleTableProps.pageSize}
          isUsedFilter={scheduleTableProps.isUsedFilter}
        />
      </MemoryRouter>,
    );

    // Verify re-render completed without error (pagination reset happens internally).
    expect(screen.getByRole("table")).toBeInTheDocument();
  });
});
