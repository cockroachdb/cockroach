// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { SortedTable } from "./";

storiesOf("Sorted table", module)
  .add("Empty state", () => <SortedTable empty />)
  .add("With data", () => {
    const columns = [
      {
        name: "Col 1",
        title: "Col 1",
        cell: (idx: number) => `row-${idx} col-1`,
      },
      {
        name: "Col 2",
        title: "Col 2",
        cell: (idx: number) => `row-${idx} col-2`,
      },
      {
        name: "Col 3",
        title: "Col 3",
        cell: (idx: number) => `row-${idx} col-3`,
      },
    ];
    return <SortedTable columns={columns} data={[1, 2, 3]} />;
  });
