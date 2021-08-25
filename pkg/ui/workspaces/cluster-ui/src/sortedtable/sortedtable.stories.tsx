// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";

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
