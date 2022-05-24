// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useState } from "react";
import { storiesOf } from "@storybook/react";

import { SortedTable, SortSetting } from "./";

export function SortedTableWrapper(): React.ReactElement {
  const [sortSetting, setSortSetting] = useState<SortSetting>({
    ascending: true,
    columnTitle: "col1",
  });
  const columns = [
    {
      name: "col1",
      title: "Col 1",
      cell: (idx: number) => `Col 1: row-${idx}`,
      sort: (idx: number) => idx,
    },
    {
      name: "col2",
      title: "Col 2",
      cell: (idx: number) => `Col 2: row-${idx}`,
      sort: (idx: number) => idx,
    },
    {
      name: "col3",
      title: "Col 3",
      cell: (idx: number) => `Col 3: row-${idx}`,
      sort: (idx: number) => idx,
    },
  ];
  return (
    <SortedTable
      columns={columns}
      data={[1, 2, 3]}
      sortSetting={sortSetting}
      onChangeSortSetting={setSortSetting}
    />
  );
}

storiesOf("Sorted table", module)
  .add("Empty state", () => <SortedTable empty />)
  .add("No sort", () => {
    const columns = [
      {
        name: "col1",
        title: "Col 1",
        cell: (idx: number) => `Col 1: row-${idx}`,
      },
      {
        name: "col2",
        title: "Col 2",
        cell: (idx: number) => `Col 2: row-${idx}`,
      },
      {
        name: "col3",
        title: "Col 3",
        cell: (idx: number) => `Col 3: row-${idx}`,
      },
    ];
    return <SortedTable columns={columns} data={[1, 2, 3]} />;
  })
  .add("With sort", () => <SortedTableWrapper />);
