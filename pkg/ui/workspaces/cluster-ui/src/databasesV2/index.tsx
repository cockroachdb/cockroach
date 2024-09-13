// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useMemo, useState } from "react";
import Select, { OptionsType } from "react-select";

import { PageLayout, PageSection } from "src/layouts";
import { PageConfig, PageConfigItem } from "src/pageConfig";
import PageCount from "src/sharedFromCloud/pageCount";
import { PageHeader } from "src/sharedFromCloud/pageHeader";
import { Search } from "src/sharedFromCloud/search";
import { ReactSelectOption } from "src/types/selectTypes";

import { DatabasesTable } from "./databasesTable";
import { DatabaseRow } from "./databaseTypes";
import { filterDatabases } from "./utils";

const mockRegionOptions = [
  { label: "US East (N. Virginia)", value: "us-east-1" },
  { label: "US East (Ohio)", value: "us-east-2" },
];

const mockData: DatabaseRow[] = new Array(20).fill(1).map((_, i) => ({
  name: `myDB-${i}`,
  id: i,
  approximateDiskSizeMiB: i * 100,
  tableCount: i,
  rangeCount: i,
  nodesByRegion:
    i % 2 === 0
      ? {
          [mockRegionOptions[0].value]: [1, 2],
          [mockRegionOptions[1].value]: [3],
        }
      : null,
  schemaInsightsCount: i,
}));

export const DatabasesPageV2 = () => {
  const data = mockData;
  const [search, setSearch] = useState<string>("");
  const [nodeRegions, setNodeRegions] = useState<ReactSelectOption[]>([]);
  const onNodeRegionsChange = (selected: OptionsType<ReactSelectOption>) => {
    setNodeRegions((selected ?? []).map(v => v));
  };
  const filteredData = useMemo(() => {
    return filterDatabases(data, {
      search,
      regionValues: nodeRegions.map(v => v.value),
    });
  }, [data, search, nodeRegions]);

  return (
    <PageLayout>
      <PageHeader title="Databases" />
      <PageSection>
        <PageConfig>
          <PageConfigItem>
            <Search placeholder="Search databases" onSubmit={setSearch} />
          </PageConfigItem>
          <PageConfigItem minWidth={"200px"}>
            <Select
              placeholder={"Regions"}
              name="nodeRegions"
              options={mockRegionOptions}
              clearable={true}
              isMulti
              value={nodeRegions}
              onChange={onNodeRegionsChange}
            />
          </PageConfigItem>
        </PageConfig>
      </PageSection>
      <PageSection>
        <PageCount
          page={1}
          pageSize={10}
          total={filteredData.length}
          entity="databases"
        />
        <DatabasesTable data={filteredData} />
      </PageSection>
    </PageLayout>
  );
};
