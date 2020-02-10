// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { ReactWrapper } from "enzyme";
import { assert } from "chai";

import { NodeList, NodeStatusRow } from "./index";
import { AdminUIState} from "src/redux/state";
import { LocalSetting } from "src/redux/localsettings";
import { SortSetting } from "src/views/shared/components/sortabletable";
import { connectedMount } from "src/test-utils";

describe("Nodes Overview page", () => {
  describe("Live <NodeList/> section initial state", () => {
    const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
      "nodes/live_sort_setting", (s) => s.localSettings,
    );
    const nodesCount = 9;
    const regionsCount = 3;

    const dataSource: NodeStatusRow[] = [
      {
        "key": "us-east1",
        "region": "us-east1",
        "nodesCount": 3,
        "replicas": 224,
        "usedCapacity": 0,
        "availableCapacity": 1610612736,
        "usedMemory": 1904611328,
        "availableMemory": 51539607552,
        "status": 6,
        "children": [
          {
            "key": "us-east1-0",
            "nodeId": 1,
            "region": "us-east1",
            "uptime": "3 minutes",
            "replicas": 78,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 639758336,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "us-east1-1",
            "nodeId": 2,
            "region": "us-east1",
            "uptime": "3 minutes",
            "replicas": 74,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 631373824,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "us-east1-2",
            "nodeId": 3,
            "region": "us-east1",
            "uptime": "3 minutes",
            "replicas": 72,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 633479168,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
        ],
      },
      {
        "key": "us-west1",
        "region": "us-west1",
        "nodesCount": 3,
        "replicas": 229,
        "usedCapacity": 0,
        "availableCapacity": 1610612736,
        "usedMemory": 1913843712,
        "availableMemory": 51539607552,
        "status": 6,
        "children": [
          {
            "key": "us-west1-0",
            "nodeId": 4,
            "region": "us-west1",
            "uptime": "3 minutes",
            "replicas": 73,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 634728448,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "us-west1-1",
            "nodeId": 5,
            "region": "us-west1",
            "uptime": "3 minutes",
            "replicas": 78,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 638218240,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "us-west1-2",
            "nodeId": 6,
            "region": "us-west1",
            "uptime": "3 minutes",
            "replicas": 78,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 640897024,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
        ],
      },
      {
        "key": "europe-west1",
        "region": "europe-west1",
        "nodesCount": 3,
        "replicas": 216,
        "usedCapacity": 0,
        "availableCapacity": 1610612736,
        "usedMemory": 1924988928,
        "availableMemory": 51539607552,
        "status": 6,
        "children": [
          {
            "key": "europe-west1-0",
            "nodeId": 7,
            "region": "europe-west1",
            "uptime": "3 minutes",
            "replicas": 71,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 641097728,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "europe-west1-1",
            "nodeId": 8,
            "region": "europe-west1",
            "uptime": "3 minutes",
            "replicas": 74,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 641945600,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
          {
            "key": "europe-west1-2",
            "nodeId": 9,
            "region": "europe-west1",
            "uptime": "3 minutes",
            "replicas": 71,
            "usedCapacity": 0,
            "availableCapacity": 536870912,
            "usedMemory": 641945600,
            "availableMemory": 17179869184,
            "version": "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            "status": 3,
          },
        ],
      },
    ];

    it("displays correct header of Nodes section with total number of nodes", () => {
      const wrapper: ReactWrapper = connectedMount(store => (
        <NodeList
          dataSource={dataSource}
          nodesCount={nodesCount}
          regionsCount={regionsCount}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />));
      assert.equal(wrapper.find("h3.text.text--heading-3").text(), `Nodes (${nodesCount})`);
    });

    it("displays table with required columns when nodes partitioned by locality", () => {
      const wrapper: ReactWrapper = connectedMount(store => (
        <NodeList
          dataSource={dataSource}
          nodesCount={nodesCount}
          regionsCount={regionsCount}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />));
      const expectedColumns = [
        "nodes",
        "# of nodes",
        "uptime",
        "replicas",
        "capacity use",
        "memory use",
        "version",
        "status",
        "", // logs column doesn't have header text
      ];
      const columnCells = wrapper.find(".table-section__content table thead th");
      assert.equal(columnCells.length, expectedColumns.length);

      expectedColumns.forEach(
        (columnName, idx) => assert.equal(columnCells.at(idx).text(), columnName));
    });

    it("doesn't display '# of nodes column' when nodes are in single regions", () => {
      const expectedColumns = [
        "nodes",
        // should not be displayed "# of nodes",
        "uptime",
        "replicas",
        "capacity use",
        "memory use",
        "version",
        "status",
        "", // logs column doesn't have header text
      ];
      const singleRegionDataSource = dataSource[0];
      const wrapper = connectedMount(store => (
        <NodeList
          dataSource={[singleRegionDataSource]}
          nodesCount={singleRegionDataSource.children.length}
          regionsCount={1}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />));
      const columnCells = wrapper.find(".table-section__content table thead th");
      assert.equal(columnCells.length, expectedColumns.length);
      expectedColumns.forEach(
        (columnName, idx) => assert.equal(columnCells.at(idx).text(), columnName));
    });

    it("displays table with fixed column width", () => {
      const wrapper: ReactWrapper = connectedMount(store => (
        <NodeList
          dataSource={dataSource}
          nodesCount={nodesCount}
          regionsCount={regionsCount}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />));
      const columnAttributes = wrapper.find("table colgroup col");
      columnAttributes.forEach(node => assert.exists(node.hostNodes().props().style.width));
    });
  });
});
