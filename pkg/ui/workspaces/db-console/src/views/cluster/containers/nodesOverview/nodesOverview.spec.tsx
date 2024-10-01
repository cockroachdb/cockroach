// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SortSetting } from "@cockroachlabs/cluster-ui";
import { ReactWrapper } from "enzyme";
import times from "lodash/times";
import Long from "long";
import React from "react";

import { cockroach } from "src/js/protos";
import { LocalSetting } from "src/redux/localsettings";
import { livenessByNodeIDSelector, LivenessStatus } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";
import { connectedMount } from "src/test-utils";

import {
  decommissionedNodesTableDataSelector,
  getLivenessStatusName,
  liveNodesTableDataSelector,
  NodeList,
  NodeStatusRow,
} from "./index";

import NodeLivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;

describe("Nodes Overview page", () => {
  describe("Live <NodeList/> section initial state", () => {
    const sortSetting = new LocalSetting<AdminUIState, SortSetting>(
      "nodes/live_sort_setting",
      s => s.localSettings,
    );
    const nodesCount = 9;
    const regionsCount = 3;

    const dataSource: NodeStatusRow[] = [
      {
        key: "us-east1",
        region: "us-east1",
        tiers: [
          { key: "region", value: "us-west" },
          { key: "az", value: "us-west-01" },
        ],
        nodesCount: 3,
        replicas: 224,
        usedCapacity: 0,
        availableCapacity: 1610612736,
        usedMemory: 1904611328,
        numCpus: 48,
        availableMemory: 51539607552,
        status: 6,
        children: [
          {
            key: "us-east1-0",
            nodeId: 1,
            nodeName: "127.0.0.1:50945",
            uptime: "3 minutes",
            replicas: 78,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 639758336,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "us-east1-1",
            nodeId: 2,
            nodeName: "127.0.0.2:50945",
            uptime: "3 minutes",
            replicas: 74,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 631373824,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "us-east1-2",
            nodeId: 3,
            nodeName: "127.0.0.3:50945",
            uptime: "3 minutes",
            replicas: 72,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 633479168,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
        ],
      },
      {
        key: "us-west1",
        region: "us-west1",
        tiers: [
          { key: "region", value: "us-west" },
          { key: "az", value: "us-west-01" },
        ],
        nodesCount: 3,
        replicas: 229,
        usedCapacity: 0,
        availableCapacity: 1610612736,
        usedMemory: 1913843712,
        numCpus: 48,
        availableMemory: 51539607552,
        status: 6,
        children: [
          {
            key: "us-west1-0",
            nodeId: 4,
            nodeName: "127.0.0.4:50945",
            uptime: "3 minutes",
            replicas: 73,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 634728448,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "us-west1-1",
            nodeId: 5,
            nodeName: "127.0.0.5:50945",
            uptime: "3 minutes",
            replicas: 78,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 638218240,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "us-west1-2",
            nodeId: 6,
            nodeName: "127.0.0.6:50945",
            uptime: "3 minutes",
            replicas: 78,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 640897024,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
        ],
      },
      {
        key: "europe-west1",
        region: "europe-west1",
        tiers: [
          { key: "region", value: "europe-west1" },
          { key: "az", value: "us-west-01" },
        ],
        nodesCount: 3,
        replicas: 216,
        usedCapacity: 0,
        availableCapacity: 1610612736,
        usedMemory: 1924988928,
        numCpus: 48,
        availableMemory: 51539607552,
        status: 6,
        children: [
          {
            key: "europe-west1-0",
            nodeId: 7,
            nodeName: "127.0.0.7:50945",
            uptime: "3 minutes",
            replicas: 71,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 641097728,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "europe-west1-1",
            nodeId: 8,
            nodeName: "127.0.0.8:50945",
            uptime: "3 minutes",
            replicas: 74,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 641945600,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
          },
          {
            key: "europe-west1-2",
            nodeId: 9,
            nodeName: "127.0.0.9:50945",
            uptime: "3 minutes",
            replicas: 71,
            usedCapacity: 0,
            availableCapacity: 536870912,
            usedMemory: 641945600,
            numCpus: 16,
            availableMemory: 17179869184,
            version: "v20.1.0-alpha.20191118-1798-g0161286a62-dirty",
            status: 3,
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
        />
      ));
      expect(wrapper.find("h3.text.text--heading-3").text()).toEqual(
        `Nodes (${nodesCount})`,
      );
    });

    it("displays table with required columns when nodes partitioned by locality", () => {
      const wrapper: ReactWrapper = connectedMount(store => (
        <NodeList
          dataSource={dataSource}
          nodesCount={nodesCount}
          regionsCount={regionsCount}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />
      ));
      const expectedColumns = [
        "nodes",
        "node count",
        "uptime",
        "replicas",
        "capacity usage",
        "memory use",
        "vcpus",
        "version",
        "status",
        "", // logs column doesn't have header text
      ];
      const columnCells = wrapper.find(
        ".table-section__content table thead th",
      );
      expect(columnCells.length).toEqual(expectedColumns.length);

      expectedColumns.forEach((columnName, idx) =>
        expect(columnCells.at(idx).text().toLowerCase()).toEqual(columnName),
      );
    });

    it("doesn't display 'node count' column when nodes are in single regions", () => {
      const expectedColumns = [
        "nodes",
        // should not be displayed "node count",
        "uptime",
        "replicas",
        "capacity usage",
        "memory use",
        "vcpus",
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
        />
      ));
      const columnCells = wrapper.find(
        ".table-section__content table thead th",
      );
      expect(columnCells.length).toEqual(expectedColumns.length);
      expectedColumns.forEach((columnName, idx) =>
        expect(columnCells.at(idx).text().toLowerCase()).toEqual(columnName),
      );
    });

    it("displays table with fixed column width", () => {
      const wrapper: ReactWrapper = connectedMount(store => (
        <NodeList
          dataSource={dataSource}
          nodesCount={nodesCount}
          regionsCount={regionsCount}
          setSort={sortSetting.set}
          sortSetting={sortSetting.selector(store.getState())}
        />
      ));
      const columnAttributes = wrapper.find("table colgroup col");
      columnAttributes.forEach(node =>
        expect(node.hostNodes().props().style).toHaveProperty("width"),
      );
    });
  });

  describe("Selectors", () => {
    const state = {
      cachedData: {
        nodes: {
          data: times(7).map(idx => ({
            desc: {
              node_id: idx + 1,
              locality: {
                tiers: [{ key: "region", value: "us-west" }],
              },
              address: {
                address_field: `127.0.0.${idx + 1}:50945`,
              },
            },
            metrics: {
              "capacity.used": 0,
              "capacity.available": 0,
            },
            started_at: Long.fromNumber(Date.now()),
            total_system_memory: Long.fromNumber(Math.random() * 1000000),
            build_info: {
              tag: "tag_value",
            },
          })),
          inFlight: false,
          valid: true,
        },
        liveness: {
          data: {
            livenesses: [
              { node_id: 1 },
              {
                node_id: 2,
                expiration: { wall_time: Long.fromNumber(Date.now()) },
                membership: MembershipStatus.DECOMMISSIONED,
              },
              { node_id: 3 },
              { node_id: 4 },
              { node_id: 5 },
              { node_id: 6 },
              {
                node_id: 7,
                expiration: { wall_time: Long.fromNumber(Date.now()) },
                membership: MembershipStatus.DECOMMISSIONED,
              },
            ],
            statuses: {
              1: NodeLivenessStatus.NODE_STATUS_LIVE,
              2: NodeLivenessStatus.NODE_STATUS_DECOMMISSIONED, // node_id: 2
              3: NodeLivenessStatus.NODE_STATUS_DEAD,
              4: NodeLivenessStatus.NODE_STATUS_UNAVAILABLE,
              5: NodeLivenessStatus.NODE_STATUS_UNKNOWN,
              6: NodeLivenessStatus.NODE_STATUS_DECOMMISSIONING,
              7: NodeLivenessStatus.NODE_STATUS_DECOMMISSIONED, // node_id: 7
            },
            toJSON: () => ({}),
          },
          inFlight: false,
          valid: true,
        },
      },
    };
    const partitionedNodes = {
      live: [
        state.cachedData.nodes.data[0],
        state.cachedData.nodes.data[2],
        state.cachedData.nodes.data[3],
        state.cachedData.nodes.data[4],
        state.cachedData.nodes.data[5],
      ],
      decommissioned: [
        state.cachedData.nodes.data[1],
        state.cachedData.nodes.data[6],
      ],
    };
    const nodeSummary: any = {
      livenessStatusByNodeID: state.cachedData.liveness.data.statuses,
      livenessByNodeID: livenessByNodeIDSelector.resultFunc(
        state.cachedData.liveness.data,
      ),
      nodeIDs: undefined,
      nodeDisplayNameByID: undefined,
      nodeStatusByID: undefined,
      nodeStatuses: undefined,
      nodeSums: undefined,
      storeIDsByNodeID: undefined,
    };

    describe("decommissionedNodesTableDataSelector", () => {
      it("returns node records with 'decommissioned' status only", () => {
        const expectedDecommissionedNodeIds = [2, 7];
        const records =
          decommissionedNodesTableDataSelector.resultFunc(nodeSummary);

        expect(records.length).toBe(expectedDecommissionedNodeIds.length);
        records.forEach(record => {
          expect(
            expectedDecommissionedNodeIds.some(
              nodeId => nodeId === record.nodeId,
            ),
          ).toBe(true);
        });
      });

      it("returns correct node name", () => {
        const recordsGroupedByRegion =
          decommissionedNodesTableDataSelector.resultFunc(nodeSummary);
        recordsGroupedByRegion.forEach(record => {
          expect(record.nodeName).toEqual(record.nodeId.toString());
        });
      });
    });

    describe("liveNodesTableDataSelector", () => {
      it("returns node records with all statuses except 'decommissioned' status", () => {
        const expectedLiveNodeIds = [1, 3, 4, 5, 6];
        const recordsGroupedByRegion = liveNodesTableDataSelector.resultFunc(
          partitionedNodes,
          nodeSummary,
        );

        expect(recordsGroupedByRegion.length).toBe(1);
        expect(recordsGroupedByRegion[0].children.length).toBe(
          expectedLiveNodeIds.length,
        );
        recordsGroupedByRegion[0].children.forEach(record => {
          expect(
            expectedLiveNodeIds.some(nodeId => nodeId === record.nodeId),
          ).toBe(true);
        });
      });

      it("returns correct node name", () => {
        const recordsGroupedByRegion = liveNodesTableDataSelector.resultFunc(
          partitionedNodes,
          nodeSummary,
        );
        recordsGroupedByRegion[0].children.forEach(record => {
          const expectedName = `127.0.0.${record.nodeId}:50945`;
          expect(record.nodeName).toEqual(expectedName);
        });
      });
    });
  });

  describe("getLivenessStatusName", () => {
    it("return node liveness names without prefix", () => {
      expect(getLivenessStatusName(LivenessStatus.NODE_STATUS_LIVE)).toEqual(
        "LIVE",
      );
      expect(
        getLivenessStatusName(LivenessStatus.NODE_STATUS_DECOMMISSIONED),
      ).toEqual("DECOMMISSIONED");
      expect(getLivenessStatusName(LivenessStatus.NODE_STATUS_DEAD)).toEqual(
        "DEAD",
      );
      expect(getLivenessStatusName(3)).toEqual("LIVE");
    });
  });
});
