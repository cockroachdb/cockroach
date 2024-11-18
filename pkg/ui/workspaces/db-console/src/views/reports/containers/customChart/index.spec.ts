// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "src/js/protos";
import { MetricsMetadata } from "src/redux/metricMetadata";
import { NodesSummary } from "src/redux/nodes";
import { INodeStatus } from "src/util/proto";
import { CustomMetricState } from "src/views/reports/containers/customChart/customMetric";
import { getSources } from "src/views/reports/containers/customChart/index";

import TimeSeriesQueryAggregator = protos.cockroach.ts.tspb.TimeSeriesQueryAggregator;
import TimeSeriesQueryDerivative = protos.cockroach.ts.tspb.TimeSeriesQueryDerivative;

const emptyMetricsMetadata: MetricsMetadata = {
  metadata: {},
  recordedNames: {},
};
describe("Custom charts page", function () {
  describe("Getting metric sources", function () {
    it("returns empty when nodesSummary is undefined", function () {
      const metricState = new testCustomMetricStateBuilder().build();
      expect(
        getSources(undefined, metricState, emptyMetricsMetadata),
      ).toStrictEqual([]);
    });

    it("returns empty when the nodeStatuses collection is empty", function () {
      const nodesSummary = new testNodesSummaryBuilder().build();
      nodesSummary.nodeStatuses = [];
      const metricState = new testCustomMetricStateBuilder().build();
      expect(
        getSources(nodesSummary, metricState, emptyMetricsMetadata),
      ).toStrictEqual([]);
    });

    it("returns empty when no specific node source is requested, nor per-source metrics", function () {
      const nodesSummary = new testNodesSummaryBuilder().build();
      const metricState = new testCustomMetricStateBuilder()
        .setNodeSource("")
        .setIsPerSource(false)
        .build();
      expect(
        getSources(nodesSummary, metricState, emptyMetricsMetadata),
      ).toStrictEqual([]);
    });

    describe("The metric is at the store-level", function () {
      const storeMetricName = "cr.store.metric";

      it("returns the store IDs associated with a specific node when a node source is set", function () {
        const expectedSources = ["1", "2", "3"];
        const metricState = new testCustomMetricStateBuilder()
          .setName(storeMetricName)
          .setNodeSource("1")
          .build();
        const nodesSummary = new testNodesSummaryBuilder()
          .setStoreIDsByNodeID({
            "1": expectedSources,
          })
          .build();
        expect(
          getSources(nodesSummary, metricState, emptyMetricsMetadata),
        ).toStrictEqual(expectedSources);
      });

      it("returns all known store IDs for the cluster when no node source is set", function () {
        const expectedSources = ["1", "2", "3", "4", "5", "6", "7", "8", "9"];
        const metricState = new testCustomMetricStateBuilder()
          .setName(storeMetricName)
          .build();
        const nodesSummary = new testNodesSummaryBuilder()
          .setStoreIDsByNodeID({
            "1": ["1", "2", "3"],
            "2": ["4", "5", "6"],
            "3": ["7", "8", "9"],
          })
          .build();
        const actualSources = getSources(
          nodesSummary,
          metricState,
          emptyMetricsMetadata,
        ).sort();
        expect(actualSources).toStrictEqual(expectedSources);
      });
    });

    describe("The metric is at the node-level", function () {
      const nodeMetricName = "cr.node.metric";

      it("returns the specified node source when a node source is set", function () {
        const expectedSources = ["1"];
        const metricState = new testCustomMetricStateBuilder()
          .setName(nodeMetricName)
          .setNodeSource("1")
          .build();
        const nodesSummary = new testNodesSummaryBuilder().build();
        expect(
          getSources(nodesSummary, metricState, emptyMetricsMetadata),
        ).toStrictEqual(expectedSources);
      });

      it("returns all known node IDs when no node source is set", function () {
        const expectedSources = ["1", "2", "3"];
        const metricState = new testCustomMetricStateBuilder()
          .setName(nodeMetricName)
          .build();
        const nodesSummary = new testNodesSummaryBuilder()
          .setNodeIDs(["1", "2", "3"])
          .build();
        expect(
          getSources(nodesSummary, metricState, emptyMetricsMetadata),
        ).toStrictEqual(expectedSources);
      });
    });
  });
});

class testCustomMetricStateBuilder {
  name: string;
  nodeSource: string;
  perSource: boolean;

  setName(name: string): testCustomMetricStateBuilder {
    this.name = name;
    return this;
  }

  setNodeSource(nodeSource: string): testCustomMetricStateBuilder {
    this.nodeSource = nodeSource;
    return this;
  }

  setIsPerSource(perSource: boolean): testCustomMetricStateBuilder {
    this.perSource = perSource;
    return this;
  }

  build(): CustomMetricState {
    return {
      metric: this.name,
      downsampler: TimeSeriesQueryAggregator.AVG,
      aggregator: TimeSeriesQueryAggregator.SUM,
      derivative: TimeSeriesQueryDerivative.NONE,
      perSource: this.perSource,
      perTenant: false,
      nodeSource: this.nodeSource,
      tenantSource: "",
    };
  }
}

class testNodesSummaryBuilder {
  nodeStatuses: INodeStatus[];
  storeIDsByNodeID: { [key: string]: string[] };
  nodeIDs: string[];

  setStoreIDsByNodeID(storeIDsByNodeID: {
    [key: string]: string[];
  }): testNodesSummaryBuilder {
    this.storeIDsByNodeID = storeIDsByNodeID;
    return this;
  }

  setNodeIDs(nodeIDs: string[]): testNodesSummaryBuilder {
    this.nodeIDs = nodeIDs;
    return this;
  }

  build(): NodesSummary {
    return {
      // We normally don't care about the nodeStatuses elements, so long as it's not an empty list.
      // Populate with an empty object.
      nodeStatuses: [
        {
          // We also need a non-empty list of store_statuses, for the isStoreMetric() call made.
          store_statuses: [{}],
        },
      ],
      nodeIDs: this.nodeIDs,
      nodeStatusByID: { "": {} },
      nodeDisplayNameByID: { "": "" },
      livenessStatusByNodeID: {},
      livenessByNodeID: {},
      storeIDsByNodeID: this.storeIDsByNodeID,
      nodeLastError: undefined,
    };
  }
}
