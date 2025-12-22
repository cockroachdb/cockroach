// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  commonStyles,
  Visualization,
  BarGraph,
  AxisUnits,
} from "@cockroachlabs/cluster-ui";
import cx from "classnames";
import Long from "long";
import moment from "moment";
import React, { useCallback, useEffect, useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { getHotRanges, getNodesUI, queryTimeSeries } from "src/util/api";

import { HexagonHeatmap } from "./components/hexagonHeatmap";
import styles from "./explorer.module.styl";

type MetricType = "cpu" | "writeBytes";

const DEFAULT_RANGES_LIMIT = 10; // Default number of ranges to show

interface RangeData {
  value: number;
  rangeId: number;
  databases?: string[];
  tables?: string[];
  indexes?: string[];
}

interface NodeData {
  x: number;
  y: number;
  value: number;
  nodeId: number;
}

const EMPTY_RANGE_DATA: RangeData[] = [
  { value: 0, rangeId: 1, databases: [], tables: [], indexes: [] },
  { value: 0, rangeId: 2, databases: [], tables: [], indexes: [] },
  { value: 0, rangeId: 3, databases: [], tables: [], indexes: [] },
];

const ClusterExplorerPage = (_props: RouteComponentProps) => {
  const [currentRangeData, setCurrentRangeData] =
    useState<RangeData[]>(EMPTY_RANGE_DATA);

  const [selectedMetricType, setSelectedMetricType] =
    useState<MetricType>("cpu");

  const [selectedNodeId, setSelectedNodeId] = useState<number | null>(null);
  const [_selectedRangeId, setSelectedRangeId] = useState<number | null>(null);

  const [rangesLimit, setRangesLimit] = useState<number>(DEFAULT_RANGES_LIMIT);

  const [nodesData, setNodesData] = useState<
    { nodeId: number; cpuPercent: number; writeBytes: number }[]
  >([]);
  const [_isLoading, setIsLoading] = useState(true);

  // Fetch node data (CPU and write bytes)
  useEffect(() => {
    const fetchNodeData = async () => {
      try {
        setIsLoading(true);

        const nodesResponse = await getNodesUI({ redact: false });

        if (!nodesResponse.nodes || nodesResponse.nodes.length === 0) {
          setIsLoading(false);
          return;
        }

        // Extract node IDs and prepare data arrays
        const nodeIds = nodesResponse.nodes.map(
          (node: any) => node.desc.node_id,
        );
        const endTime = moment();
        const startTime = moment(endTime).subtract(5, "minutes");

        // Maps for all nodes data
        const cpuDataMap = new Map<number, number>();
        const writeBytesDataMap = new Map<number, number>();

        // Process each node
        await Promise.all(
          nodeIds.map(async (nodeId: number) => {
            try {
              // Fetch both data about nodes
              const tsRequest = new cockroach.ts.tspb.TimeSeriesQueryRequest({
                start_nanos: Long.fromNumber(startTime.valueOf() * 1e6),
                end_nanos: Long.fromNumber(endTime.valueOf() * 1e6),
                sample_nanos: Long.fromNumber(30 * 1e9), // 30 second samples
                queries: [
                  {
                    name: "cr.node.sys.cpu.combined.percent-normalized",
                    sources: [String(nodeId)],
                    downsampler: 3, // max
                    source_aggregator: 2,
                  },
                  {
                    name: "cr.node.sys.host.disk.write.bytes",
                    sources: [String(nodeId)],
                    downsampler: 3, // max
                    source_aggregator: 2,
                  },
                ],
              });

              const tsResponse = await queryTimeSeries(tsRequest);

              // Process response to get latest values for this node
              if (tsResponse.results && tsResponse.results.length > 0) {
                // Process CPU data (first query)
                const cpuResult = tsResponse.results[0];
                if (cpuResult.datapoints && cpuResult.datapoints.length > 0) {
                  const cpuDp = cpuResult.datapoints.at(-1);
                  if (cpuDp) {
                    const cpuValue = cpuDp.value * 100; // Convert to percentage
                    cpuDataMap.set(nodeId, cpuValue);
                  }
                }

                // Process write bytes data (second query)
                const writeBytesResult = tsResponse.results[1];
                if (
                  writeBytesResult?.datapoints &&
                  writeBytesResult.datapoints.length > 0
                ) {
                  const writeBytesDp = writeBytesResult.datapoints.at(-1);
                  if (writeBytesDp) {
                    const writeBytesValue = writeBytesDp.value / (1024 * 1024); // Convert to MB
                    writeBytesDataMap.set(nodeId, writeBytesValue);
                  }
                }
              }
            } catch (error) {
              // Set default values for this node
              cpuDataMap.set(nodeId, 0);
              writeBytesDataMap.set(nodeId, 0);
            }
          }),
        );

        // Build node data array
        const nodeData = nodesResponse.nodes.map((node: any) => {
          const nodeId = node.desc.node_id;
          const cpuPercent = Math.round(cpuDataMap.get(nodeId) || 0);
          const writeBytes = Math.round(writeBytesDataMap.get(nodeId) || 0);

          return {
            nodeId,
            cpuPercent,
            writeBytes,
          };
        });

        setNodesData(nodeData);
      } catch (error) {
        // Fallback to empty
        setNodesData([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchNodeData();

    // Refresh data every 10 seconds
    const interval = setInterval(fetchNodeData, 10 * 1000);
    return () => clearInterval(interval);
  }, [selectedMetricType]);

  // Fetch hot ranges for a selected node
  const fetchHotRangesForNode = useCallback(
    async (nodeId: number): Promise<RangeData[]> => {
      try {
        const request = new cockroach.server.serverpb.HotRangesRequest({
          nodes: [String(nodeId)],
          per_node_limit: 20,
          stats_only: false, // Get descriptors to include database/table information
          page_size: 20,
        });

        const response = await getHotRanges(
          request,
          moment.duration(30, "seconds"),
        );

        if (!response.ranges || response.ranges.length === 0) {
          return [];
        }

        // Transform hot ranges data into BarChartData format based on selected metric
        const rangeData = response.ranges.map(
          (range: cockroach.server.serverpb.HotRangesResponseV2.IHotRange) => {
            let value: number;
            if (selectedMetricType === "cpu") {
              value = range.cpu_time_per_second / 1000000; // Convert ns to ms
            } else {
              value = (range.write_bytes_per_second || 0) / (1024 * 1024); // Convert to MB/s
            }

            return {
              value,
              rangeId: range.range_id,
              databases: range.databases || [],
              tables: range.tables || [],
              indexes: range.indexes || [],
            };
          },
        );

        // Check for duplicates and consolidate them
        const rangeMap = new Map<number, RangeData>();
        for (const range of rangeData) {
          const existingRange = rangeMap.get(range.rangeId);
          if (existingRange) {
            // If duplicate, keep the one with higher value
            if (range.value > existingRange.value) {
              rangeMap.set(range.rangeId, range);
            }
          } else {
            rangeMap.set(range.rangeId, range);
          }
        }

        const deduplicatedRangeData = Array.from(rangeMap.values());

        // Sort by the selected metric value (descending order - highest values first)
        return deduplicatedRangeData.sort(
          (a: RangeData, b: RangeData) => b.value - a.value,
        );
      } catch (error) {
        // Return empty data on error
        return [];
      }
    },
    [selectedMetricType],
  );

  // Refresh hot ranges data for selected node every 10 seconds
  useEffect(() => {
    if (!selectedNodeId) return;

    const fetchRanges = async () => {
      try {
        const hotRangesData = await fetchHotRangesForNode(selectedNodeId);
        setCurrentRangeData(hotRangesData);
      } catch (error) {
        setCurrentRangeData(EMPTY_RANGE_DATA);
      }
    };

    fetchRanges();

    // Refresh data every 10 seconds
    const interval = setInterval(fetchRanges, 10 * 1000);
    return () => clearInterval(interval);
  }, [selectedNodeId, fetchHotRangesForNode]);

  // Generate hexagon grid data using real node data
  const generateNodeData = (): NodeData[] => {
    // For visual balance: columns ≈ sqrt(n) * 0.6 to get more rows
    const aspectRatio = 0.5; // Fewer columns, more rows for visual balance
    const MapColumns = Math.max(
      2,
      Math.ceil(Math.sqrt(nodesData.length * aspectRatio)),
    );
    const xGap = 62;
    const yGap = 31;

    const points: NodeData[] = [];
    for (let i = 0; i < nodesData.length; i++) {
      const row = Math.floor(i / MapColumns);
      const col = i % MapColumns;

      let x = xGap * col * Math.sqrt(3);
      if (row % 2 === 1) x += (xGap * Math.sqrt(3)) / 2;
      const y = yGap * row;

      const nodeInfo = nodesData[i];
      const value =
        selectedMetricType === "cpu"
          ? nodeInfo.cpuPercent
          : nodeInfo.writeBytes;

      points.push({
        x,
        y,
        value,
        nodeId: nodeInfo.nodeId,
      });
    }
    return points;
  };

  const nodeData = generateNodeData();

  const handleNodeClick = useCallback(
    async (nodeId: number) => {
      setSelectedNodeId(nodeId);
      setSelectedRangeId(null);

      try {
        const hotRangesData = await fetchHotRangesForNode(nodeId);
        setCurrentRangeData(hotRangesData);
      } catch (error) {
        setCurrentRangeData(EMPTY_RANGE_DATA);
      }
    },
    [fetchHotRangesForNode],
  );

  // Get display names for the current metric
  const getMetricDisplayName = () => {
    return selectedMetricType === "cpu" ? "CPU Usage" : "Write Bytes";
  };

  const getMetricUnit = () => {
    return selectedMetricType === "cpu" ? "ms/s" : "MB/s";
  };

  return (
    <div>
      <Helmet title="Cluster Explorer" />
      <h3 className={commonStyles("base-heading")}>Cluster Explorer</h3>

      {/* 2x2 Grid Layout */}
      <div className={cx(styles.gridContainer, commonStyles("small-margin"))}>
        {/* Top Left: Metric Type Selector */}
        <div className={styles.controlGroup}>
          <label className={styles.controlLabel}>Metric Type:</label>
          <select
            value={selectedMetricType}
            onChange={e => {
              setSelectedMetricType(e.target.value as MetricType);
              // Reset selected node when changing metrics
              setSelectedNodeId(null);
              setCurrentRangeData(EMPTY_RANGE_DATA);
            }}
            className={styles.selectControl}
          >
            <option value="cpu">CPU Usage (%)</option>
            <option value="writeBytes">Write Bytes (MB/s)</option>
          </select>
        </div>

        {/* Top Right: Ranges Limit Selector - only visible when a node is selected */}
        {selectedNodeId ? (
          <div className={styles.controlGroup}>
            <label className={styles.controlLabel}>Show Top:</label>
            <select
              value={rangesLimit}
              onChange={e => {
                setRangesLimit(Number(e.target.value));
              }}
              className={styles.selectControl}
            >
              <option value={10}>10 Ranges</option>
              <option value={15}>15 Ranges</option>
              <option value={20}>20 Ranges</option>
            </select>
          </div>
        ) : (
          <div />
        )}

        {/* Bottom Left: Hexagon Heatmap for all nodes */}
        <div>
          <Visualization
            title={`Node ${getMetricDisplayName()} Heatmap`}
            subtitle="Click on hexagons to see detailed metrics"
          >
            <HexagonHeatmap
              data={nodeData}
              width={600}
              height={450}
              metricType={selectedMetricType}
              onHexagonClick={handleNodeClick}
            />
          </Visualization>
        </div>

        {/* Bottom Right: Bar Graph for node ranges - only visible when a node is selected */}
        {selectedNodeId ? (
          <div>
            <BarGraph
              title={`Range ${getMetricDisplayName()} - Node ${selectedNodeId}`}
              tooltip={`Showing top ${rangesLimit} ranges for node ${selectedNodeId} (${getMetricUnit()})`}
              data={currentRangeData.slice(0, rangesLimit).map(range => ({
                label: `Range ${range.rangeId}`,
                value: range.value,
                databases: range.databases,
                tables: range.tables,
                indexes: range.indexes,
              }))}
              yAxisUnits={
                selectedMetricType === "cpu"
                  ? AxisUnits.Duration
                  : AxisUnits.Bytes
              }
            />
          </div>
        ) : (
          <div />
        )}
      </div>
    </div>
  );
};

export default ClusterExplorerPage;
