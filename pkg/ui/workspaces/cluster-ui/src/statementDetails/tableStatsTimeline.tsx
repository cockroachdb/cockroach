// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useMemo } from "react";
import { Tooltip } from "antd";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { generateTableStatsCollectionTimeline } from "./timeseriesUtils";

type TableStatsCollectionEvents = { [key: string]: {
  events?: Array<{
    timestamp?: any;
    event_type?: string;
    reporting_id?: any;
    info?: string;
    unique_id?: Uint8Array;
    stats_name?: string;
    column_ids?: number[];
    info_timestamp?: any;
    stats_id?: any;
  }>;
} };

interface TableStatsTimelineProps {
  tableStatsCollectionEvents: TableStatsCollectionEvents;
  width?: number;
  height?: number;
}

interface TooltipInfo {
  tableName: string;
  statsId: number;
  statsName: string;
  columnIds: number[];
  eventInfo: string;
  startTime: Date;
  endTime: Date;
}

// Base color themes for different tables - each table gets a different hue
const COLOR_THEMES = [
  // Reds
  ["#ffebee", "#ffcdd2", "#ef9a9a", "#e57373", "#ef5350", "#f44336", "#e53935", "#d32f2f", "#c62828", "#b71c1c"],
  // Blues
  ["#e3f2fd", "#bbdefb", "#90caf9", "#64b5f6", "#42a5f5", "#2196f3", "#1e88e5", "#1976d2", "#1565c0", "#0d47a1"],
  // Greens
  ["#e8f5e8", "#c8e6c9", "#a5d6a7", "#81c784", "#66bb6a", "#4caf50", "#43a047", "#388e3c", "#2e7d32", "#1b5e20"],
  // Oranges
  ["#fff3e0", "#ffe0b2", "#ffcc02", "#ffb74d", "#ffa726", "#ff9800", "#fb8c00", "#f57c00", "#ef6c00", "#e65100"],
  // Purples
  ["#f3e5f5", "#e1bee7", "#ce93d8", "#ba68c8", "#ab47bc", "#9c27b0", "#8e24aa", "#7b1fa2", "#6a1b9a", "#4a148c"],
  // Teals
  ["#e0f2f1", "#b2dfdb", "#80cbc4", "#4db6ac", "#26a69a", "#009688", "#00897b", "#00796b", "#00695c", "#004d40"],
  // Yellows/Ambers
  ["#fffde7", "#fff9c4", "#fff59d", "#fff176", "#ffee58", "#ffeb3b", "#fdd835", "#f9a825", "#f57f17", "#ff6f00"],
  // Indigos
  ["#e8eaf6", "#c5cae9", "#9fa8da", "#7986cb", "#5c6bc0", "#3f51b5", "#3949ab", "#303f9f", "#283593", "#1a237e"],
  // Cyans
  ["#e0f7fa", "#b2ebf2", "#80deea", "#4dd0e1", "#26c6da", "#00bcd4", "#00acc1", "#0097a7", "#00838f", "#006064"],
  // Browns
  ["#efebe9", "#d7ccc8", "#bcaaa4", "#a1887f", "#8d6e63", "#795548", "#6d4c41", "#5d4037", "#4e342e", "#3e2723"]
];

// Generate color for a table segment based on table index and segment index
const getSegmentColor = (tableIndex: number, segmentIndex: number, totalSegments: number): string => {
  const colorTheme = COLOR_THEMES[tableIndex % COLOR_THEMES.length];
  
  if (totalSegments === 1) {
    return colorTheme[4]; // Use middle color for single segment
  }
  
  // Map segment index to color intensity (lighter to darker)
  const colorIndex = Math.min(
    Math.floor((segmentIndex / (totalSegments - 1)) * (colorTheme.length - 1)),
    colorTheme.length - 1
  );
  
  return colorTheme[colorIndex];
};

export const TableStatsTimeline: React.FC<TableStatsTimelineProps> = ({
  tableStatsCollectionEvents,
  width = 800,
  height = 400,
}) => {
  const { tableNames, eventsByTable } = useMemo(() => 
    generateTableStatsCollectionTimeline(tableStatsCollectionEvents || {}),
    [tableStatsCollectionEvents]
  );

  if (tableNames.length === 0) {
    return <div>No table stats collection events found</div>;
  }

  // Calculate time range
  let minTime = Number.MAX_SAFE_INTEGER;
  let maxTime = 0;
  
  eventsByTable.forEach(segments => {
    segments.forEach(segment => {
      minTime = Math.min(minTime, segment.startTime);
      maxTime = Math.max(maxTime, segment.endTime);
    });
  });

  const timeRange = maxTime - minTime;
  const barHeight = Math.max(30, (height - 120) / tableNames.length); // Increased minimum bar height
  const barSpacing = 10; // Space between table bars
  const bottomMargin = 80; // Increased space from x-axis
  const chartHeight = tableNames.length * (barHeight + barSpacing) + bottomMargin;

  const renderTimelineBar = (
    tableName: string,
    segments: Array<{
      startTime: number;
      endTime: number;
      statsId: number;
      statsName: string;
      columnIds: number[];
      eventInfo: string;
    }>,
    yPosition: number,
    tableIndex: number,
  ) => {
    return segments.map((segment, index) => {
      const startPercent = ((segment.startTime - minTime) / timeRange) * 100;
      const endPercent = ((segment.endTime - minTime) / timeRange) * 100;
      const widthPercent = Math.max(0.5, endPercent - startPercent); // Minimum width for visibility
      
      const color = getSegmentColor(tableIndex, index, segments.length);

      const tooltipInfo: TooltipInfo = {
        tableName,
        statsId: segment.statsId,
        statsName: segment.statsName,
        columnIds: segment.columnIds,
        eventInfo: segment.eventInfo,
        startTime: new Date(segment.startTime),
        endTime: new Date(segment.endTime),
      };

      return (
        <Tooltip
          key={`${tableName}-${index}`}
          title={
            <div>
              <div><strong>Table:</strong> {tooltipInfo.tableName}</div>
              <div><strong>Stats ID:</strong> {tooltipInfo.statsId}</div>
              <div><strong>Stats Name:</strong> {tooltipInfo.statsName}</div>
              <div><strong>Columns:</strong> {tooltipInfo.columnIds.join(', ') || 'N/A'}</div>
              <div><strong>Start:</strong> {tooltipInfo.startTime.toLocaleString()}</div>
              <div><strong>End:</strong> {tooltipInfo.endTime.toLocaleString()}</div>
              {tooltipInfo.eventInfo && (
                <div><strong>Info:</strong> {tooltipInfo.eventInfo}</div>
              )}
            </div>
          }
          placement="top"
        >
          <rect
            x={`${startPercent}%`}
            y={yPosition}
            width={`${widthPercent}%`}
            height={barHeight - 2}
            fill={color}
            stroke="#fff"
            strokeWidth={1}
            style={{ cursor: "pointer" }}
            opacity={0.8}
          />
        </Tooltip>
      );
    });
  };

  // Generate time axis labels
  const timeLabels = [];
  const numLabels = 5;
  for (let i = 0; i <= numLabels; i++) {
    const time = minTime + (timeRange * i) / numLabels;
    const date = new Date(time);
    const xPercent = (i / numLabels) * 100;
    timeLabels.push(
      <g key={i}>
        <line
          x1={`${xPercent}%`}
          y1={0}
          x2={`${xPercent}%`}
          y2={chartHeight - bottomMargin + 20}
          stroke="#ddd"
          strokeWidth={1}
        />
        <text
          x={`${xPercent}%`}
          y={chartHeight - 30}
          textAnchor="middle"
          fontSize={14}
          fill="#333"
          fontWeight="500"
        >
          {date.toLocaleTimeString()}
        </text>
      </g>
    );
  }

  return (
    <div style={{ width, overflowX: "auto" }}>
      <svg width="100%" height={chartHeight} style={{ minWidth: width }}>
        {timeLabels}
        
        {tableNames.map((tableName, index) => {
          const yPosition = index * (barHeight + barSpacing) + 40;
          const segments = eventsByTable.get(tableName) || [];
          
          return (
            <g key={tableName}>
              {/* Table name label */}
              <text
                x={0}
                y={yPosition + barHeight / 2}
                textAnchor="start"
                fontSize={12}
                fill="#333"
                dominantBaseline="middle"
                style={{ fontWeight: "bold" }}
              >
                {tableName}
              </text>
              
              {/* Timeline bars */}
              <g transform={`translate(120, 0)`}>
                {renderTimelineBar(tableName, segments, yPosition, index)}
              </g>
            </g>
          );
        })}
      </svg>
    </div>
  );
};