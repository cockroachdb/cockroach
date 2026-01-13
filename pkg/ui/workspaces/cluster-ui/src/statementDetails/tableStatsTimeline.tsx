// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useMemo } from "react";
import { Tooltip } from "antd";

import { generateTableStatsCollectionTimeline } from "./timeseriesUtils";

type TableStatsCollectionEvents = { [key: string]: {
  events?: Array<{
    timestamp?: { seconds?: number; nanos?: number };
    event_type?: string;
    reporting_id?: number;
    info?: string;
    unique_id?: Uint8Array;
    stats_name?: string;
    column_ids?: number[];
    info_timestamp?: { seconds?: number; nanos?: number };
    stats_id?: number;
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

const COLORS = [
  "#3366CC", "#DC3912", "#FF9900", "#109618", "#990099",
  "#0099C6", "#DD4477", "#66AA00", "#B82E2E", "#316395",
  "#994499", "#22AA99", "#AAAA11", "#6633CC", "#E67300",
  "#8B0707", "#651067", "#329262", "#5574A6", "#3B3EAC"
];

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
  const barHeight = Math.max(20, (height - 60) / tableNames.length);
  const chartHeight = tableNames.length * barHeight + 60;

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
  ) => {
    return segments.map((segment, index) => {
      const startPercent = ((segment.startTime - minTime) / timeRange) * 100;
      const endPercent = ((segment.endTime - minTime) / timeRange) * 100;
      const widthPercent = Math.max(0.5, endPercent - startPercent); // Minimum width for visibility
      
      const colorIndex = (segment.statsId % COLORS.length);
      const color = COLORS[colorIndex];

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
          y2={chartHeight - 40}
          stroke="#ddd"
          strokeWidth={1}
        />
        <text
          x={`${xPercent}%`}
          y={chartHeight - 20}
          textAnchor="middle"
          fontSize={12}
          fill="#666"
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
          const yPosition = index * barHeight + 40;
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
                {renderTimelineBar(tableName, segments, yPosition)}
              </g>
            </g>
          );
        })}
      </svg>
    </div>
  );
};