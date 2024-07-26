import { Table } from "antd";
import React, { useState } from "react";
import uPlot from "uplot";

import type { ColumnsType } from "antd/es/table";

import "./sortableLegend.css";

type LegendVal = {
  raw: number;
  formatted: string | number;
};

export type SeriesInfo = {
  index: number;
  label: string;
  stroke: string;
  value?: LegendVal;
  min?: LegendVal;
  max?: LegendVal;
  show: boolean;
};

interface SortableLegendProps {
  u: uPlot;
  series: SeriesInfo[];
}

const columns: ColumnsType<SeriesInfo> = [
  {
    title: "Series",
    dataIndex: "label",
    key: "label",
    sorter: (a, b) => a.label.localeCompare(b.label),
    render: (text, record) => (
      <span>
        <span
          className="color-swatch"
          style={{
            background: record.stroke,
          }}
        />
        <span>{text}</span>
      </span>
    ),
  },
  {
    title: "Value",
    dataIndex: ["value", "formatted"],
    key: "value",
    sorter: (a, b) => (a.value?.raw ?? 0) - (b.value?.raw ?? 0),
  },
  {
    title: "Min",
    dataIndex: ["min", "formatted"],
    key: "min",
    sorter: (a, b) => (a.min?.raw ?? 0) - (b.min?.raw ?? 0),
  },
  {
    title: "Max",
    dataIndex: ["max", "formatted"],
    key: "max",
    sorter: (a, b) => (a.max?.raw ?? 0) - (b.max?.raw ?? 0),
  },
];

const SortableLegend: React.FC<SortableLegendProps> = ({ u, series }) => {
  const [isolatedSeries, setIsolatedSeries] = useState<number | null>(null);

  const onToggleSeries = (index: number) => {
    if (!u?.series) return;

    const originalCursor = {
      top: u.cursor?.top,
      left: u.cursor?.left,
    };

    const nextIsolatedIdx = isolatedSeries === index ? null : index;

    u.series.forEach((_, i) => {
      i > 0 &&
        u.setSeries(i, {
          show: nextIsolatedIdx != null ? i === nextIsolatedIdx : true,
        });
    });

    if (nextIsolatedIdx == null) {
      // If we're un-isolating, we need to make sure we maintain the cursor position.
      u.setCursor({
        left: originalCursor.left,
        top: originalCursor.top,
      });
      u.setSeries(index, { focus: true });
    } else {
      const cursorTopVal = u.valToPos(u.data[index][u.cursor.idx], "yAxis");
      u.setCursor({ left: u.cursor.left, top: cursorTopVal });
      u.setSeries(nextIsolatedIdx, { focus: true });
    }

    u.redraw();
    setIsolatedSeries(nextIsolatedIdx);
  };

  const formattedTime =
    u?.data?.length && u?.cursor.idx
      ? new Date(u.data[0][u.cursor.idx]).toLocaleString()
      : "--";

  return (
    <div className="sortable-legend">
      <div className="time-label">Time: {formattedTime}</div>
      <Table
        columns={columns}
        dataSource={series}
        pagination={false}
        size="small"
        onRow={r => ({
          onClick: () => onToggleSeries(r.index),
          className: `sortable-legend-row ${!isolatedSeries || r.index === isolatedSeries ? "" : "hidden-row"}`,
        })}
        style={{ width: "100%" }}
      />
    </div>
  );
};

export default React.memo(SortableLegend);
