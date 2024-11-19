// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useContext, useEffect, useRef } from "react";
import uPlot, { AlignedData, Options } from "uplot";

import { TimezoneContext } from "../../contexts";
import {
  AxisUnits,
  calculateXAxisDomainBarChart,
  calculateYAxisDomain,
} from "../utils/domain";
import { Visualization } from "../visualization";

import styles from "./bargraph.module.scss";
import { getStackedBarOpts, stack } from "./bars";

const cx = classNames.bind(styles);

export type BarGraphTimeSeriesProps = {
  alignedData?: AlignedData;
  colourPalette?: string[]; // Series colour palette.
  preCalcGraphSize?: boolean;
  title: string;
  tooltip?: React.ReactNode;
  uPlotOptions: Partial<Options>;
  yAxisUnits: AxisUnits;
  xScale?: XScale;
};

export type XScale = {
  graphTsStartMillis: number;
  graphTsEndMillis: number;
};

// Currently this component only supports stacked multi-series bars.
// The value of xScale will take precedent over the start and end of the data provided.
export const BarGraphTimeSeries: React.FC<BarGraphTimeSeriesProps> = ({
  alignedData,
  colourPalette,
  preCalcGraphSize = true,
  title,
  tooltip,
  uPlotOptions,
  yAxisUnits,
  xScale,
}) => {
  const graphRef = useRef<HTMLDivElement>(null);
  const samplingIntervalMillis =
    alignedData[0].length > 1 ? alignedData[0][1] - alignedData[0][0] : 1e3;
  const timezone = useContext(TimezoneContext);

  useEffect(() => {
    if (!alignedData) return;

    const start = xScale.graphTsStartMillis
      ? xScale.graphTsStartMillis
      : alignedData[0][0];
    const end = xScale.graphTsEndMillis
      ? xScale.graphTsEndMillis
      : alignedData[0][alignedData[0].length - 1];
    const xAxisDomain = calculateXAxisDomainBarChart(
      start, // startMillis
      end, // endMillis
      samplingIntervalMillis,
      timezone,
    );

    const stackedData = stack(alignedData, () => false);

    const allYDomainPoints: number[] = [];
    stackedData.slice(1).forEach(points => allYDomainPoints.push(...points));
    const yAxisDomain = calculateYAxisDomain(yAxisUnits, allYDomainPoints);

    const opts = getStackedBarOpts(
      alignedData,
      uPlotOptions,
      xAxisDomain,
      yAxisDomain,
      yAxisUnits,
      colourPalette,
      timezone,
    );

    const plot = new uPlot(opts, stackedData, graphRef.current);

    return () => {
      plot?.destroy();
    };
  }, [
    alignedData,
    colourPalette,
    uPlotOptions,
    yAxisUnits,
    samplingIntervalMillis,
    timezone,
    xScale,
  ]);

  return (
    <Visualization
      title={title}
      loading={!alignedData}
      preCalcGraphSize={preCalcGraphSize}
      tooltip={tooltip}
    >
      <div className={cx("bargraph")}>
        <div ref={graphRef} />
      </div>
    </Visualization>
  );
};
