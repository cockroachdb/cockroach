// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext, useEffect, useRef } from "react";
import classNames from "classnames/bind";
import { getStackedBarOpts, stack } from "./bars";
import uPlot, { AlignedData } from "uplot";
import styles from "./bargraph.module.scss";
import { Visualization } from "../visualization";
import {
  AxisUnits,
  calculateXAxisDomainBarChart,
  calculateYAxisDomain,
} from "../utils/domain";
import { Options } from "uplot";
import { TimezoneContext } from "../../contexts";

const cx = classNames.bind(styles);

export type BarGraphTimeSeriesProps = {
  alignedData?: AlignedData;
  colourPalette?: string[]; // Series colour palette.
  preCalcGraphSize?: boolean;
  title: string;
  tooltip?: React.ReactNode;
  uPlotOptions: Partial<Options>;
  yAxisUnits: AxisUnits;
};

// Currently this component only supports stacked multi-series bars.
export const BarGraphTimeSeries: React.FC<BarGraphTimeSeriesProps> = ({
  alignedData,
  colourPalette,
  preCalcGraphSize = true,
  title,
  tooltip,
  uPlotOptions,
  yAxisUnits,
}) => {
  const graphRef = useRef<HTMLDivElement>(null);
  const samplingIntervalMillis =
    alignedData[0].length > 1 ? alignedData[0][1] - alignedData[0][0] : 1e3;
  const timezone = useContext(TimezoneContext);

  useEffect(() => {
    if (!alignedData) return;

    const xAxisDomain = calculateXAxisDomainBarChart(
      alignedData[0][0], // startMillis
      alignedData[0][alignedData[0].length - 1], // endMillis
      samplingIntervalMillis,
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
