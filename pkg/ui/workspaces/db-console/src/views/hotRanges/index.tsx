// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Loading,
  Text,
  Anchor,
  util,
  TimezoneContext,
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import React, { useRef, useEffect, useState, useContext } from "react";
import { Helmet } from "react-helmet";
import { useDispatch, useSelector } from "react-redux";

import { cockroach } from "src/js/protos";
import { refreshHotRanges } from "src/redux/apiReducers";
import {
  hotRangesSelector,
  isLoadingSelector,
  isValidSelector,
  lastErrorSelector,
  lastSetAtSelector,
} from "src/redux/hotRanges";
import { selectNodeLocalities } from "src/redux/localities";
import { performanceBestPracticesHotSpots } from "src/util/docs";
import { HotRangesFilter } from "src/views/hotRanges/hotRangesFilter";

import ErrorBoundary from "../app/components/errorMessage/errorBoundary";

import styles from "./hotRanges.module.styl";
import HotRangesTable from "./hotRangesTable";

const cx = classNames.bind(styles);
const HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;
type HotRange = cockroach.server.serverpb.HotRangesResponseV2.IHotRange;

const HotRangesPage = () => {
  const dispatch = useDispatch();
  const hotRanges = useSelector(hotRangesSelector);
  const isValid = useSelector(isValidSelector);
  const lastError = useSelector(lastErrorSelector);
  const lastSetAt = useSelector(lastSetAtSelector);
  const isLoading = useSelector(isLoadingSelector);
  const nodeIdToLocalityMap = useSelector(selectNodeLocalities);
  const timezone = useContext(TimezoneContext);

  useEffect(() => {
    if (!isValid) {
      dispatch(refreshHotRanges(new HotRangesRequest()));
    }
  }, [dispatch, isValid]);

  const [filteredHotRanges, setFilteredHotRanges] =
    useState<HotRange[]>(hotRanges);

  const clearButtonRef = useRef<HTMLSpanElement>();

  return (
    <React.Fragment>
      <Helmet title="Hot Ranges" />
      <h3 className="base-heading">Hot Ranges</h3>
      <Text className={cx("hotranges-description")}>
        The Hot Ranges table shows ranges receiving a high number of reads or
        writes. By default, the table is sorted by ranges with the highest QPS
        (queries per second). <br />
        Use this information to
        <Anchor href={performanceBestPracticesHotSpots} target="_blank">
          {" "}
          find and reduce hot spots.
        </Anchor>
      </Text>
      <HotRangesFilter
        hotRanges={hotRanges}
        onChange={setFilteredHotRanges}
        nodeIdToLocalityMap={nodeIdToLocalityMap}
        clearButtonContainer={clearButtonRef.current}
      />
      <ErrorBoundary>
        <Loading
          loading={isLoading}
          error={lastError}
          render={() => (
            <HotRangesTable
              hotRangesList={filteredHotRanges}
              lastUpdate={
                lastSetAt &&
                util.FormatWithTimezone(
                  lastSetAt,
                  util.DATE_FORMAT_24_TZ,
                  timezone,
                )
              }
              nodeIdToLocalityMap={nodeIdToLocalityMap}
              clearFilterContainer={<span ref={clearButtonRef} />}
            />
          )}
          page={undefined}
        />
      </ErrorBoundary>
    </React.Fragment>
  );
};

export default HotRangesPage;
