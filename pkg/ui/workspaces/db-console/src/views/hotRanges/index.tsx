// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import { useDispatch, useSelector } from "react-redux";
import React, { useRef, useEffect, useState } from "react";
import { Helmet } from "react-helmet";
import { refreshHotRanges } from "src/redux/apiReducers";
import HotRangesTable from "./hotRangesTable";
import ErrorBoundary from "../app/components/errorMessage/errorBoundary";
import { Loading, Text, Anchor, util } from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./hotRanges.module.styl";
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

  useEffect(() => {
    if (!isValid) {
      dispatch(refreshHotRanges(HotRangesRequest.create()));
    }
  }, [dispatch, isValid]);

  useEffect(() => {
    dispatch(
      refreshHotRanges(
        HotRangesRequest.create({
          page_size: 1000,
        }),
      ),
    );
  }, [dispatch]);

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
                lastSetAt && lastSetAt?.utc().format(util.DATE_FORMAT_24_UTC)
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
