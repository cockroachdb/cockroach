// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createHash } from "crypto";

import {
  Loading,
  Text,
  Anchor,
  util,
  TimezoneContext,
  SortSetting,
  ISortedTablePagination,
} from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import React, { useRef, useMemo, useEffect, useContext, useState } from "react";
import { Helmet } from "react-helmet";
import { useDispatch, useSelector } from "react-redux";

import { cockroach } from "src/js/protos";
import { analytics } from "src/redux/analytics";
import {
  refreshHotRanges,
  clearHotRanges,
  refreshDatabases,
} from "src/redux/apiReducers";
import {
  hotRangesSelector,
  isLoadingSelector,
  lastErrorSelector,
  lastSetAtSelector,
} from "src/redux/hotRanges";
import { selectNodeLocalities } from "src/redux/localities";
import { performanceBestPracticesHotSpots } from "src/util/docs";
import { HotRangesFilter } from "src/views/hotRanges/hotRangesFilter";
import useFilters, { filterRanges } from "src/views/hotRanges/useFilters";

import ErrorBoundary from "../app/components/errorMessage/errorBoundary";

import styles from "./hotRanges.module.styl";
import HotRangesTable from "./hotRangesTable";

const cx = classNames.bind(styles);
const HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;

const emptyMessages = {
  SELECT_NODES: {
    title: "Select nodes to view top ranges",
    message:
      "Select one or more nodes with high activity, such as high CPU usage, to investigate potential hotspots. Filtering fewer nodes helps you identify the hottest ranges more quickly and improves page load time.",
  },
  MODIFY_FILTERS: {
    title: "No results found",
    message:
      "No hot ranges found for the selected filters. Modify the filters to identify hot ranges",
  },
};

const HotRangesPage = () => {
  const dispatch = useDispatch();
  const hotRanges = useSelector(hotRangesSelector);
  const lastError = useSelector(lastErrorSelector);
  const lastSetAt = useSelector(lastSetAtSelector);
  const isLoading = useSelector(isLoadingSelector);
  const nodeIdToLocalityMap = useSelector(selectNodeLocalities);
  const timezone = useContext(TimezoneContext);

  const { filters, applyFilters } = useFilters();
  const [sortSetting, setSortSetting] = useState<SortSetting>(null);
  const [pagination, setPagination] = useState<ISortedTablePagination>(null);

  // dispatch hot ranges call whenever the filters change and are not empty
  useEffect(() => {
    if (filters.nodeIds.length > 0) {
      dispatch(
        refreshHotRanges(
          new HotRangesRequest({ nodes: filters.nodeIds.map(String) }),
        ),
      );
    } else {
      dispatch(clearHotRanges());
    }
  }, [filters.nodeIds, dispatch]);

  // track analytics on filters, pagination and sort.
  const analyticsKey = createHash("md5")
    .update(JSON.stringify([filters, sortSetting, pagination]))
    .digest("hex");
  useEffect(() => {
    if (!filters.nodeIds.length || !pagination || !sortSetting) {
      return;
    }
    analytics.track({
      event: "Hot Ranges Page Load",
      properties: {
        filters,
        pagination,
        sortSetting,
      },
    });
    // this is keyed on a hash of the contents for filters, pagination and sortSetting
    // as opposed to the references themselves, as we don't want to re-run this effect
    // when the contents haven't changed, but the references have.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [analyticsKey]);

  // load the databases if possible.
  useEffect(() => {
    dispatch(refreshDatabases());
  }, [dispatch]);

  const clearButtonRef = useRef<HTMLSpanElement>();
  const filteredRanges = useMemo(() => {
    return filterRanges(hotRanges, filters);
  }, [filters, hotRanges]);

  const emptyMessage = useMemo(
    () =>
      filters.nodeIds.length
        ? emptyMessages.MODIFY_FILTERS
        : emptyMessages.SELECT_NODES,
    [filters.nodeIds],
  );

  return (
    <React.Fragment>
      <Helmet title="Top Ranges" />
      <h3 className="base-heading">Top Ranges</h3>
      <Text className={cx("hotranges-description")}>
        The Top Ranges table shows ranges receiving a high number of reads or
        writes. By default, the table is sorted by ranges with the highest QPS
        (queries per second). <br />
        Use this information to
        <Anchor href={performanceBestPracticesHotSpots} target="_blank">
          {" "}
          find and reduce hot spots.
        </Anchor>
      </Text>
      <HotRangesFilter filters={filters} applyFilters={applyFilters} />
      <ErrorBoundary>
        <Loading
          loading={isLoading}
          loadingText={`Loading ranges for ${filters.nodeIds?.length} nodes...`}
          error={lastError}
          render={() => (
            <HotRangesTable
              hotRangesList={filteredRanges}
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
              emptyMessage={emptyMessage}
              onViewPropertiesChange={({
                sortSetting,
                pagination,
              }: {
                sortSetting: SortSetting;
                pagination: ISortedTablePagination;
              }) => {
                setSortSetting(sortSetting);
                setPagination(pagination);
              }}
            />
          )}
          page={undefined}
        />
      </ErrorBoundary>
    </React.Fragment>
  );
};

export default HotRangesPage;
