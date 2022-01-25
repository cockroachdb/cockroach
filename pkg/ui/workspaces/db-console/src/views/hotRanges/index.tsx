// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import moment from "moment";
import { cockroach } from "src/js/protos";
import { useDispatch, useSelector } from "react-redux";
import React, { useEffect } from "react";
import { Helmet } from "react-helmet";
import { refreshHotRanges } from "../../redux/apiReducers";
import { selectHotRanges } from "../../redux/hotRanges/hotRangesSelectors";
import HotRangesTable from "./hotRangesTable";
import ErrorBoundary from "../app/components/errorMessage/errorBoundary";
import { Loading, Text, Anchor } from "@cockroachlabs/cluster-ui";
import classNames from "classnames/bind";
import styles from "./hotRanges.module.styl";

const cx = classNames.bind(styles);
const HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;

const HotRangesPage = () => {
  const dispatch = useDispatch();
  const hotRanges = useSelector(selectHotRanges);

  useEffect(() => {
    if(!hotRanges.valid) {
      dispatch(refreshHotRanges(new HotRangesRequest()));
    }
  }, [dispatch, hotRanges.valid]);

  const formatCurrentDateTime = (datetime: moment.Moment) => {
    return (
      datetime.format("MMM DD, YYYY") + " at " + datetime.format("h:mm A") + " (UTC)"
    );
  };
  // TODO: Alex S add url to anchor once it's available
  return (
    <div className="section">
      <Helmet title="Hot Ranges" />
      <h1 className="base-heading">Hot ranges</h1>
      <Text className={cx("hotranges-description")}>
        The hot ranges table shows ranges receiving a high number of reads or writes. By default the table is sorted by 
        <br /> 
        ranges with the highest QPS (Queries Per Second). Use this information to... 
        <Anchor href="" target="_blank"> Learn more</Anchor>
      </Text>
      <ErrorBoundary>
        <Loading
          loading={!hotRanges.data && !hotRanges.lastError}
          error={hotRanges.lastError}
          render={() => (
            <HotRangesTable
              hotRangesList={hotRanges.data?.ranges}
              lastUpdate={formatCurrentDateTime(hotRanges.setAt?.utc())}
            />
          )}
        />
      </ErrorBoundary>
    </div>
  );
};

export default HotRangesPage;
