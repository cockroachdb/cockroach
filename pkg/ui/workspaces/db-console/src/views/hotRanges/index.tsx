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
import { useDispatch, useSelector } from "react-redux";
import React, { useEffect } from "react";
import { Helmet } from "react-helmet";
import { getHotRangesAction } from "../../redux/hotRanges/hotRangesActions";
import { HotRangesState } from "../../redux/hotRanges/hotRangesReducer";
import { selectHotRanges } from "../../redux/hotRanges/hotRangesSelectors";
import HotRangesTable from "./hotRangesTable";

const HotRangesPage = () => {
  const dispatch = useDispatch();
  const hotRanges: HotRangesState = useSelector(selectHotRanges);

  useEffect(() => {
    dispatch(getHotRangesAction());
  }, [dispatch]);

  return (
    <div className="section">
      <Helmet title="Hot Ranges" />
      <h1 className="base-heading">Hot ranges</h1>
      {hotRanges.data && <HotRangesTable hotRangesList={hotRanges.data} />}
    </div>
  );
};

export default HotRangesPage;
