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
import { useDispatch } from "react-redux";
import { getHotRangesAction } from "oss/src/redux/hotRanges/hotRangesActions";
import React, { useEffect } from "react";
import { Helmet } from "react-helmet";

const HotRanges = () => {
  const dispatch = useDispatch();

  useEffect(() => {
    dispatch(getHotRangesAction());
  });
  return (
    <div className="section">
      <Helmet title="Hot Ranges" />
      <h1 className="base-heading">Hot ranges</h1>
    </div>
  );
};

export default HotRanges;
