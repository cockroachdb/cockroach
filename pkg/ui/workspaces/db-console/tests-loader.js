// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

const testsContext = require.context("./src", true, /\.spec\.(ts|tsx)$/);
const cclTestsContext = require.context("./ccl/src", true, /\.spec\.(ts|tsx)$/);

testsContext.keys().forEach(testsContext);
cclTestsContext.keys().forEach(cclTestsContext);
