// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import Enzyme from "enzyme";
import Adapter from "enzyme-adapter-react-16";

// As of v3, Enzyme requires an "adapter" to be initialized.
// See https://github.com/airbnb/enzyme/blob/master/docs/guides/migration-from-2-to-3.md
Enzyme.configure({ adapter: new Adapter() });
