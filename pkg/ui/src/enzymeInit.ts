// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import Enzyme from "enzyme";
import Adapter from "enzyme-adapter-react-16";

// As of v3, Enzyme requires an "adapter" to be initialized.
// See https://github.com/airbnb/enzyme/blob/master/docs/guides/migration-from-2-to-3.md
Enzyme.configure({ adapter: new Adapter() });
