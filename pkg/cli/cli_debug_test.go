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

package cli

func Example_debug_decode_key_value() {
	cliTest{}.RunWithArgs([]string{"debug", "decode-value", "016b12bd8980c0b6c2e211ba5182000172647363", "884d186d03089b09120bbd8980c0b6c2e211ba51821a0bbd8980c0b9e7c5c610e99622060801100118012206080410041802220608021002180428053004"})

	// Output:
	// debug decode-value 016b12bd8980c0b6c2e211ba5182000172647363 884d186d03089b09120bbd8980c0b6c2e211ba51821a0bbd8980c0b9e7c5c610e99622060801100118012206080410041802220608021002180428053004
	// unable to decode key: invalid encoded mvcc key: 016b12bd8980c0b6c2e211ba5182000172647363, assuming it's a roachpb.Key with fake timestamp;
	// if the result below looks like garbage, then it likely is:
	//
	// 0.987654321,0 /Local/Range/Table/53/1/-4560243296450227838/RangeDescriptor: [/Table/53/1/-4560243296450227838, /Table/53/1/-4559358311118345834)
	// 	Raw:r1179:/Table/53/1/-45{60243296450227838-59358311118345834} [(n1,s1):1, (n4,s4):2, (n2,s2):4, next=5, gen=4]
}
