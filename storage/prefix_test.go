// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	config1 = 1
	config2 = 2
	config3 = 3
	config4 = 4
	config5 = 5
)

func buildTestPrefixConfigMap() PrefixConfigMap {
	configs := []*PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("/db1"), nil, config2},
		{proto.Key("/db1/table"), nil, config3},
		{proto.Key("/db3"), nil, config4},
	}
	pcc, err := NewPrefixConfigMap(configs)
	if err != nil {
		log.Fatalf("unexpected error building config map: %v", err)
	}
	return pcc
}

func verifyPrefixConfigMap(pcc PrefixConfigMap, expPrefixConfigs []PrefixConfig, t *testing.T) {
	if len(pcc) != len(expPrefixConfigs) {
		t.Fatalf("incorrect number of built prefix configs; expected %d, got %d",
			len(expPrefixConfigs), len(pcc))
	}
	for i, pc := range pcc {
		exp := expPrefixConfigs[i]
		if bytes.Compare(pc.Prefix, exp.Prefix) != 0 {
			t.Errorf("prefix for index %d incorrect; expected %q, got %q", i, exp.Prefix, pc.Prefix)
		}
		if pc.Config != exp.Config {
			t.Errorf("config for index %d incorrect: expected %v, got %v", i, exp.Config, pc.Config)
		}
	}
}

// TestPrefixEndKey verifies the end keys on prefixes.
func TestPrefixEndKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	testData := []struct {
		prefix, expEnd proto.Key
	}{
		{engine.KeyMin, engine.KeyMax},
		{proto.Key("0"), proto.Key("1")},
		{proto.Key("a"), proto.Key("b")},
		{proto.Key("db0"), proto.Key("db1")},
		{proto.Key("\xfe"), proto.Key("\xff")},
		{engine.KeyMax, engine.KeyMax},
		{proto.Key("\xff\xff"), proto.Key("\xff\xff")},
	}

	for i, test := range testData {
		if !test.prefix.PrefixEnd().Equal(test.expEnd) {
			t.Errorf("%d: %q end key %q != %q", i, test.prefix, test.prefix.PrefixEnd(), test.expEnd)
		}
	}
}

// TestPrefixConfigSort verifies sorting of keys.
func TestPrefixConfigSort(t *testing.T) {
	defer leaktest.AfterTest(t)
	keys := []proto.Key{
		engine.KeyMax,
		proto.Key("c"),
		proto.Key("a"),
		proto.Key("b"),
		proto.Key("aa"),
		proto.Key("\xfe"),
		engine.KeyMin,
	}
	expKeys := []proto.Key{
		engine.KeyMin,
		proto.Key("a"),
		proto.Key("aa"),
		proto.Key("b"),
		proto.Key("c"),
		proto.Key("\xfe"),
		engine.KeyMax,
	}
	pcc := PrefixConfigMap{}
	for _, key := range keys {
		pcc = append(pcc, &PrefixConfig{key, nil, nil})
	}
	sort.Sort(pcc)
	for i, pc := range pcc {
		if bytes.Compare(pc.Prefix, expKeys[i]) != 0 {
			t.Errorf("order for index %d incorrect; expected %q, got %q", i, expKeys[i], pc.Prefix)
		}
	}
}

// TestPrefixConfigBuild adds prefixes and verifies they're
// sorted and proper end keys are generated.
func TestPrefixConfigBuild(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	expPrefixConfigs := []PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("/db1"), nil, config2},
		{proto.Key("/db1/table"), nil, config3},
		{proto.Key("/db1/tablf"), nil, config2},
		{proto.Key("/db2"), nil, config1},
		{proto.Key("/db3"), nil, config4},
		{proto.Key("/db4"), nil, config1},
	}
	verifyPrefixConfigMap(pcc, expPrefixConfigs, t)
}

func TestPrefixConfigMapDuplicates(t *testing.T) {
	defer leaktest.AfterTest(t)
	configs := []*PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("/db2"), nil, config2},
		{proto.Key("/db2"), nil, config3},
	}
	if _, err := NewPrefixConfigMap(configs); err == nil {
		log.Fatalf("expected an error building config map")
	}
}

func TestPrefixConfigSuccessivePrefixes(t *testing.T) {
	defer leaktest.AfterTest(t)
	configs := []*PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("/db2"), nil, config2},
		{proto.Key("/db2/table1"), nil, config3},
		{proto.Key("/db2/table2"), nil, config4},
		{proto.Key("/db3"), nil, config5},
	}
	pcc, err := NewPrefixConfigMap(configs)
	if err != nil {
		log.Fatalf("unexpected error building config map: %v", err)
	}
	expPrefixConfigs := []PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("/db2"), nil, config2},
		{proto.Key("/db2/table1"), nil, config3},
		{proto.Key("/db2/table2"), nil, config4},
		{proto.Key("/db2/table3"), nil, config2},
		{proto.Key("/db3"), nil, config5},
		{proto.Key("/db4"), nil, config1},
	}
	verifyPrefixConfigMap(pcc, expPrefixConfigs, t)
}

// TestMatchByPrefix verifies matching on longest prefix.
func TestMatchByPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	testData := []struct {
		key       proto.Key
		expConfig interface{}
	}{
		{engine.KeyMin, config1},
		{proto.Key("\x01"), config1},
		{proto.Key("/db"), config1},
		{proto.Key("/db1"), config2},
		{proto.Key("/db1/a"), config2},
		{proto.Key("/db1/table1"), config3},
		{proto.Key("/db1/table\xff"), config3},
		{proto.Key("/db2"), config1},
		{proto.Key("/db3"), config4},
		{proto.Key("/db3\xff"), config4},
		{proto.Key("/db5"), config1},
		{proto.Key("/xfe"), config1},
		{proto.Key("/xff"), config1},
	}
	for i, test := range testData {
		pc := pcc.MatchByPrefix(test.key)
		if test.expConfig != pc.Config {
			t.Errorf("%d: expected config %v for %q; got %v", i, test.expConfig, test.key, pc.Config)
		}
	}
}

// TestesMatchesByPrefix verifies all matching prefixes.
func TestMatchesByPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	testData := []struct {
		key        proto.Key
		expConfigs []interface{}
	}{
		{engine.KeyMin, []interface{}{config1}},
		{proto.Key("\x01"), []interface{}{config1}},
		{proto.Key("/db"), []interface{}{config1}},
		{proto.Key("/db1"), []interface{}{config2, config1}},
		{proto.Key("/db1/a"), []interface{}{config2, config1}},
		{proto.Key("/db1/table1"), []interface{}{config3, config2, config1}},
		{proto.Key("/db1/table\xff"), []interface{}{config3, config2, config1}},
		{proto.Key("/db2"), []interface{}{config1}},
		{proto.Key("/db3"), []interface{}{config4, config1}},
		{proto.Key("/db3\xff"), []interface{}{config4, config1}},
		{proto.Key("/db5"), []interface{}{config1}},
		{proto.Key("/xfe"), []interface{}{config1}},
		{proto.Key("/xff"), []interface{}{config1}},
	}
	for i, test := range testData {
		pcs := pcc.MatchesByPrefix(test.key)
		if len(pcs) != len(test.expConfigs) {
			t.Errorf("%d: expected %d matches, got %d", i, len(test.expConfigs), len(pcs))
			continue
		}
		for j, pc := range pcs {
			if pc.Config != test.expConfigs[j] {
				t.Errorf("%d: expected \"%d\"th config %v for %q; got %v", i, j, test.expConfigs[j], test.key, pc.Config)
			}
		}
	}
}

// TestSplitRangeByPrefixesErrors verifies various error conditions
// for splitting ranges.
func TestSplitRangeByPrefixesError(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc, err := NewPrefixConfigMap([]*PrefixConfig{})
	if err == nil {
		t.Error("expected error building config map with no default prefix")
	}
	pcc = buildTestPrefixConfigMap()
	// Key order is reversed.
	if _, err := pcc.SplitRangeByPrefixes(engine.KeyMax, engine.KeyMin); err == nil {
		t.Error("expected error with reversed keys")
	}
	// Same start and end keys.
	if _, err := pcc.SplitRangeByPrefixes(engine.KeyMin, engine.KeyMin); err != nil {
		t.Error("unexpected error with same start & end keys")
	}
}

// TestSplitRangeByPrefixes verifies splitting of a key range
// into sub-ranges based on config prefixes.
func TestSplitRangeByPrefixes(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	testData := []struct {
		start, end proto.Key
		expRanges  []*RangeResult
	}{
		// The full range.
		{engine.KeyMin, engine.KeyMax, []*RangeResult{
			{engine.KeyMin, proto.Key("/db1"), config1},
			{proto.Key("/db1"), proto.Key("/db1/table"), config2},
			{proto.Key("/db1/table"), proto.Key("/db1/tablf"), config3},
			{proto.Key("/db1/tablf"), proto.Key("/db2"), config2},
			{proto.Key("/db2"), proto.Key("/db3"), config1},
			{proto.Key("/db3"), proto.Key("/db4"), config4},
			{proto.Key("/db4"), engine.KeyMax, config1},
		}},
		// A subrange containing all databases.
		{proto.Key("/db"), proto.Key("/dc"), []*RangeResult{
			{proto.Key("/db"), proto.Key("/db1"), config1},
			{proto.Key("/db1"), proto.Key("/db1/table"), config2},
			{proto.Key("/db1/table"), proto.Key("/db1/tablf"), config3},
			{proto.Key("/db1/tablf"), proto.Key("/db2"), config2},
			{proto.Key("/db2"), proto.Key("/db3"), config1},
			{proto.Key("/db3"), proto.Key("/db4"), config4},
			{proto.Key("/db4"), proto.Key("/dc"), config1},
		}},
		// A subrange spanning from arbitrary points within zones.
		{proto.Key("/db1/a"), proto.Key("/db3/b"), []*RangeResult{
			{proto.Key("/db1/a"), proto.Key("/db1/table"), config2},
			{proto.Key("/db1/table"), proto.Key("/db1/tablf"), config3},
			{proto.Key("/db1/tablf"), proto.Key("/db2"), config2},
			{proto.Key("/db2"), proto.Key("/db3"), config1},
			{proto.Key("/db3"), proto.Key("/db3/b"), config4},
		}},
		// A subrange containing only /db1.
		{proto.Key("/db1"), proto.Key("/db2"), []*RangeResult{
			{proto.Key("/db1"), proto.Key("/db1/table"), config2},
			{proto.Key("/db1/table"), proto.Key("/db1/tablf"), config3},
			{proto.Key("/db1/tablf"), proto.Key("/db2"), config2},
		}},
		// A subrange containing only /db1/table.
		{proto.Key("/db1/table"), proto.Key("/db1/tablf"), []*RangeResult{
			{proto.Key("/db1/table"), proto.Key("/db1/tablf"), config3},
		}},
		// A subrange within /db1/table.
		{proto.Key("/db1/table3"), proto.Key("/db1/table4"), []*RangeResult{
			{proto.Key("/db1/table3"), proto.Key("/db1/table4"), config3},
		}},
	}
	for i, test := range testData {
		results, err := pcc.SplitRangeByPrefixes(test.start, test.end)
		if err != nil {
			t.Errorf("%d: unexpected error splitting ranges: %v", i, err)
		}
		if len(results) != len(test.expRanges) {
			t.Errorf("%d: expected %d matches, got %d:", i, len(test.expRanges), len(results))
			for j, r := range results {
				t.Errorf("match %d: %q, %q, %v", j, r.start, r.end, r.config)
			}
			continue
		}
		for j, r := range results {
			if bytes.Compare(r.start, test.expRanges[j].start) != 0 {
				t.Errorf("%d: expected \"%d\"th range start key %q, got %q", i, j, test.expRanges[j].start, r.start)
			}
			if bytes.Compare(r.end, test.expRanges[j].end) != 0 {
				t.Errorf("%d: expected \"%d\"th range end key %q, got %q", i, j, test.expRanges[j].end, r.end)
			}
			if r.config != test.expRanges[j].config {
				t.Errorf("%d: expected \"%d\"th range config %v, got %v", i, j, test.expRanges[j].config, r.config)
			}
		}
	}
}

// TestVisitPrefixesHierarchically verifies visitor pattern on a hierarchically
// matching set of prefixes from longest to shortest.
func TestVisitPrefixesHierarchically(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	var configs []interface{}
	pcc.VisitPrefixesHierarchically(proto.Key("/db1/table/1"), func(start, end proto.Key, config interface{}) (bool, error) {
		configs = append(configs, config)
		return false, nil
	})
	expConfigs := []interface{}{config3, config2, config1}
	if !reflect.DeepEqual(expConfigs, configs) {
		t.Errorf("expected configs %+v; got %+v", expConfigs, configs)
	}

	// Now, stop partway through by returning done=true.
	configs = []interface{}{}
	pcc.VisitPrefixesHierarchically(proto.Key("/db1/table/1"), func(start, end proto.Key, config interface{}) (bool, error) {
		configs = append(configs, config)
		if len(configs) == 2 {
			return true, nil
		}
		return false, nil
	})
	expConfigs = []interface{}{config3, config2}
	if !reflect.DeepEqual(expConfigs, configs) {
		t.Errorf("expected configs %+v; got %+v", expConfigs, configs)
	}

	// Now, stop partway through by returning an error.
	configs = []interface{}{}
	pcc.VisitPrefixesHierarchically(proto.Key("/db1/table/1"), func(start, end proto.Key, config interface{}) (bool, error) {
		configs = append(configs, config)
		if len(configs) == 2 {
			return false, util.Errorf("foo")
		}
		return false, nil
	})
	if !reflect.DeepEqual(expConfigs, configs) {
		t.Errorf("expected configs %+v; got %+v", expConfigs, configs)
	}
}

// TestVisitPrefixes verifies visitor pattern across matching prefixes.
func TestVisitPrefixes(t *testing.T) {
	defer leaktest.AfterTest(t)
	pcc := buildTestPrefixConfigMap()
	testData := []struct {
		start, end proto.Key
		expRanges  [][2]proto.Key
		expConfigs []interface{}
	}{
		{engine.KeyMin, engine.KeyMax,
			[][2]proto.Key{
				{engine.KeyMin, proto.Key("/db1")},
				{proto.Key("/db1"), proto.Key("/db1/table")},
				{proto.Key("/db1/table"), proto.Key("/db1/tablf")},
				{proto.Key("/db1/tablf"), proto.Key("/db2")},
				{proto.Key("/db2"), proto.Key("/db3")},
				{proto.Key("/db3"), proto.Key("/db4")},
				{proto.Key("/db4"), engine.KeyMax},
			}, []interface{}{config1, config2, config3, config2, config1, config4, config1}},
		{proto.Key("/db0"), proto.Key("/db1/table/foo"),
			[][2]proto.Key{
				{proto.Key("/db0"), proto.Key("/db1")},
				{proto.Key("/db1"), proto.Key("/db1/table")},
				{proto.Key("/db1/table"), proto.Key("/db1/table/foo")},
			}, []interface{}{config1, config2, config3}},
	}
	for i, test := range testData {
		ranges := [][2]proto.Key{}
		configs := []interface{}{}
		pcc.VisitPrefixes(test.start, test.end, func(start, end proto.Key, config interface{}) (bool, error) {
			ranges = append(ranges, [2]proto.Key{start, end})
			configs = append(configs, config)
			return false, nil
		})
		if !reflect.DeepEqual(test.expRanges, ranges) {
			t.Errorf("%d: expected ranges %+v; got %+v", i, test.expRanges, ranges)
		}
		if !reflect.DeepEqual(test.expConfigs, configs) {
			t.Errorf("%d: expected configs %+v; got %+v", i, test.expConfigs, configs)
		}
	}

	// Now, stop partway through by returning done=true.
	configs := []interface{}{}
	pcc.VisitPrefixes(proto.Key("/db2"), proto.Key("/db4"), func(start, end proto.Key, config interface{}) (bool, error) {
		configs = append(configs, config)
		if len(configs) == 2 {
			return true, nil
		}
		return false, nil
	})
	expConfigs := []interface{}{config1, config4}
	if !reflect.DeepEqual(expConfigs, configs) {
		t.Errorf("expected configs %+v; got %+v", expConfigs, configs)
	}

	// Now, stop partway through by returning an error.
	configs = []interface{}{}
	pcc.VisitPrefixes(proto.Key("/db2"), proto.Key("/db4"), func(start, end proto.Key, config interface{}) (bool, error) {
		configs = append(configs, config)
		if len(configs) == 2 {
			return false, util.Errorf("foo")
		}
		return false, nil
	})
	if !reflect.DeepEqual(expConfigs, configs) {
		t.Errorf("expected configs %+v; got %+v", expConfigs, configs)
	}
}
