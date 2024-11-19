// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigbounds"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/proto"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/require"
)

// TestDataDriven is a datadriven test to exercise spanconfigbounds.
// The test files are in testdata. The following syntax is provided:
//
//   - "let $<variable-name>"
//     Create a text variable for reuse of portions of a config or bounds.
//     The body of the input can be substituted into definitions in commands
//     which take proto text as input. The name of the variable must start with
//     a letter and be alphanumeric or _. Output is empty.
//
//   - bounds name=<name>
//     Defines a new bounds which can be used in later commands. The
//     body of the input is the protobuf text format for a
//     tenantcapabilitiespb.SpanConfigBounds. Variables defined in let can
//     be used. Output is empty.
//
//   - config name=<name>
//     Defines a new config which can be used in later commands. The semantics
//     are like bounds, but for a roachpb.SpanConfig. Output is empty.
//
//   - clamp bounds=<bounds-name> [config=<config-name>]
//     Clamps a config to the specified bounds. Either the input should be the
//     body of a SpanConfig, with semantics identical to the config command or
//     the name of a config should be supplied with the config arg. The output
//     is a diff between the input and the output.
//
//   - conforms bounds=<bounds-name> [config=<config-name>]
//     Determines whether a config conforms to the specified bounds. The input
//     semantics are identical to clamp, but the output is just a boolean.
//
//   - check bounds=<bounds-name> [config=<config-name>]
//     Checks whether a config conforms to the specified bounds, The input
//     semantics are identical to clamp, but the output is the detailed error
//     returned from the Check method on the Bounds, if there is an error.
//
//   - bounds-fields bounds=<bounds-name>
//     Prints the fields of the specified bounds. This is used to exercise the
//     field retrieval and printing logic for bounds.
//
//   - config-fields config=<config-name>
//     Prints the fields of the specified config. This is used to exercise the
//     field retrieval and printing logic for configs.
func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		bounds := make(map[string]*spanconfigbounds.Bounds)
		configs := make(map[string]*roachpb.SpanConfig)
		vars := make(map[string]string)
		variableRegexp := regexp.MustCompile(`(?P<prefix>^|\s)(?P<name>\$[A-Za-z][A-Za-z0-9_]*)(?P<suffix>$|\s)`)
		var (
			variablePrefixIndex = variableRegexp.SubexpIndex("prefix")
			variableNameIndex   = variableRegexp.SubexpIndex("name")
			variableSuffixIndex = variableRegexp.SubexpIndex("suffix")
		)
		datadriven.RunTest(t, path, func(tt *testing.T, d *datadriven.TestData) string {
			t := &dt{T: tt, d: d}
			expand := func(s string) string {
				return variableRegexp.ReplaceAllStringFunc(s, func(s string) string {
					submatches := variableRegexp.FindStringSubmatch(s)
					name := submatches[variableNameIndex]
					require.Contains(t, vars, name)
					return submatches[variablePrefixIndex] + vars[name] + submatches[variableSuffixIndex]
				})
			}
			getBounds := func() *spanconfigbounds.Bounds {
				var boundsName string
				d.ScanArgs(tt, "bounds", &boundsName)
				require.Contains(t, bounds, boundsName, "bounds")
				return bounds[boundsName]
			}
			getConfig := func() *roachpb.SpanConfig {
				var configName string
				d.ScanArgs(tt, "config", &configName)
				require.Contains(t, configs, configName, "config")
				return configs[configName]
			}
			getBoundsAndConfig := func() (*spanconfigbounds.Bounds, *roachpb.SpanConfig) {
				var cfg *roachpb.SpanConfig
				if d.HasArg("config") {
					require.Zero(t, d.Input)
					cfg = protoutil.Clone(getConfig()).(*roachpb.SpanConfig)
				} else {
					cfg = &roachpb.SpanConfig{}
					require.NoError(t, proto.UnmarshalText(expand(d.Input), cfg))
				}
				return getBounds(), cfg
			}
			switch d.Cmd {
			case "let":
				require.Len(t, d.CmdArgs, 1)
				name := d.CmdArgs[0].Key
				require.Regexp(t, variableRegexp, name)
				require.NotContains(t, vars, name)
				vars[name] = d.Input
				return ""
			case "bounds":
				// I guess what I'm going to do is go yaml to json to protobuf and pray.
				var name string
				d.ScanArgs(tt, "name", &name)
				require.NotContains(t, bounds, name)
				var b tenantcapabilitiespb.SpanConfigBounds
				require.NoError(t, proto.UnmarshalText(expand(d.Input), &b))
				bounds[name] = spanconfigbounds.New(&b)
				return ""
			case "config":
				// I guess what I'm going to do is go yaml to json to protobuf and pray.
				var name string
				d.ScanArgs(tt, "name", &name)
				require.NotContains(t, configs, name)
				var cfg roachpb.SpanConfig
				require.NoError(t, proto.UnmarshalText(expand(d.Input), &cfg))
				configs[name] = &cfg
				return ""
			case "clamp":
				bounds, cfg := getBoundsAndConfig()
				orig := proto.MarshalTextString(cfg)
				changed := bounds.Clamp(cfg)
				after := proto.MarshalTextString(cfg)
				out, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
					A:       difflib.SplitLines(orig),
					B:       difflib.SplitLines(after),
					Context: 4,
				})
				require.NoError(t, err)
				if changed {
					require.NotZero(t, out, "clamp said it changed something, but did not")
				} else {
					require.Zero(t, out, "clamp said it changed nothing, but there's a diff")
				}
				return out
			case "conforms":
				return fmt.Sprint(
					(*spanconfigbounds.Bounds).Conforms(getBoundsAndConfig()))
			case "check":
				err := (*spanconfigbounds.Bounds).Check(getBoundsAndConfig()).AsError()
				// Exercise both the short and long form of the error.
				return fmt.Sprintf("%v\n%+v", err, err)
			case "bounds-fields":
				b := getBounds()
				var buf strings.Builder
				spanconfigbounds.ForEachField(func(field spanconfigbounds.Field) {
					if buf.Len() > 0 {
						buf.WriteRune('\n')
					}
					_, _ = fmt.Fprintf(&buf, "%s: %s", field, field.FieldBound(b))
				})
				return buf.String()
			case "config-fields":
				cfg := getConfig()
				var buf strings.Builder
				spanconfigbounds.ForEachField(func(field spanconfigbounds.Field) {
					if buf.Len() > 0 {
						buf.WriteRune('\n')
					}
					_, _ = fmt.Fprintf(&buf, "%s: %s", field, field.FieldValue(cfg))
				})
				return buf.String()
			default:
				d.Fatalf(t, "unknown command")
				panic("unreachable")
			}
		})
	})
}

type dt struct {
	*testing.T
	d *datadriven.TestData
}

func (d dt) Errorf(format string, args ...interface{}) {
	d.T.Helper()
	d.T.Errorf("%s: %s", d.d.Pos, fmt.Sprintf(format, args...))
}

var _ require.TestingT = (*dt)(nil)
