// Copyright 2016 The Cockroach Authors.
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

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// statementsValue is an implementation of pflag.Value that appends any
// argument to a slice.
type statementsValue []string

func (s *statementsValue) String() string {
	return strings.Join(*s, ";")
}

func (s *statementsValue) Type() string {
	return "statementsValue"
}

func (s *statementsValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type cliContext struct {
	// Embed the base context.
	*base.Config

	// tableDisplayFormat indicates how to format result tables.
	tableDisplayFormat tableDisplayFormat

	// showTimes indicates whether to display query times after each result line.
	showTimes bool
}

var serverCfg = func() server.Config {
	st := cluster.MakeClusterSettings()
	// This is the real cluster settings object that is used by users' servers,
	// and which should receive updates from associated Updaters.
	st.Manual.Store(false)

	// The server package has its own copy of the singleton for use in the
	// /debug/requests handler.
	server.ClusterSettings = st
	// A similar singleton exists in the log package. See comment there.
	f := log.ReportingSettings(st.ReportingSettings)
	log.ReportingSettingsSingleton.Store(&f)

	return server.MakeConfig(st)
}()

var baseCfg = serverCfg.Config
var cliCtx = cliContext{Config: baseCfg}

type tableDisplayFormat int

const (
	tableDisplayTSV tableDisplayFormat = iota
	tableDisplayCSV
	tableDisplayPretty
	tableDisplayRecords
	tableDisplaySQL
	tableDisplayHTML
	tableDisplayRaw
)

// Type implements the pflag.Value interface.
func (f *tableDisplayFormat) Type() string { return "string" }

// String implements the pflag.Value interface.
func (f *tableDisplayFormat) String() string {
	switch *f {
	case tableDisplayTSV:
		return "tsv"
	case tableDisplayCSV:
		return "csv"
	case tableDisplayPretty:
		return "pretty"
	case tableDisplayRecords:
		return "records"
	case tableDisplaySQL:
		return "sql"
	case tableDisplayHTML:
		return "html"
	case tableDisplayRaw:
		return "raw"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (f *tableDisplayFormat) Set(s string) error {
	switch s {
	case "tsv":
		*f = tableDisplayTSV
	case "csv":
		*f = tableDisplayCSV
	case "pretty":
		*f = tableDisplayPretty
	case "records":
		*f = tableDisplayRecords
	case "sql":
		*f = tableDisplaySQL
	case "html":
		*f = tableDisplayHTML
	case "raw":
		*f = tableDisplayRaw
	default:
		return fmt.Errorf("invalid table display format: %s "+
			"(possible values: tsv, csv, pretty, records, sql, html, raw)", s)
	}
	return nil
}

// sqlCtx captures the command-line parameters of the `sql` command.
var sqlCtx = struct {
	*cliContext

	// execStmts is a list of statements to execute.
	execStmts statementsValue
}{cliContext: &cliCtx}

// dumpCtx captures the command-line parameters of the `sql` command.
var dumpCtx = struct {
	// dumpMode determines which part of the database should be dumped.
	dumpMode dumpMode

	// asOf determines the time stamp at which the dump should be taken.
	asOf string
}{
	dumpMode: dumpBoth,
}

type dumpMode int

const (
	dumpBoth dumpMode = iota
	dumpSchemaOnly
	dumpDataOnly
)

// Type implements the pflag.Value interface.
func (m *dumpMode) Type() string { return "string" }

// String implements the pflag.Value interface.
func (m *dumpMode) String() string {
	switch *m {
	case dumpBoth:
		return "both"
	case dumpSchemaOnly:
		return "schema"
	case dumpDataOnly:
		return "data"
	}
	return ""
}

// Set implements the pflag.Value interface.
func (m *dumpMode) Set(s string) error {
	switch s {
	case "both":
		*m = dumpBoth
	case "schema":
		*m = dumpSchemaOnly
	case "data":
		*m = dumpDataOnly
	default:
		return fmt.Errorf("invalid value for --dump-mode: %s", s)
	}
	return nil
}

type keyType int

//go:generate stringer -type=keyType
const (
	raw keyType = iota
	human
	rangeID
)

var _keyTypes []string

func keyTypes() []string {
	if _keyTypes == nil {
		for i := 0; i+1 < len(_keyType_index); i++ {
			_keyTypes = append(_keyTypes, _keyType_name[_keyType_index[i]:_keyType_index[i+1]])
		}
	}
	return _keyTypes
}

func parseKeyType(value string) (keyType, error) {
	for i, typ := range keyTypes() {
		if strings.EqualFold(value, typ) {
			return keyType(i), nil
		}
	}
	return 0, fmt.Errorf("unknown key type '%s'", value)
}

// unquoteArg unquotes the provided argument using Go double-quoted
// string literal rules.
func unquoteArg(arg string) (string, error) {
	s, err := strconv.Unquote(`"` + arg + `"`)
	if err != nil {
		return "", errors.Wrapf(err, "invalid argument %q", arg)
	}
	return s, nil
}

type mvccKey engine.MVCCKey

func (k *mvccKey) String() string {
	return engine.MVCCKey(*k).String()
}

func (k *mvccKey) Set(value string) error {
	var typ keyType
	var keyStr string
	i := strings.IndexByte(value, ':')
	if i == -1 {
		keyStr = value
	} else {
		var err error
		typ, err = parseKeyType(value[:i])
		if err != nil {
			return err
		}
		keyStr = value[i+1:]
	}

	switch typ {
	case raw:
		unquoted, err := unquoteArg(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(roachpb.Key(unquoted)))
	case human:
		key, err := keys.UglyPrint(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(key))
	case rangeID:
		fromID, err := parseRangeID(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(keys.MakeRangeIDPrefix(fromID)))
	default:
		return fmt.Errorf("unknown key type %s", typ)
	}

	return nil
}

func (k *mvccKey) Type() string {
	return "engine.MVCCKey"
}

// debugCtx captures the command-line parameters of the `debug` command.
var debugCtx = struct {
	startKey, endKey  engine.MVCCKey
	values            bool
	sizes             bool
	replicated        bool
	inputFile         string
	printSystemConfig bool
}{
	startKey: engine.NilKey,
	endKey:   engine.MVCCKeyMax,
}

// zoneCtx captures the command-line parameters of the `zone` command.
var zoneCtx struct {
	zoneConfig             string
	zoneDisableReplication bool
}

// startCtx captures the command-line arguments for the `start` command.
var startCtx struct {
	// server-specific values of some flags.
	serverInsecure    bool
	serverSSLCertsDir string
}

// quitCtx captures the command-line parameters of the `quit` command.
var quitCtx struct {
	serverDecommission bool
}

// nodeCtx captures the command-line parameters of the `node` command.
var nodeCtx = struct {
	nodeDecommissionWait nodeDecommissionWaitType
}{
	nodeDecommissionWait: nodeDecommissionWaitAll,
}

type nodeDecommissionWaitType int

const (
	nodeDecommissionWaitAll nodeDecommissionWaitType = iota
	nodeDecommissionWaitLive
	nodeDecommissionWaitNone
)

func (s *nodeDecommissionWaitType) String() string {
	switch *s {
	case nodeDecommissionWaitAll:
		return "all"
	case nodeDecommissionWaitLive:
		return "live"
	case nodeDecommissionWaitNone:
		return "none"
	}
	return ""
}

func (s *nodeDecommissionWaitType) Type() string {
	return "string"
}

func (s *nodeDecommissionWaitType) Set(value string) error {
	switch value {
	case "all":
		*s = nodeDecommissionWaitAll
	case "live":
		*s = nodeDecommissionWaitLive
	case "none":
		*s = nodeDecommissionWaitNone
	default:
		return fmt.Errorf("invalid node decommission parameter: %s "+
			"(possible values: all, live, none)", value)
	}
	return nil
}
