// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func Example_debug_decode_key_value() {
	TestCLI{}.RunWithArgs([]string{"debug", "decode-value", "016b12bd8980c0b6c2e211ba5182000172647363", "884d186d03089b09120bbd8980c0b6c2e211ba51821a0bbd8980c0b9e7c5c610e99622060801100118012206080410041802220608021002180428053004"})

	// Output:
	// debug decode-value 016b12bd8980c0b6c2e211ba5182000172647363 884d186d03089b09120bbd8980c0b6c2e211ba51821a0bbd8980c0b9e7c5c610e99622060801100118012206080410041802220608021002180428053004
	// unable to decode key: invalid encoded mvcc key: 016b12bd8980c0b6c2e211ba5182000172647363, assuming it's a roachpb.Key with fake timestamp;
	// if the result below looks like garbage, then it likely is:
	//
	// 0.987654321,0 /Local/Range/Table/53/1/-4560243296450227838/RangeDescriptor (0x016b12bd8980c0b6c2e211ba518200017264736300000000003ade68b109): [/Table/53/1/-4560243296450227838, /Table/53/1/-4559358311118345834)
	// 	Raw:r1179:/Table/53/1/-45{60243296450227838-59358311118345834} [(n1,s1):1, (n4,s4):2, (n2,s2):4, next=5, gen=4]
}

func TestDebugKeysHex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	storePath := filepath.Join(baseDir, "store")
	createStore(t, storePath)

	{
		out, err := TestCLI{}.RunWithCapture("debug keys " + storePath +
			" --from hex:016b12bd898090d79e52e79b0144000174786e2d733fb094e9aa4d67974c71486b9823b900" +
			" --to   hex:016b12bd898090d79e52e79b0144000174786e2d733fb094e9aa4d67974c71486b9823b900")
		if err != nil {
			t.Fatal(err)
		}
		// Should just output the command invocation and no results.
		if !strings.HasSuffix(strings.TrimSpace(out), "b900") {
			t.Fatalf("%q", out)
		}
	}

	// Test invalid key, verify we get a good suggestion back.
	out, err := TestCLI{}.RunWithCapture("debug keys " + storePath +
		" --to hex:01")
	if err != nil {
		t.Fatal(err)
	}
	expOut := `invalid argument "hex:01" for "--to" flag: perhaps this is just a hex-encoded key; ` +
		`you need an encoded MVCCKey (i.e. with a timestamp component); here's one with a zero timestamp: ` +
		`0100: invalid encoded mvcc key: 01`
	if !strings.Contains(out, expOut) {
		t.Fatalf("%q", out)
	}
}

func TestDebugDecodeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	data := []struct {
		key, result, encoding string
		userKey               bool
	}{
		{
			key:      "f4891273656174746c6500011270a3d70a3d704400ff8000ff00ff00ff00ff00ff00ffdc000100000000000001e240000000020d",
			encoding: "hex",
			result:   "/Table/108/1/\"seattle\"/\"p\\xa3\\xd7\\n=pD\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xdc\"/0.000123456,2",
		},
		{
			key:      "9IkSc2VhdHRsZQABEnCj1wo9cEQA/4AA/wD/AP8A/wD/AP/cAAEAAAAAAAAB4kAAAAACDQ==",
			encoding: "base64",
			result:   "/Table/108/1/\"seattle\"/\"p\\xa3\\xd7\\n=pD\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xdc\"/0.000123456,2",
		},
		{
			key:      "f4891273656174746c6500011270a3d70a3d704400ff8000ff00ff00ff00ff00ff00ffdc0001",
			encoding: "hex",
			userKey:  true,
			result:   "/Table/108/1/\"seattle\"/\"p\\xa3\\xd7\\n=pD\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xdc\"",
		},
		{
			key:      "9IkSc2VhdHRsZQABEnCj1wo9cEQA/4AA/wD/AP8A/wD/AP/cAAE=",
			encoding: "base64",
			userKey:  true,
			result:   "/Table/108/1/\"seattle\"/\"p\\xa3\\xd7\\n=pD\\x00\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\xdc\"",
		},
		{
			key:      "jg==",
			encoding: "base64",
			result:   "ERROR: invalid encoded mvcc key: 8e",
		},
	}
	for _, d := range data {
		t.Run(d.key, func(t *testing.T) {
			out, err := TestCLI{}.RunWithCaptureArgs([]string{
				"debug",
				"decode-key",
				"--encoding",
				d.encoding,
				fmt.Sprintf("--user-key=%t", d.userKey),
				d.key,
			})
			require.NoError(t, err, "failed to run cli")
			require.Contains(t, out, "\n"+d.result+"\n")
		})
	}
}
