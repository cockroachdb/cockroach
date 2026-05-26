#!/usr/bin/env python3
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
"""Tests for engflow_artifacts.py using checked-in protobuf response payloads.

Run:  python3 -m pytest engflow_artifacts_test.py -v
  or: python3 engflow_artifacts_test.py
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from engflow_artifacts import (
    parse_labels_response,
    parse_target_response,
    parse_invocation_url,
    _parse_bytestream_uri,
)

TESTDATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata")


def _load_fixture(name):
    with open(os.path.join(TESTDATA, name), "rb") as f:
        return f.read()


def _load_golden(name):
    with open(os.path.join(TESTDATA, name)) as f:
        return json.load(f)


class TestParseLabelsResponse(unittest.TestCase):
    """Tests for GetTargetLabelsByStatus response parsing."""

    def test_golden(self):
        """Parse labels_response.bin and compare to golden output."""
        data = _load_fixture("labels_response.bin")
        labels = parse_labels_response(data)
        expected = _load_golden("labels_response.golden.json")
        self.assertEqual(labels, expected)

    def test_labels_are_sorted_target_strings(self):
        data = _load_fixture("labels_response.bin")
        labels = parse_labels_response(data)
        for label in labels:
            self.assertTrue(
                label.startswith("//"),
                f"Expected Bazel target label, got: {label}",
            )

    def test_empty_response(self):
        labels = parse_labels_response(b"")
        self.assertEqual(labels, [])


class TestParseTargetResponse(unittest.TestCase):
    """Tests for GetTarget response parsing."""

    def test_oidcccl_golden(self):
        """Parse target_oidcccl.bin (1 shard, 3 runs) against golden."""
        data = _load_fixture("target_oidcccl.bin")
        actions, (total_shards, total_entries) = parse_target_response(data)
        golden = _load_golden("target_oidcccl.golden.json")

        self.assertEqual(total_shards, golden["total_shards"])
        self.assertEqual(total_entries, golden["total_entries"])

        got = {
            f"{s},{r}": {k: list(v) for k, v in artifacts.items()}
            for (s, r), artifacts in sorted(actions.items())
        }
        self.assertEqual(got, golden["actions"])

    def test_kvserver_golden(self):
        """Parse target_kvserver.bin (48 shards, 3 runs) against golden."""
        data = _load_fixture("target_kvserver.bin")
        actions, (total_shards, total_entries) = parse_target_response(data)
        golden = _load_golden("target_kvserver.golden.json")

        self.assertEqual(total_shards, golden["total_shards"])
        self.assertEqual(total_entries, golden["total_entries"])

        got = {
            f"{s},{r}": {k: list(v) for k, v in artifacts.items()}
            for (s, r), artifacts in sorted(actions.items())
        }
        self.assertEqual(got, golden["actions"])

    def test_kvserver_shard_count(self):
        """kvserver_test should have 48 unique shards, 3 runs each."""
        data = _load_fixture("target_kvserver.bin")
        actions, (total_shards, total_entries) = parse_target_response(data)

        shard_nums = sorted({s for s, r in actions})
        run_nums = sorted({r for s, r in actions})

        self.assertEqual(len(shard_nums), 48)
        self.assertEqual(shard_nums, list(range(1, 49)))
        self.assertEqual(run_nums, [1, 2, 3])
        self.assertEqual(len(actions), 144)  # 48 * 3

    def test_kvserver_shard_20_has_outputs(self):
        """Shard 20 run 1 is the failing shard and should have outputs.zip."""
        data = _load_fixture("target_kvserver.bin")
        actions, _ = parse_target_response(data)

        shard_20 = actions[(20, 1)]
        self.assertIn("test.log", shard_20)
        self.assertIn("test.xml", shard_20)
        self.assertIn("outputs.zip", shard_20)
        self.assertIn("manifest", shard_20)

        # The test.xml for the failing shard should be notably larger
        # than a passing shard's test.xml (contains stack traces).
        failing_xml_size = shard_20["test.xml"][1]
        passing_xml_size = actions[(1, 1)]["test.xml"][1]
        self.assertGreater(failing_xml_size, passing_xml_size * 5)

    def test_kvserver_passing_shard_no_outputs(self):
        """A passing shard (e.g. shard 1 run 1) should NOT have outputs.zip."""
        data = _load_fixture("target_kvserver.bin")
        actions, _ = parse_target_response(data)

        shard_1 = actions[(1, 1)]
        self.assertIn("test.log", shard_1)
        self.assertIn("test.xml", shard_1)
        self.assertNotIn("outputs.zip", shard_1)
        self.assertNotIn("manifest", shard_1)

    def test_oidcccl_single_shard_three_runs(self):
        """oidcccl_test has 1 shard and 3 runs."""
        data = _load_fixture("target_oidcccl.bin")
        actions, _ = parse_target_response(data)

        shard_nums = {s for s, r in actions}
        run_nums = {r for s, r in actions}

        self.assertEqual(shard_nums, {1})
        self.assertEqual(run_nums, {1, 2, 3})

    def test_artifact_hashes_are_valid_sha256(self):
        """All artifact hashes should be 64-char hex strings."""
        data = _load_fixture("target_oidcccl.bin")
        actions, _ = parse_target_response(data)

        for (shard, run), artifacts in actions.items():
            for name, (blob_hash, size) in artifacts.items():
                self.assertEqual(
                    len(blob_hash), 64,
                    f"shard {shard} run {run} {name}: "
                    f"hash length {len(blob_hash)} != 64",
                )
                int(blob_hash, 16)  # Should not raise.
                self.assertGreater(size, 0)


class TestParseInvocationUrl(unittest.TestCase):
    """Tests for EngFlow invocation URL parsing."""

    def test_full_url(self):
        url = (
            "https://mesolite.cluster.engflow.com/invocations/default/"
            "0f3c3923-460d-4399-8718-df764fdcf793"
            "?testReportRun=1&testReportShard=20&testReportAttempt=1"
            "#targets-Ly9wa2cva3Yva3ZzZXJ2ZXI6a3ZzZXJ2ZXJfdGVzdA=="
        )
        inv_id, target, shard, run, attempt = parse_invocation_url(url)
        self.assertEqual(inv_id, "0f3c3923-460d-4399-8718-df764fdcf793")
        self.assertEqual(target, "//pkg/kv/kvserver:kvserver_test")
        self.assertEqual(shard, 20)
        self.assertEqual(run, 1)
        self.assertEqual(attempt, 1)

    def test_url_without_params(self):
        url = (
            "https://mesolite.cluster.engflow.com/invocations/default/"
            "abc12345-1234-5678-9abc-def012345678"
        )
        inv_id, target, shard, run, attempt = parse_invocation_url(url)
        self.assertEqual(inv_id, "abc12345-1234-5678-9abc-def012345678")
        self.assertIsNone(target)
        self.assertIsNone(shard)
        self.assertIsNone(run)
        self.assertIsNone(attempt)


class TestParseBytestreamUri(unittest.TestCase):
    """Tests for bytestream:// URI parsing."""

    def test_valid_uri(self):
        uri = (
            "bytestream://mesolite.cluster.engflow.com/blobs/"
            "ef3bac04483976e0dc5b77760c14f3d4cf482159d0cc5d6bd58240bc67fe825f"
            "/15787"
        )
        blob_hash, size = _parse_bytestream_uri(uri)
        self.assertEqual(
            blob_hash,
            "ef3bac04483976e0dc5b77760c14f3d4cf482159d0cc5d6bd58240bc67fe825f",
        )
        self.assertEqual(size, 15787)

    def test_invalid_uri(self):
        blob_hash, size = _parse_bytestream_uri("not-a-uri")
        self.assertIsNone(blob_hash)
        self.assertEqual(size, 0)


if __name__ == "__main__":
    unittest.main()
