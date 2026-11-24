#!/usr/bin/env python3
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
"""
EngFlow artifact downloader.

Authentication (in priority order):
  1. mTLS certificates: set ENGFLOW_CERT_FILE and ENGFLOW_KEY_FILE env vars
  2. JWT from env: set ENGFLOW_TOKEN env var
  3. JWT from CLI: uses engflow_auth export (requires prior login)

Usage:
    # List all targets and their artifacts for an invocation
    python3 engflow_artifacts.py list <invocation_id> --target <target_label>

    # Download artifacts for a specific shard
    python3 engflow_artifacts.py download <invocation_id> --target <target> --shard N

    # Download a specific blob by hash/size
    python3 engflow_artifacts.py blob <hash> <size> [--outfile FILE]

    # Discover failed targets in an invocation
    python3 engflow_artifacts.py targets <invocation_id>
"""

import argparse
import json
import os
import re
import struct
import subprocess
import sys
import tempfile

# Generated from resultstore.proto — the reverse-engineered subset of
# EngFlow's internal ResultStore v1alpha API that we need for artifact
# downloading. See the proto file for field documentation.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import resultstore_pb2 as pb


ENGFLOW_HOST = "mesolite.cluster.engflow.com"
ENGFLOW_URL = f"https://{ENGFLOW_HOST}"
CAS_API = f"{ENGFLOW_URL}/api/contentaddressablestorage/v1/instances/default/blobs"
GRPC_URL = f"{ENGFLOW_URL}/engflow.resultstore.v1alpha.ResultStore"

# Artifact name mappings from EngFlow's internal names to user-facing names.
_ARTIFACT_NAMES = {
    "test.outputs__outputs.zip": "outputs.zip",
    "test.outputs_manifest__MANIFEST": "manifest",
}


def get_curl_auth_args():
    """Return curl arguments for EngFlow authentication.

    Tries, in order:
      1. mTLS — ENGFLOW_CERT_FILE and ENGFLOW_KEY_FILE env vars
      2. JWT from env — ENGFLOW_TOKEN env var
      3. JWT from CLI — engflow_auth export (interactive login required)
    """
    cert = os.environ.get("ENGFLOW_CERT_FILE")
    key = os.environ.get("ENGFLOW_KEY_FILE")
    if cert and key:
        return ["--cert", cert, "--key", key]

    token = os.environ.get("ENGFLOW_TOKEN")
    if not token:
        try:
            result = subprocess.run(
                ["engflow_auth", "export", ENGFLOW_HOST],
                capture_output=True, text=True,
            )
        except FileNotFoundError:
            print("Error: engflow_auth not found on PATH.", file=sys.stderr)
            print(
                "Install engflow_auth, or set ENGFLOW_TOKEN, or configure "
                "ENGFLOW_CERT_FILE and ENGFLOW_KEY_FILE.",
                file=sys.stderr,
            )
            sys.exit(1)

        if result.returncode != 0:
            print(
                f"Error: engflow_auth export failed: {result.stderr}",
                file=sys.stderr,
            )
            print(
                "Run: engflow_auth login mesolite.cluster.engflow.com",
                file=sys.stderr,
            )
            sys.exit(1)

        try:
            data = json.loads(result.stdout)
            token = data["token"]["access_token"]
        except (json.JSONDecodeError, KeyError):
            print(
                "Error: Failed to parse access token from engflow_auth output.",
                file=sys.stderr,
            )
            print(
                "Try running: engflow_auth login mesolite.cluster.engflow.com "
                "or set ENGFLOW_TOKEN directly.",
                file=sys.stderr,
            )
            sys.exit(1)

    return [
        "-H", "x-engflow-auth-method: jwt-v0",
        "-H", f"x-engflow-auth-token: {token}",
        "-H", f"Cookie: x-engflow-auth={token}",
    ]


def grpc_call(auth_args, method, proto_bytes):
    """Make a gRPC-web call via curl and return the response protobuf bytes.

    EngFlow's gRPC-web endpoint requires HTTP/2 — it serves the web UI HTML
    over HTTP/1.1. We use curl because it negotiates HTTP/2 via ALPN, while
    Python's requests/urllib only speak HTTP/1.1.

    The gRPC-web framing is a 5-byte header (1 byte flags + 4 byte big-endian
    length) around each protobuf message. Large responses may span multiple
    data frames; a trailer frame (flag 0x80) carries the gRPC status.
    """
    frame = struct.pack(">BI", 0, len(proto_bytes)) + proto_bytes
    url = f"{GRPC_URL}/{method}"

    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tmp:
        tmp.write(frame)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            [
                "curl", "-s", "--data-binary", f"@{tmp_path}",
                "-H", "Content-Type: application/grpc-web+proto",
                "-H", "x-grpc-web: 1",
            ] + auth_args + [url],
            capture_output=True,
        )
    finally:
        os.unlink(tmp_path)

    data = result.stdout
    if result.returncode != 0 or len(data) < 5:
        print(f"gRPC call to {method} failed", file=sys.stderr)
        if result.stderr:
            print(f"  curl error: {result.stderr.decode()}", file=sys.stderr)
        return b""

    # Parse gRPC-web frames. Data frames have flag=0x00; the trailer
    # frame has flag=0x80. Large responses may span multiple data frames.
    payload = b""
    pos = 0
    while pos + 5 <= len(data):
        flag = data[pos]
        frame_len = struct.unpack(">I", data[pos + 1:pos + 5])[0]
        if flag == 0x80:
            trailer = data[pos + 5:pos + 5 + frame_len].decode(
                "utf-8", errors="replace",
            )
            if "grpc-status: 0" not in trailer and "grpc-status:0" not in trailer:
                print(f"gRPC error: {trailer}", file=sys.stderr)
                return b""
            break
        payload += data[pos + 5:pos + 5 + frame_len]
        pos += 5 + frame_len
    return payload


def download_blob(auth_args, blob_hash, blob_size, outfile):
    """Download a blob from the CAS API via curl."""
    url = f"{CAS_API}/{blob_hash}/{blob_size}"
    result = subprocess.run(
        ["curl", "-s"] + auth_args + ["-o", outfile, url],
        capture_output=True,
    )
    if result.returncode != 0:
        print(
            f"  Error downloading blob: {result.stderr.decode()}",
            file=sys.stderr,
        )
        return 0
    return os.path.getsize(outfile)


def get_target_labels_by_status(auth_args, instance, invocation_id, status_code):
    """Call GetTargetLabelsByStatus and return target labels.

    Status codes: 0x08 = passed, 0x09 = failed.
    """
    req = pb.GetTargetLabelsByStatusRequest(
        instance=instance,
        invocation_id=invocation_id,
        status=bytes([status_code]),
        limit=100,
    )
    resp_bytes = grpc_call(
        auth_args, "GetTargetLabelsByStatus", req.SerializeToString(),
    )
    if not resp_bytes:
        return []
    return parse_labels_response(resp_bytes)


def parse_target_response(resp_bytes):
    """Parse a GetTarget response into test actions keyed by (shard, run).

    Returns a dict mapping (shard_number, run_number) to a dict of artifacts:
      {(shard, run): {"test.xml": (hash, size), "test.log": (hash, size), ...}}
    Also returns (total_shards, total_entries) as a second value.
    """
    resp = pb.GetTargetResponse()
    resp.ParseFromString(resp_bytes)

    collection = resp.target.info.test_suite.test_actions
    actions = {}
    for action in collection.actions:
        shard_artifacts = {}
        data = action.data

        # test.xml from CompactDigest.
        if data.test_xml_digest.size_bytes > 0:
            shard_artifacts["test.xml"] = (
                data.test_xml_digest.hash.hex(),
                data.test_xml_digest.size_bytes,
            )

        # test.log.
        if data.test_log.bytestream_uri:
            blob_hash, size = _parse_bytestream_uri(
                data.test_log.bytestream_uri,
            )
            if blob_hash:
                shard_artifacts["test.log"] = (blob_hash, size)

        # outputs.zip, manifest.
        for ref in data.output_files:
            name = _ARTIFACT_NAMES.get(ref.name, ref.name)
            if ref.bytestream_uri:
                blob_hash, size = _parse_bytestream_uri(ref.bytestream_uri)
                if blob_hash:
                    shard_artifacts[name] = (blob_hash, size)

        actions[(action.shard, action.run)] = shard_artifacts

    return actions, (collection.total_shards, collection.total_entries)


def parse_labels_response(resp_bytes):
    """Parse a GetTargetLabelsByStatus response into a list of target labels."""
    resp = pb.GetTargetLabelsByStatusResponse()
    resp.ParseFromString(resp_bytes)
    return list(resp.labels)


def get_target(auth_args, instance, invocation_id, target_label):
    """Call GetTarget and return parsed test actions. See parse_target_response."""
    req = pb.GetTargetRequest(
        instance=instance,
        invocation_id=invocation_id,
        target_label=target_label,
    )
    resp_bytes = grpc_call(auth_args, "GetTarget", req.SerializeToString())
    if not resp_bytes:
        return {}, (0, 0)
    return parse_target_response(resp_bytes)


def _parse_bytestream_uri(uri):
    """Extract (hash, size) from a bytestream:// URI.

    Format: bytestream://host/blobs/<sha256>/<size>
    """
    m = re.search(r"/blobs/([0-9a-f]{64})/(\d+)", uri)
    if not m:
        return None, 0
    return m.group(1), int(m.group(2))


def parse_invocation_url(url):
    """Parse an EngFlow invocation URL into components.

    Returns (invocation_id, target_label, shard, run, attempt).
    """
    inv_match = re.search(r"/invocations/default/([0-9a-f-]+)", url)
    if not inv_match:
        print(
            f"Error: cannot parse invocation ID from URL: {url}",
            file=sys.stderr,
        )
        sys.exit(1)
    invocation_id = inv_match.group(1)

    target_label = None
    frag_match = re.search(r"#targets-([A-Za-z0-9+/=_-]+)", url)
    if frag_match:
        import base64
        try:
            target_label = base64.b64decode(
                frag_match.group(1) + "=="
            ).decode("utf-8").strip("\x00")
        except Exception:
            pass

    shard = None
    run = None
    attempt = None
    shard_match = re.search(r"testReportShard=(\d+)", url)
    if shard_match:
        shard = int(shard_match.group(1))
    run_match = re.search(r"testReportRun=(\d+)", url)
    if run_match:
        run = int(run_match.group(1))
    attempt_match = re.search(r"testReportAttempt=(\d+)", url)
    if attempt_match:
        attempt = int(attempt_match.group(1))

    return invocation_id, target_label, shard, run, attempt


def _resolve_args(args, need_shard=False):
    """Extract invocation_id, target_label, shard, run from args or --url."""
    if getattr(args, "url", None):
        invocation_id, target_label, shard, run, _ = parse_invocation_url(
            args.url,
        )
        if not target_label:
            print(
                "Error: could not extract target from URL. "
                "Use --target to specify it.",
                file=sys.stderr,
            )
            sys.exit(1)
        if args.shard is not None:
            shard = args.shard
        if getattr(args, "run", None) is not None:
            run = getattr(args, "run")
    else:
        invocation_id = args.invocation_id
        target_label = args.target
        shard = args.shard
        run = getattr(args, "run", None)

    if run is None:
        run = 1

    if need_shard and shard is None:
        print(
            "Error: --shard is required "
            "(or provide a URL with testReportShard param)",
            file=sys.stderr,
        )
        sys.exit(1)

    return invocation_id, target_label, shard, run


def cmd_targets(args):
    """List targets for an invocation, optionally filtered by status."""
    auth_args = get_curl_auth_args()
    invocation_id = args.invocation_id

    if args.all:
        labels = set()
        for code in [0x08, 0x09]:
            labels.update(
                get_target_labels_by_status(
                    auth_args, "default", invocation_id, code,
                ),
            )
        labels = sorted(labels)
        print(f"All targets ({len(labels)}):")
    else:
        labels = get_target_labels_by_status(
            auth_args, "default", invocation_id, 0x09,
        )
        if not labels:
            print("No failed targets found. Use --all to list all targets.")
            return
        print(f"Failed targets ({len(labels)}):")

    for label in labels:
        print(f"  {label}")


def cmd_list(args):
    """List artifacts for a target."""
    auth_args = get_curl_auth_args()
    invocation_id, target_label, filter_shard, filter_run = _resolve_args(args)

    actions, (total_shards, total_entries) = get_target(
        auth_args, "default", invocation_id, target_label,
    )
    if not actions:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    # Collect unique shard numbers, sorted.
    shard_nums = sorted({s for s, r in actions})
    run_nums = sorted({r for s, r in actions})

    print(f"Target: {target_label}")
    print(f"Invocation: {invocation_id}")
    print(f"Shards: {len(shard_nums)}, Runs: {len(run_nums)}")
    print()

    for shard_num in shard_nums:
        if filter_shard is not None and shard_num != filter_shard:
            continue
        for run_num in run_nums:
            if filter_shard is not None and run_num != filter_run:
                continue
            key = (shard_num, run_num)
            if key not in actions:
                continue
            shard_data = actions[key]
            header = f"Shard {shard_num}"
            if len(run_nums) > 1:
                header += f" (run {run_num})"
            print(f"{header}:")
            for name, (blob_hash, size) in sorted(shard_data.items()):
                if size < 1048576:
                    size_str = f"{size:,}"
                else:
                    size_str = f"{size / 1048576:.1f}MB"
                print(f"  {name:40s} {blob_hash} ({size_str})")
            print()


def _download_shard(auth_args, shard_data, shard, outdir, artifact_names):
    """Download artifacts for a single shard."""
    os.makedirs(outdir, exist_ok=True)

    for name in artifact_names:
        if name not in shard_data:
            print(f"  {name}: not found in shard {shard}")
            continue
        blob_hash, size = shard_data[name]
        outfile = os.path.join(outdir, name)
        print(f"  Downloading {name} ({size:,} bytes)...")
        downloaded = download_blob(auth_args, blob_hash, size, outfile)
        print(f"  Saved to {outfile} ({downloaded:,} bytes)")

    # Auto-extract outputs.zip.
    if "outputs.zip" in artifact_names:
        zip_path = os.path.join(outdir, "outputs.zip")
        if os.path.exists(zip_path):
            print("\n  Extracting outputs.zip...")
            subprocess.run(
                ["unzip", "-o", zip_path, "-d", outdir],
                capture_output=True,
            )
            print(f"  Extracted to {outdir}/")


def cmd_download(args):
    """Download artifacts for a target shard."""
    auth_args = get_curl_auth_args()
    invocation_id, target_label, shard, run = _resolve_args(
        args, need_shard=True,
    )

    actions, _ = get_target(
        auth_args, "default", invocation_id, target_label,
    )
    if not actions:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    key = (shard, run)
    if key not in actions:
        available = sorted({s for s, r in actions})
        print(
            f"Error: shard {shard} run {run} not found "
            f"(available shards: {available})",
            file=sys.stderr,
        )
        sys.exit(1)

    artifact_names = args.artifacts or ["test.log", "test.xml", "outputs.zip"]
    outdir = args.outdir or "/tmp/engflow-artifacts"
    _download_shard(auth_args, actions[key], shard, outdir, artifact_names)


def cmd_blob(args):
    """Download a specific blob by hash and size."""
    auth_args = get_curl_auth_args()
    outfile = args.outfile or f"/tmp/engflow-artifacts/{args.hash[:12]}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    print(f"Downloading blob {args.hash}/{args.size}...")
    downloaded = download_blob(auth_args, args.hash, args.size, outfile)
    print(f"Saved to {outfile} ({downloaded:,} bytes)")


def cmd_url(args):
    """Download artifacts directly from an EngFlow invocation URL."""
    auth_args = get_curl_auth_args()
    invocation_id, target_label, shard, run, _ = parse_invocation_url(
        args.url,
    )

    if not target_label:
        print(
            "Error: could not extract target from URL fragment.",
            file=sys.stderr,
        )
        print(
            "Provide a URL with #targets-<base64> fragment.",
            file=sys.stderr,
        )
        sys.exit(1)

    if shard is None:
        print(
            "Error: URL has no testReportShard param. Use --shard.",
            file=sys.stderr,
        )
        sys.exit(1)

    if run is None:
        run = 1

    print(f"Invocation: {invocation_id}")
    print(f"Target: {target_label}")
    print(f"Shard: {shard}, Run: {run}")
    print()

    actions, _ = get_target(
        auth_args, "default", invocation_id, target_label,
    )
    if not actions:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    key = (shard, run)
    if key not in actions:
        available = sorted({s for s, r in actions})
        print(
            f"Error: shard {shard} run {run} not found "
            f"(available shards: {available})",
            file=sys.stderr,
        )
        sys.exit(1)

    outdir = args.outdir or "/tmp/engflow-artifacts"
    _download_shard(
        auth_args, actions[key], shard, outdir,
        ["test.log", "test.xml", "outputs.zip"],
    )


def main():
    parser = argparse.ArgumentParser(description="EngFlow artifact downloader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # targets command
    targets_parser = subparsers.add_parser(
        "targets", help="List targets in an invocation",
    )
    targets_parser.add_argument("invocation_id", help="Invocation ID")
    targets_parser.add_argument(
        "--all", action="store_true",
        help="Show all targets, not just failed",
    )
    targets_parser.set_defaults(func=cmd_targets)

    # list command
    list_parser = subparsers.add_parser(
        "list", help="List artifacts for a target",
    )
    list_parser.add_argument("--url", help="EngFlow invocation URL")
    list_parser.add_argument(
        "invocation_id", nargs="?", help="Invocation ID",
    )
    list_parser.add_argument("--target", help="Target label")
    list_parser.add_argument(
        "--shard", type=int, help="Show only this shard",
    )
    list_parser.add_argument(
        "--run", type=int, help="Show only this run (default: 1)",
    )
    list_parser.set_defaults(func=cmd_list)

    # download command
    dl_parser = subparsers.add_parser("download", help="Download artifacts")
    dl_parser.add_argument("--url", help="EngFlow invocation URL")
    dl_parser.add_argument(
        "invocation_id", nargs="?", help="Invocation ID",
    )
    dl_parser.add_argument("--target", help="Target label")
    dl_parser.add_argument("--shard", type=int, help="Shard number")
    dl_parser.add_argument(
        "--run", type=int, help="Run number (default: 1)",
    )
    dl_parser.add_argument("--outdir", help="Output directory")
    dl_parser.add_argument(
        "--artifacts", nargs="+",
        choices=["test.log", "test.xml", "outputs.zip", "manifest"],
        help="Which artifacts to download",
    )
    dl_parser.set_defaults(func=cmd_download)

    # blob command
    blob_parser = subparsers.add_parser(
        "blob", help="Download a specific blob",
    )
    blob_parser.add_argument("hash", help="Blob SHA-256 hash")
    blob_parser.add_argument("size", help="Blob size in bytes")
    blob_parser.add_argument("--outfile", help="Output file path")
    blob_parser.set_defaults(func=cmd_blob)

    # url command (convenience)
    url_parser = subparsers.add_parser(
        "url", help="Download from invocation URL",
    )
    url_parser.add_argument("url", help="Full EngFlow invocation URL")
    url_parser.add_argument("--outdir", help="Output directory")
    url_parser.set_defaults(func=cmd_url)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
