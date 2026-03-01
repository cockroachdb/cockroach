# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#!/usr/bin/env python3
"""
EngFlow artifact downloader.

Uses engflow_auth CLI for authentication and curl for downloads.
No browser needed.

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


ENGFLOW_HOST = "mesolite.cluster.engflow.com"
ENGFLOW_URL = f"https://{ENGFLOW_HOST}"
CAS_API = f"{ENGFLOW_URL}/api/contentaddressablestorage/v1/instances/default/blobs"
GRPC_URL = f"{ENGFLOW_URL}/engflow.resultstore.v1alpha.ResultStore"


def get_token():
    """Get auth token from engflow_auth CLI."""
    result = subprocess.run(
        ["engflow_auth", "export", ENGFLOW_HOST],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"Error: engflow_auth export failed: {result.stderr}", file=sys.stderr)
        print("Run: engflow_auth login mesolite.cluster.engflow.com", file=sys.stderr)
        sys.exit(1)
    data = json.loads(result.stdout)
    return data["token"]["access_token"]


def encode_varint(val):
    """Encode an integer as a protobuf varint."""
    result = b""
    while val > 0x7F:
        result += bytes([(val & 0x7F) | 0x80])
        val >>= 7
    result += bytes([val])
    return result


def encode_string_field(field_num, value):
    """Encode a string as a protobuf field."""
    tag = (field_num << 3) | 2
    encoded = value.encode("utf-8")
    return bytes([tag]) + encode_varint(len(encoded)) + encoded


def encode_bytes_field(field_num, value):
    """Encode bytes as a protobuf field."""
    tag = (field_num << 3) | 2
    return bytes([tag]) + encode_varint(len(value)) + value


def make_grpc_frame(proto_bytes):
    """Wrap protobuf bytes in a gRPC frame (5-byte header)."""
    return struct.pack(">BI", 0, len(proto_bytes)) + proto_bytes


def grpc_call(token, method, proto_bytes):
    """Make a gRPC-web call via curl and return the response protobuf bytes.

    Python's urllib mangles headers in a way that EngFlow's server rejects
    (returns HTML instead of gRPC), so we shell out to curl.
    """
    frame = make_grpc_frame(proto_bytes)
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
                "-H", f"x-engflow-auth-method: jwt-v0",
                "-H", f"x-engflow-auth-token: {token}",
                "-H", f"Cookie: x-engflow-auth={token}",
                url,
            ],
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

    flag = data[0]
    payload_len = struct.unpack(">I", data[1:5])[0]
    if flag == 0x80:
        # Trailer-only response (error).
        trailer = data[5:5 + payload_len].decode("utf-8", errors="replace")
        print(f"gRPC error: {trailer}", file=sys.stderr)
        return b""
    return data[5:5 + payload_len]


def download_blob(token, blob_hash, blob_size, outfile):
    """Download a blob from the CAS API via curl."""
    url = f"{CAS_API}/{blob_hash}/{blob_size}"
    result = subprocess.run(
        [
            "curl", "-s",
            "-H", f"Cookie: x-engflow-auth={token}",
            "-H", f"x-engflow-auth-method: jwt-v0",
            "-H", f"x-engflow-auth-token: {token}",
            "-o", outfile,
            url,
        ],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"  Error downloading blob: {result.stderr.decode()}", file=sys.stderr)
        return 0
    return os.path.getsize(outfile)


def get_target_labels_by_status(token, instance, invocation_id, status_code):
    """Call GetTargetLabelsByStatus and return target labels.

    Status codes observed from the EngFlow UI:
      0x08 = passed, 0x09 = failed
    The status is encoded as a single-byte length-delimited field (field 3).
    """
    proto = encode_string_field(1, instance)
    proto += encode_string_field(2, invocation_id)
    # Field 3 is a single-byte length-delimited value (nested enum).
    proto += encode_bytes_field(3, bytes([status_code]))
    # Field 4 is a varint limit.
    proto += bytes([(4 << 3) | 0]) + encode_varint(100)
    resp = grpc_call(token, "GetTargetLabelsByStatus", proto)
    if not resp:
        return []
    text = resp.decode("latin-1")
    return [s for s in re.findall(r"//[^\x00-\x1f]+", text) if not s.startswith("///")]


def get_target(token, instance, invocation_id, target_label):
    """Call GetTarget and return the raw protobuf response."""
    proto = encode_string_field(1, instance)
    proto += encode_string_field(2, invocation_id)
    proto += encode_string_field(3, target_label)
    return grpc_call(token, "GetTarget", proto)


def decode_varint(data, pos):
    """Decode a protobuf varint starting at pos. Returns (value, new_pos)."""
    val = 0
    shift = 0
    while pos < len(data):
        b = data[pos]
        val |= (b & 0x7F) << shift
        shift += 7
        pos += 1
        if not (b & 0x80):
            break
    return val, pos


def find_compact_digests(data):
    """Find CompactDigest entries in raw protobuf bytes.

    CompactDigest (from engflow.type.CompactDigest):
      field 1 (tag 0x0a) = bytes, 32-byte SHA-256 hash
      field 2 (tag 0x10) = uint64, size in bytes

    Returns list of (offset, hex_hash, size) tuples.
    """
    results = []
    for i in range(len(data) - 35):
        if data[i] == 0x0a and data[i + 1] == 0x20:
            raw_hash = data[i + 2:i + 34]
            if i + 34 < len(data) and data[i + 34] == 0x10:
                size_val, _ = decode_varint(data, i + 35)
                results.append((i, raw_hash.hex(), size_val))
    return results


def extract_shard_artifacts(proto_bytes):
    """Extract artifact blob references from GetTarget response.

    The response interleaves two types of blob references per shard:
      - CompactDigest (raw binary hash): used for test.xml (JUnit results)
      - bytestream:// URL (hex hash): used for test.log, outputs.zip, manifest

    The first CompactDigest is the test binary (very large); remaining ones
    are test.xml entries, each followed by bytestream URLs for that shard.
    """
    text = proto_bytes.decode("latin-1")

    # Collect all entries with offsets for ordering.
    entries = []

    # CompactDigest entries (test.xml).
    for offset, hex_hash, size in find_compact_digests(proto_bytes):
        entries.append(("test.xml", offset, hex_hash, size))

    # Bytestream URL entries (test.log, outputs.zip, manifest).
    artifact_pattern = (
        r"(test\.log|test\.outputs__outputs\.zip|"
        r"test\.outputs_manifest__MANIFEST)"
        r"[^\x00]*?bytestream://[^/]+/blobs/([0-9a-f]{64})/(\d+)"
    )
    for m in re.finditer(artifact_pattern, text):
        name = m.group(1)
        if name == "test.outputs__outputs.zip":
            key = "outputs.zip"
        elif name == "test.outputs_manifest__MANIFEST":
            key = "manifest"
        else:
            key = name
        entries.append((key, m.start(), m.group(2), int(m.group(3))))

    entries.sort(key=lambda x: x[1])

    # Skip the first CompactDigest if it's much larger than the rest (test binary).
    if entries and entries[0][0] == "test.xml" and entries[0][3] > 100_000_000:
        entries = entries[1:]

    # Group into shards. Each shard starts with a test.xml CompactDigest,
    # followed by test.log (and optionally outputs.zip + manifest).
    shards = []
    current_shard = {}
    for key, _, blob_hash, size in entries:
        if key == "test.xml" and current_shard:
            shards.append(current_shard)
            current_shard = {}
        current_shard[key] = (blob_hash, size)

    if current_shard:
        shards.append(current_shard)

    return shards


def parse_invocation_url(url):
    """Parse an EngFlow invocation URL into components.

    Returns (invocation_id, target_label, shard_num, run_num, attempt_num).
    """
    # Extract invocation ID
    inv_match = re.search(r"/invocations/default/([0-9a-f-]+)", url)
    if not inv_match:
        print(f"Error: cannot parse invocation ID from URL: {url}", file=sys.stderr)
        sys.exit(1)
    invocation_id = inv_match.group(1)

    # Extract target from URL fragment
    target_label = None
    frag_match = re.search(r"#targets-([A-Za-z0-9+/=_-]+)", url)
    if frag_match:
        import base64
        try:
            target_label = base64.b64decode(frag_match.group(1) + "==").decode("utf-8").strip("\x00")
        except Exception:
            pass

    # Extract shard/run/attempt from query params
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


def cmd_targets(args):
    """List targets for an invocation, optionally filtered by status."""
    token = get_token()
    invocation_id = args.invocation_id

    if args.all:
        # Try both passed (0x08) and failed (0x09).
        labels = set()
        for code in [0x08, 0x09]:
            labels.update(get_target_labels_by_status(token, "default", invocation_id, code))
        labels = sorted(labels)
        print(f"All targets ({len(labels)}):")
    else:
        labels = get_target_labels_by_status(token, "default", invocation_id, 0x09)
        if not labels:
            print("No failed targets found. Use --all to list all targets.")
            return
        print(f"Failed targets ({len(labels)}):")

    for label in labels:
        print(f"  {label}")


def cmd_list(args):
    """List artifacts for a target."""
    token = get_token()

    if args.url:
        invocation_id, target_label, shard, _, _ = parse_invocation_url(args.url)
        if not target_label:
            print("Error: could not extract target from URL. "
                  "Use --target to specify it.", file=sys.stderr)
            sys.exit(1)
    else:
        invocation_id = args.invocation_id
        target_label = args.target
        shard = args.shard

    proto = get_target(token, "default", invocation_id, target_label)
    if not proto:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    shards = extract_shard_artifacts(proto)
    print(f"Target: {target_label}")
    print(f"Invocation: {invocation_id}")
    print(f"Total shards with artifacts: {len(shards)}")
    print()

    for i, shard_data in enumerate(shards):
        if shard is not None and i != shard:
            continue
        print(f"Shard {i}:")
        for name, (blob_hash, size) in sorted(shard_data.items()):
            size_str = f"{size:,}" if size < 1048576 else f"{size/1048576:.1f}MB"
            print(f"  {name:40s} {blob_hash} ({size_str})")
        print()


def cmd_download(args):
    """Download artifacts for a target shard."""
    token = get_token()

    if args.url:
        invocation_id, target_label, shard, _, _ = parse_invocation_url(args.url)
        if not target_label:
            print("Error: could not extract target from URL. "
                  "Use --target to specify it.", file=sys.stderr)
            sys.exit(1)
        if shard is None:
            shard = args.shard
    else:
        invocation_id = args.invocation_id
        target_label = args.target
        shard = args.shard

    if shard is None:
        print("Error: --shard is required (or provide a URL with testReportShard param)",
              file=sys.stderr)
        sys.exit(1)

    proto = get_target(token, "default", invocation_id, target_label)
    if not proto:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    shards = extract_shard_artifacts(proto)
    if shard >= len(shards):
        print(f"Error: shard {shard} not found (have {len(shards)} shards)",
              file=sys.stderr)
        sys.exit(1)

    shard_data = shards[shard]
    outdir = args.outdir or "/tmp/engflow-artifacts"
    os.makedirs(outdir, exist_ok=True)

    artifacts_to_download = args.artifacts or ["test.log", "test.xml", "outputs.zip"]

    for name in artifacts_to_download:
        if name not in shard_data:
            print(f"  {name}: not found in shard {shard}")
            continue
        blob_hash, size = shard_data[name]
        outfile = os.path.join(outdir, name)
        print(f"  Downloading {name} ({size:,} bytes)...")
        downloaded = download_blob(token, blob_hash, size, outfile)
        print(f"  Saved to {outfile} ({downloaded:,} bytes)")

    # Auto-extract outputs.zip
    if "outputs.zip" in artifacts_to_download and "outputs.zip" in shard_data:
        zip_path = os.path.join(outdir, "outputs.zip")
        if os.path.exists(zip_path):
            print(f"\n  Extracting outputs.zip...")
            subprocess.run(["unzip", "-o", zip_path, "-d", outdir],
                           capture_output=True)
            print(f"  Extracted to {outdir}/")


def cmd_blob(args):
    """Download a specific blob by hash and size."""
    token = get_token()
    outfile = args.outfile or f"/tmp/engflow-artifacts/{args.hash[:12]}"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)

    print(f"Downloading blob {args.hash}/{args.size}...")
    downloaded = download_blob(token, args.hash, args.size, outfile)
    print(f"Saved to {outfile} ({downloaded:,} bytes)")


def cmd_url(args):
    """Download artifacts directly from an EngFlow invocation URL."""
    token = get_token()
    invocation_id, target_label, shard, _, _ = parse_invocation_url(args.url)

    if not target_label:
        print("Error: could not extract target from URL fragment.", file=sys.stderr)
        print("Provide a URL with #targets-<base64> fragment.", file=sys.stderr)
        sys.exit(1)

    if shard is None:
        print("Error: URL has no testReportShard param. Use --shard.", file=sys.stderr)
        sys.exit(1)

    print(f"Invocation: {invocation_id}")
    print(f"Target: {target_label}")
    print(f"Shard: {shard}")
    print()

    proto = get_target(token, "default", invocation_id, target_label)
    if not proto:
        print("Error: GetTarget returned empty response", file=sys.stderr)
        sys.exit(1)

    shards = extract_shard_artifacts(proto)
    if shard >= len(shards):
        print(f"Error: shard {shard} not found (have {len(shards)} shards)",
              file=sys.stderr)
        sys.exit(1)

    shard_data = shards[shard]
    outdir = args.outdir or "/tmp/engflow-artifacts"
    os.makedirs(outdir, exist_ok=True)

    for name in ["test.log", "test.xml", "outputs.zip"]:
        if name not in shard_data:
            print(f"  {name}: not available")
            continue
        blob_hash, size = shard_data[name]
        outfile = os.path.join(outdir, name)
        print(f"  Downloading {name} ({size:,} bytes)...")
        downloaded = download_blob(token, blob_hash, size, outfile)
        print(f"  Saved to {outfile} ({downloaded:,} bytes)")

    # Auto-extract outputs.zip
    if "outputs.zip" in shard_data:
        zip_path = os.path.join(outdir, "outputs.zip")
        if os.path.exists(zip_path):
            print(f"\n  Extracting outputs.zip...")
            subprocess.run(["unzip", "-o", zip_path, "-d", outdir],
                           capture_output=True)
            print(f"  Extracted to {outdir}/")


def main():
    parser = argparse.ArgumentParser(description="EngFlow artifact downloader")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # targets command
    targets_parser = subparsers.add_parser("targets", help="List targets in an invocation")
    targets_parser.add_argument("invocation_id", help="Invocation ID")
    targets_parser.add_argument("--all", action="store_true", help="Show all targets, not just failed")
    targets_parser.set_defaults(func=cmd_targets)

    # list command
    list_parser = subparsers.add_parser("list", help="List artifacts for a target")
    list_parser.add_argument("--url", help="EngFlow invocation URL")
    list_parser.add_argument("invocation_id", nargs="?", help="Invocation ID")
    list_parser.add_argument("--target", help="Target label")
    list_parser.add_argument("--shard", type=int, help="Show only this shard")
    list_parser.set_defaults(func=cmd_list)

    # download command
    dl_parser = subparsers.add_parser("download", help="Download artifacts")
    dl_parser.add_argument("--url", help="EngFlow invocation URL")
    dl_parser.add_argument("invocation_id", nargs="?", help="Invocation ID")
    dl_parser.add_argument("--target", help="Target label")
    dl_parser.add_argument("--shard", type=int, help="Shard number")
    dl_parser.add_argument("--outdir", help="Output directory")
    dl_parser.add_argument("--artifacts", nargs="+",
                           choices=["test.log", "test.xml", "outputs.zip", "manifest"],
                           help="Which artifacts to download")
    dl_parser.set_defaults(func=cmd_download)

    # blob command
    blob_parser = subparsers.add_parser("blob", help="Download a specific blob")
    blob_parser.add_argument("hash", help="Blob SHA-256 hash")
    blob_parser.add_argument("size", help="Blob size in bytes")
    blob_parser.add_argument("--outfile", help="Output file path")
    blob_parser.set_defaults(func=cmd_blob)

    # url command (convenience)
    url_parser = subparsers.add_parser("url", help="Download from invocation URL")
    url_parser.add_argument("url", help="Full EngFlow invocation URL")
    url_parser.add_argument("--outdir", help="Output directory")
    url_parser.set_defaults(func=cmd_url)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
