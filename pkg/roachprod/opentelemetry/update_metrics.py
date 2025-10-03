#!/usr/bin/env python3

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

import os
import re
import subprocess
from datetime import datetime

def parse_metrics(yaml_file):
    """Parse metrics.yaml and extract name -> exported_name mapping."""
    metrics_map = {}

    with open(yaml_file, 'r') as f:
        lines = f.readlines()

    current_name = None
    for line in lines:
        # Match metric name.
        name_match = re.match(r'    - name: (.+)', line)
        if name_match:
            current_name = name_match.group(1)
            continue

        # Match exported_name (should come right after name).
        if current_name:
            exported_match = re.match(r'      exported_name: (.+)', line)
            if exported_match:
                exported_name = exported_match.group(1)
                metrics_map[exported_name] = current_name
            else:
                print(f"Unexpectedly didn't find exported_name for metric {current_name}")
            current_name = None

    return metrics_map

def generate_go_code(metrics_map):
    """Generate Go code matching cockroachdb_metrics.go format."""

    lines = []
    lines.append(f"// Copyright 2024 The Cockroach Authors.")
    lines.append("//")
    lines.append("// Use of this software is governed by the CockroachDB Software License")
    lines.append("// included in the /LICENSE file.")
    lines.append("")
    lines.append("package opentelemetry")
    lines.append("")
    lines.append("// cockroachdbMetrics is a mapping of CockroachDB metric names to cockroachdb")
    lines.append("// Datadog integration metric names. This allows CockroachDB metrics to comply")
    lines.append("// with the naming requirements for the cockroachdb Datadog integration listed")
    lines.append("// in its metadata.csv.")
    lines.append("// - https://github.com/DataDog/integrations-core/blob/master/cockroachdb/metadata.csv")
    lines.append("// Only metrics whose name matches a key in this map will have its")
    lines.append("// corresponding value exported from the OpenTelemetry Collector.")
    lines.append("var cockroachdbMetrics = map[string]string{")

    # Sort by exported_name (the key)
    sorted_metrics = sorted(metrics_map.items())

    for exported_name, metric_name in sorted_metrics:
        lines.append(f'\t"{exported_name}": "{metric_name}",')

    lines.append("}")

    return '\n'.join(lines) + '\n'

def main():
    # Get the directory containing this script.
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Build paths relative to script location.
    yaml_file = os.path.join(script_dir, '../../../docs/generated/metrics/metrics.yaml')
    output_file = os.path.join(script_dir, 'cockroachdb_metrics.go')

    metrics_map = parse_metrics(yaml_file)
    go_code = generate_go_code(metrics_map)

    # Write to output file.
    with open(output_file, 'w') as f:
        f.write(go_code)

    # Run go fmt on the generated file.
    try:
        subprocess.run(['go', 'fmt', output_file], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Warning: go fmt failed: {e}")

    print(f"Extracted {len(metrics_map)} metrics to {output_file}")

if __name__ == '__main__':
    main()
