- Feature Name: Easier Hash Sharded Indexes
- Status: draft
- Start Date: 2019-10-28
- Authors: Aayush Shah, Andrew Werner
- RFC PR: PR # after acceptance of initial draft
- Cockroach Issue: [#39340] (https://github.com/cockroachdb/cockroach/issues/39340)

# Summary

This is a proposal to provide better UX for creating hash sharded indexes through easier
syntax. This allows a useful mechanism to alleviate hot spots due to sequential write
workloads. 

# Motivation

In 19.2, we added optimizer support to automatically add filters based on check
constraints. This can allow users to alleviate single range hotspots during sequential
workloads by creating an index on a computed shard column. However, this feature still
requires some relatively unattractive syntax to manually add a computed column which will
act as the shard key.

![Latency](https://user-images.githubusercontent.com/10788754/67722397-63c5a100-f9af-11e9-8316-a173e52fe20b.png)