- Feature Name: Recovery from Permanent Loss of Quorum
- Status: draft
- Start Date: 2021-09-23
- Authors: @tbg
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/17186

# Summary

We propose a plan to productionize our tooling for recovery from loss
of quorum for use in situations in which operators want to restore service and
allow as much data to be recovered, without regard for its consistency. The plan revolves around extensions of `cockroach debug unsafe-remove-dead-replicas` that primarily improve the UX and the detection of situations in which the tool cannot be used. Possible future extensions are outlined as well.

I'm trying something mildly new here by keeping the technical discussion in a Gdoc in the early stages of the RFC: 

[Inconsistent Recovery from Permanent Loss of Quorum](https://docs.google.com/document/d/1CyO7qhYF7tU0igvzk7k0ScdP9ySMgqmtiZ_hydBTfrA/edit?usp=sharing)
