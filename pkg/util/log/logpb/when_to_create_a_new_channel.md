# When to create a new logging channel

In general, we should avoid creating new logging channels.

Additional logging channels increase the surface area of logging 
configuration and introduce more points of confusion for customers 
and operators in their deployments. We should strive to find ways 
to satisfy logging needs through existing channels rather than 
creating new ones.

Here are some valid motivations for introducing new logging channels.
Generally, it's recommended that your new channel fulfills at least 
2 of these criteria before moving forward:

1. There is a _collection_ of related logging activity that can be 
   logically grouped separately from all other logs.
   - For instance: `SQL_SCHEMA` is a channel that groups together 
     all logs pertaining to schema changes. This is helpful because 
     this information could otherwise be scattered across many 
     different logs. 
2. There is a strong need for _routing_ a subset of logging 
   information differently from all other channels. (Note that 
   logging infrastructure already provides tooling for 
   differentiating between different sources or categorizations of 
   logs.)
   - For instance: `USER_ADMIN` and `PRIVILEGES` are likely to be 
     important for security audit purposes and exist to help with 
     routing to a security team, rather than a DB operations team.
3. There is a pattern of logging traffic that is at risk of 
   crowding-out other low-frequency data by forcing more frequent 
   file rotations.

Ideally, these needs should be clearly expressed in the issue linked 
to the PR that introduces a new logging channel so that there is a 
justification for its need.
