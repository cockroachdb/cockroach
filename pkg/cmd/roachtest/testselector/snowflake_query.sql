with ts as (
  select current_date() as t -- this returns the current date
), builds as (
  -- select all the build IDs in the last "forPastDays" days
  select
    ID as run_id,
    min(start_date) as first_run, -- get the first time the test was run
    max(start_date) as last_run, -- -- get the last time the test was run
  from DATAMART_PROD.TEAMCITY.BUILDS, ts
  where
    start_date > dateadd(DAY, ?, ts.t) -- last "forPastDays" days
    and lower(status) = 'success' -- consider only successful builds
    and branch_name = ? -- consider the builds from target branch
    and lower(name) like ? -- name is based on the suite and cloud e.g. '%roachtest nightly - gce%'
  group by 1
), test_stats as (
  -- for all the build IDs select do a inner join on all the tests
  select test_name, -- distinct test names
         count(case when status='SUCCESS' then test_name end) as total_successful_runs, -- all successful runs
         -- count the number of failed tests. tests failed due to preempted VMs are ignored.
         count(case when status='FAILURE' and details not like '%VMs preempted during the test run%' then test_name end) as failure_count,
         -- record the latest details of the test. this is useful for identifying if the latest failure of
         -- the test is due to infra failure
         MAX_BY(details, b.last_run) as recent_details,
         MAX_BY(ignore_details, b.last_run) as recent_ignore_details,
         MAX_BY(status, b.last_run) as last_status,
         -- get the first_run and last_run only if the status is not UNKNOWN. This returns nil for runs that have never run
         min(case when status!='UNKNOWN' then b.first_run end) as first_run,
         max(case when status!='UNKNOWN' then b.last_run end) as last_run,
         sum(case when status='SUCCESS' then duration end) as total_duration -- the total duration of the tests that are successful
  from DATAMART_PROD.TEAMCITY.TESTS t
         -- inner join as explained in the beginning
         inner join builds b on
    b.run_id=t.build_id
  where test_name is not null -- not interested in tests with no names
  group  by 1
)
select
  test_name,
  case when
         -- mark as selected if
          -- the test has failed at least once in the past "forPastDays" days
          -- recently added test - test has not run for more than "firstRunOn" days
          -- the test has not been run for last "lastRunOn" days
          -- the last failure was VM preemption
         failure_count > 0 or
         first_run > dateadd(DAY, ?, ts.t) or
         last_run < dateadd(DAY, ?, ts.t) or
         recent_details like '%VMs preempted during the test run%' or
          -- last_status='UNKNOWN' can be for the following scenarios:
           -- test is run in the past, but added to skip by the test writer
            -- In this scenario, we want to select the test as we do not know when user may mark it to not skipped
           -- test is not run due to incompatibility
            -- here also, user can make it compatible at any point. So, we always select.
           -- test is never run - we always select
           -- test is not run by test selector in the last run
            -- The test should not be selected by default in this case and should be selected based on the sorted unselected list
         (last_status='UNKNOWN' and recent_ignore_details!='test selector')
         then 'yes' else 'no' end as selected,
  -- average duration - this is set to 0 if the test is never run (total_successful_runs=0)
  case when total_successful_runs > 0 then total_duration/total_successful_runs else 0 end as avg_duration,
  -- indicates the last failure was due to infra flake
  case when recent_details like '%VMs preempted during the test run%' then 'yes' else 'no' end as last_failure_is_preeempt,
from test_stats, ts
order by selected desc, last_run -- selected="yes" appears first. Rest of the tests are sorted by the last run.
