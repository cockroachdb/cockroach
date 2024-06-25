with builds as (
  -- select all the build IDs in the last "forPastDays" days
  select
    ID as run_id,
    min(start_date) as first_run, -- get the first time the test was run
    max(start_date) as last_run, -- -- get the last time the test was run
  from DATAMART_PROD.TEAMCITY.BUILDS
  where
    start_date > dateadd(DAY, ?, current_date()) -- last "forPastDays" days
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
         failure_count > 0 or -- the test has failed at least once in the past "forPastDays" days
         first_run > dateadd(DAY, ?, current_date()) or -- recently added test - test has not run for more than "firstRunOn" days
         last_run < dateadd(DAY, ?, current_date()) or -- the test has not been run for last "lastRunOn" days
         last_run is null -- the test is always ignored till now or have never been run
         then 'yes' else 'no' end as selected,
  -- average duration - this is set to 0 if the test is never run (total_successful_runs=0)
  case when total_successful_runs > 0 then total_duration/total_successful_runs else 0 end as avg_duration,
  total_successful_runs,
from test_stats
order by selected desc, total_successful_runs
