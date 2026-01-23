-- NB: The TC_BUILDS thing is duplicated in failed-tc-engflow-tests.sql.
-- If it changes in one place, it should probably change in the other.
WITH TC_BUILDS AS (
    SELECT
        id AS build_id,
        name AS build_name,
        build_type_id AS build_type
    FROM
        DATAMART_PROD.TEAMCITY.BUILDS
    WHERE
        branch_name = 'master' AND
        start_date > dateadd(DAY, -?, current_date()) AND
        -- exclude roachtest
        build_name NOT ILIKE '%roachtest%' AND
        -- exclude managed-service builds
        build_type NOT LIKE 'Internal_ManagedService%' AND
        -- exclude pipelines
        build_name NOT IN (
            'Bazel CI',
            'Bazel Extended CI',
            'Merged Bazel Extended CI',
            'Cockroach_Nightlies_NightlySuiteBazel',
            'Cockroach_PostMerge_MergedArm64Tests'
        ) -- exclude scratch pipelines and builds
        AND build_type NOT LIKE 'Cockroach_ScratchProject%'
),
ENGFLOW_TC AS (
    SELECT
        server,
        teamcity_build_id as build_id,
	label,
	test_name,
        lower(status) as status,
	invocation_id
    FROM
        DATAMART_PROD.ENGINEERING.ENGFLOW_DATA_DAILY_SNAPSHOT
    WHERE
        teamcity_build_id IS NOT NULL AND
        started_at > dateadd(DAY, -?, current_date()) AND
        branch = 'master'
)
SELECT
    b.build_type,
    b.build_name,
    a.label,
    a.test_name,
    a.server,
    sum(CASE WHEN a.status = 'success' THEN 1 ELSE 0 END) AS pass_cnt,
    array_agg(CASE WHEN a.status = 'failure' THEN invocation_id END) AS failed_builds
FROM
    ENGFLOW_TC a
    INNER JOIN TC_BUILDS b ON (a.build_id = b.build_id)
WHERE
    test_name IS NOT NULL
    AND a.status NOT IN ('unknown', 'skipped', 'error')
GROUP BY 1, 2, 3, 4, 5
HAVING array_size(failed_builds) > 0 AND pass_cnt + array_size(failed_builds) > 5 AND TO_NUMBER((array_size(failed_builds) / (array_size(failed_builds) + pass_cnt)) * 100, 10, 1) > 0.01
