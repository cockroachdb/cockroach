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
TC_EXECUTED_TESTS AS (
    SELECT
	b.build_name AS build_name,
        b.build_id AS build_id,
        build_type,
        test_name,
        lower(status) AS test_status
    FROM
        datamart_prod.teamcity.tests a
        INNER JOIN TC_BUILDS b ON (a.BUILD_ID = b.BUILD_ID)
    WHERE
        test_name IS NOT NULL
        AND test_status NOT IN ('unknown', 'skipped', 'error')
)
SELECT
    build_type,
    build_name,
    test_name,
    sum(CASE WHEN test_status = 'success' THEN 1 ELSE 0 END) AS pass_cnt,
    array_agg(CASE WHEN test_status = 'failure' THEN build_id END) AS failed_builds
FROM TC_EXECUTED_TESTS
GROUP BY 1, 2, 3
HAVING array_size(failed_builds) > 0 AND pass_cnt + array_size(failed_builds) > 5 AND TO_NUMBER((array_size(failed_builds) / (array_size(failed_builds) + pass_cnt)) * 100, 10, 1) > 0.01
