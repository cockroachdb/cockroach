WITH ENGFLOW_GH_EXECUTED_TESTS AS (
    SELECT
        server,
        github_job AS build_type,
	label,
	test_name,
        lower(status) AS test_status,
	invocation_id
    FROM
        DATAMART_PROD.ENGINEERING.ENGFLOW_DATA_DAILY_SNAPSHOT
    WHERE
        github_job IS NOT NULL
        AND test_name IS NOT NULL
        AND started_at > dateadd(DAY, -?, current_date())
        AND branch = 'master'
)
SELECT
    server,
    build_type,
    label,
    test_name,
    sum(CASE WHEN test_status = 'success' THEN 1 ELSE 0 END) AS pass_cnt,
    array_agg(CASE WHEN test_status = 'failure' THEN invocation_id END) AS failed_builds
FROM
    ENGFLOW_GH_EXECUTED_TESTS
GROUP BY 1, 2, 3, 4
HAVING array_size(failed_builds) > 0 AND pass_cnt + array_size(failed_builds) > 5 AND TO_NUMBER((array_size(failed_builds) / (array_size(failed_builds) + pass_cnt)) * 100, 10, 1) > 0.01
