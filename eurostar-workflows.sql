-- workflows
WITH migrations AS (
SELECT distinct SPLIT(workflow, "#")[1] as workflow_name, bucketState as state FROM `spotify-batuta.batuta_exporter.migrations_*`, UNNEST(workflows) as workflow WHERE
  PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT owners.*, _TABLE_SUFFIX as LAST_PARTITION, COALESCE(m.state, "NOT_MIGRATED") as workflowState
FROM `drr-shift.eurostars_workflows_onboard.eurostars_workflows_owners_*` owners
LEFT JOIN migrations m USING (workflow_name)
WHERE _TABLE_SUFFIX = (SELECT MAX(REPLACE(table_id, "eurostars_workflows_owners_", ""))
FROM `drr-shift.eurostars_workflows_onboard.__TABLES__`
WHERE STARTS_WITH(table_id, "eurostars_workflows_owners_"))
