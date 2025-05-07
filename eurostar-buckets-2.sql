WITH 
spotify_owners as (
    SELECT
        project_owners.project_id,
        project_owners.owner as project_owner,
        spotify_groups.id as group_id,
        spotify_groups.type as owner_type,
        spotify_groups.contact_info.slack_channel,
        IF(spotify_groups.contact_info.private_email is not null, contact_info.private_email, contact_info.public_email) as email,
        spotify_groups.flattened_org_hierarchy.squad,
        spotify_groups.flattened_org_hierarchy.product_area,
        spotify_groups.flattened_org_hierarchy.studio,
        spotify_groups.flattened_org_hierarchy.mission
    FROM `caohs-prod.caohs_export.gcp_project_ownership_*` project_owners
        JOIN `spotify-people.groups.groups_*` spotify_groups
        ON lower(project_owners.owner) = lower(spotify_groups.id)
    WHERE
        PARSE_DATE("%Y%m%d", project_owners._table_suffix)
        = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
        AND
        PARSE_DATE("%Y%m%d", spotify_groups._table_suffix)
        = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
),

eurostar_scope AS (
    SELECT *
    FROM `gcs-insights.relocation.eurostar_scope_*`
    WHERE _TABLE_SUFFIX = (SELECT MAX(REPLACE(table_id, "eurostar_scope_", "")) FROM `gcs-insights.relocation.__TABLES__` WHERE STARTS_WITH(table_id, "eurostar_scope_"))
),

migrations AS (
SELECT distinct bucket as name, bucketState as state FROM `spotify-batuta.batuta_exporter.migrations_*`  WHERE
  PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)

SELECT eurostar_scope.*, spotify_owners.*, COALESCE(state, "NOT_MIGRATED") AS migration_status
 FROM eurostar_scope
LEFT JOIN spotify_owners ON spotify_owners.project_id = project
LEFT JOIN migrations USING (name)
