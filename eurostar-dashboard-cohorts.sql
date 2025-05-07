-- Buckets query for P0 dashboard

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

-- DRR Buckets are out of scope.
drr_buckets AS (
    SELECT distinct bucket_id AS name FROM `drr-adoption.qer_bucket_migration_v2.qer_bucket_migration_v2` 
    WHERE 
        TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = 
        TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY), DAY)
    GROUP BY 1
),

-- 202 Buckets are out of scope.
two_oh_two_buckets AS (
    SELECT distinct name
    FROM `storage-insights.gcs.gcs_consolidated_buckets_*` b
    WHERE PARSE_DATE("%Y%m%d", b._table_suffix)
    = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND project IN (
        'instrumentation-infra',
        'events-anonym-gcs',
        'spotify-gabito-e2e',
        'gabo-xpn',
        'gabo-backups',
        'events-backup',
        'instrumentation-rnd',
        'gabo-raw-data',
        'gabito-audit'
    )
),

--- Total Traffic data
traffic_data AS (
  SELECT
    bucket_id AS name,
    SUM(metered_gib_in
        + metered_gib_out
        + metadata_rewrite_gib_in
        + metadata_rewrite_gib_out
        + physical_rewrite_gib_in
        + physical_rewrite_gib_out
        + unknown_rewrite_gib_in
        + unknown_rewrite_gib_out
        + within_bucket_rewrite_gib) AS total_traffic_gib
  FROM
    `billing-data-2.gcs_bucket_data_movement_hourly.gcs_bucket_data_movement_hourly_202503*`
  GROUP BY
    bucket_id
),

migrations AS (
SELECT distinct bucket as name, bucketState as state FROM `spotify-batuta.batuta_exporter.migrations_*`  WHERE
  PARSE_DATE("%Y%m%d", _TABLE_SUFFIX) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
)


SELECT
    name AS bucket_name,
    location,
    location_type,
    drr_buckets.name IS NOT NULL as is_drr,
    two_oh_two_buckets.name IS NOT NULL as is_202,
    stored_bytes_total / 1.1e+12 AS stored_tibs,
    total_traffic_gib AS network_gib_per_mo,
    spotify_owners.*,
	COALESCE(state, "NOT_MIGRATED") AS state
FROM
    `storage-insights.gcs.gcs_consolidated_buckets_*` b
LEFT JOIN drr_buckets USING(name)
LEFT JOIN two_oh_two_buckets USING(name)
LEFT JOIN spotify_owners ON spotify_owners.project_id = b.project
LEFT JOIN traffic_data USING(name)
LEFT JOIN migrations USING(name)
WHERE
    PARSE_DATE("%Y%m%d", b._table_suffix)
    = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)

  --- Exclude Buckets already in EW4
  AND UPPER(location) != 'EUROPE-WEST4'

  --- Consider only EW1_SR and EU_MR Buckets
  AND (
    UPPER(location) = 'EUROPE-WEST1'
    OR
    (UPPER(location) = 'EU' AND UPPER(location_type) = 'MULTI-REGION')
    
    --- Uncomment to include 84 EU_DR buckets (+5.2 PiB Stored, 50PiB /mo traffic)
    --- OR (UPPER(location) LIKE 'EU%' AND UPPER(location_type) = 'DUAL-REGION')

    --- Uncomment to include 194 EW2,3,5,.. SR buckets  (+2.2 PiB Stored, 750k GiB /mo traffic )
    --- OR UPPER(location) LIKE 'EUROPE-WEST%'
  )

  --- Exclude empty buckets with < 1 GiB of traffic per month (Excludes ~9,000 buckets, 0 Stored, ~100 GiB /mo traffic)
  AND NOT (stored_bytes_total = 0 AND total_traffic_gib < 1)
      
  --- Exlucde SR buckets with low traffic (3500 buckets, 39.5 PiB)
  --- AND NOT (UPPER(location_type) = 'REGION' AND total_traffic_gib < 1)

  --- Exclude Scio cookie dedicated temp buckets (!!!COMMENT OUT THIS LINE FOR COHORT SELECTION - MR TEMP BUCKET SAVINGS SHOULD BE INCLUDED)
  AND name NOT like '%-temp'

  --- Exclude Google buckets eg. GCR
  AND name NOT like '%appspot%'
