{{ config(materialized='incremental', unique_key='dr_no') }}

with source as (
SELECT "LOCATION", "Weapon Used Cd", "Status Desc", "DATE OCC", "Weapon Desc", "Premis Desc", "_ab_source_file_url", area,
 "TIME OCC", "Premis Cd", "Rpt Dist No", "Part 1-2", "AREA NAME", "Vict Sex", status, "_ab_source_file_last_modified"
 , lon, "Crm Cd 1", "Crm Cd 2", "Crm Cd", "Crm Cd 3", "Crm Cd 4", "Crm Cd Desc", "Vict Descent"
 , mocodes, "Date Rptd", dr_no, "Vict Age", "Cross Street", "_ab_additional_properties", lat,
  "_airbyte_ab_id", "_airbyte_emitted_at", "_airbyte_normalized_at", "_airbyte_crime_data_hashid"
FROM analytic.crime_data
)

select
*
from source

{% if is_incremental() %}
    where dr_no > (select max(dr_no) from {{ this }})
{% endif %}
