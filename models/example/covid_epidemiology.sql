create table "dev"."public"."covid_epidemiology__dbt_tmp"
as
(select
        cast(json_extract_path_text("_airbyte_data",'key') as varchar) is not NULL as "key",
        cast(json_extract_path_text("_airbyte_data",'date') as varchar) is not NULL as "date",
        cast(json_extract_path_text("_airbyte_data",'new_tested') as float) is not NULL as new_tested,
        cast(json_extract_path_text("_airbyte_data",'new_deceased') as float) is not NULL as new_deceased,
        cast(json_extract_path_text("_airbyte_data",'total_tested') as float) is not NULL as total_tested,
        cast(json_extract_path_text("_airbyte_data",'new_confirmed') as float)is not NULL  as new_confirmed,
        cast(json_extract_path_text("_airbyte_data",'new_recovered') as float) is not NULL as new_recovered,
        cast(json_extract_path_text("_airbyte_data",'total_deceased') as float) is not NULL as total_deceased,
        cast(json_extract_path_text("_airbyte_data",'total_confirmed') as float) is not NULL as total_confirmed,
        cast(json_extract_path_text("_airbyte_data",'total_recovered') as float) is not NULL as total_recovered
from "dev".public._airbyte_raw_covid_epidemiology as table_alias
 );
