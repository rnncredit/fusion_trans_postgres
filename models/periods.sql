{{ config(materialized='table',
		  post_hook='delete from {{this}} where 1=1 and rownum<5') }}

with __dbt__cte__gl_periods_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "airbyte".airbyte._airbyte_raw_gl_periods
select
    jsonb_extract_path_text(_airbyte_data, 'END_DATE') as end_date,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE1') as attribute1,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE2') as attribute2,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE3') as attribute3,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE4') as attribute4,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE5') as attribute5,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE6') as attribute6,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE7') as attribute7,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE8') as attribute8,
    jsonb_extract_path_text(_airbyte_data, 'CREATED_BY') as created_by,
    jsonb_extract_path_text(_airbyte_data, 'PERIOD_NUM') as period_num,
    jsonb_extract_path_text(_airbyte_data, 'START_DATE') as start_date,
    jsonb_extract_path_text(_airbyte_data, 'DESCRIPTION') as description,
    jsonb_extract_path_text(_airbyte_data, 'FISCAL_YEAR') as fiscal_year,
    jsonb_extract_path_text(_airbyte_data, 'PERIOD_NAME') as period_name,
    jsonb_extract_path_text(_airbyte_data, 'PERIOD_TYPE') as period_type,
    jsonb_extract_path_text(_airbyte_data, 'PERIOD_YEAR') as period_year,
    jsonb_extract_path_text(_airbyte_data, 'QUARTER_NUM') as quarter_num,
    jsonb_extract_path_text(_airbyte_data, 'CREATION_DATE') as creation_date,
    jsonb_extract_path_text(_airbyte_data, 'ENTERPRISE_ID') as enterprise_id,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_DATE1') as attribute_date1,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_DATE2') as attribute_date2,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_DATE3') as attribute_date3,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_DATE4') as attribute_date4,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_DATE5') as attribute_date5,
    jsonb_extract_path_text(_airbyte_data, 'LAST_UPDATED_BY') as last_updated_by,
    jsonb_extract_path_text(_airbyte_data, 'PERIOD_SET_NAME') as period_set_name,
    jsonb_extract_path_text(_airbyte_data, 'YEAR_START_DATE') as year_start_date,
    jsonb_extract_path_text(_airbyte_data, 'LAST_UPDATE_DATE') as last_update_date,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_NUMBER1') as attribute_number1,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_NUMBER2') as attribute_number2,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_NUMBER3') as attribute_number3,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_NUMBER4') as attribute_number4,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_NUMBER5') as attribute_number5,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE1') as global_attribute1,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE2') as global_attribute2,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE3') as global_attribute3,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE4') as global_attribute4,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE5') as global_attribute5,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE6') as global_attribute6,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE7') as global_attribute7,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE8') as global_attribute8,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE9') as global_attribute9,
    jsonb_extract_path_text(_airbyte_data, 'LAST_UPDATE_LOGIN') as last_update_login,
    jsonb_extract_path_text(_airbyte_data, 'ATTRIBUTE_CATEGORY') as attribute_category,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE10') as global_attribute10,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE11') as global_attribute11,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE12') as global_attribute12,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE13') as global_attribute13,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE14') as global_attribute14,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE15') as global_attribute15,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE16') as global_attribute16,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE17') as global_attribute17,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE18') as global_attribute18,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE19') as global_attribute19,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE20') as global_attribute20,
    jsonb_extract_path_text(_airbyte_data, 'QUARTER_START_DATE') as quarter_start_date,
    jsonb_extract_path_text(_airbyte_data, 'CONFIRMATION_STATUS') as confirmation_status,
    jsonb_extract_path_text(_airbyte_data, 'ENTERED_PERIOD_NAME') as entered_period_name,
    jsonb_extract_path_text(_airbyte_data, 'OBJECT_VERSION_NUMBER') as object_version_number,
    jsonb_extract_path_text(_airbyte_data, 'ADJUSTMENT_PERIOD_FLAG') as adjustment_period_flag,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_DATE1') as global_attribute_date1,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_DATE2') as global_attribute_date2,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_DATE3') as global_attribute_date3,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_DATE4') as global_attribute_date4,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_DATE5') as global_attribute_date5,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_NUMBER1') as global_attribute_number1,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_NUMBER2') as global_attribute_number2,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_NUMBER3') as global_attribute_number3,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_NUMBER4') as global_attribute_number4,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_NUMBER5') as global_attribute_number5,
    jsonb_extract_path_text(_airbyte_data, 'GLOBAL_ATTRIBUTE_CATEGORY') as global_attribute_category,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "airbyte".airbyte._airbyte_raw_gl_periods as table_alias
-- gl_periods
where 1 = 1

),
__dbt__cte__gl_periods_ab2 as (
select
    cast(nullif(end_date, '') as 
    date
) as end_date,
    cast(attribute1 as 
    varchar
) as attribute1,
    cast(attribute2 as 
    varchar
) as attribute2,
    cast(attribute3 as 
    varchar
) as attribute3,
    cast(attribute4 as 
    varchar
) as attribute4,
    cast(attribute5 as 
    varchar
) as attribute5,
    cast(attribute6 as 
    varchar
) as attribute6,
    cast(attribute7 as 
    varchar
) as attribute7,
    cast(attribute8 as 
    varchar
) as attribute8,
    cast(created_by as 
    varchar
) as created_by,
    cast(period_num as 
    float
) as period_num,
    cast(nullif(start_date, '') as 
    date
) as start_date,
    cast(description as 
    varchar
) as description,
    cast(fiscal_year as 
    varchar
) as fiscal_year,
    cast(period_name as 
    varchar
) as period_name,
    cast(period_type as 
    varchar
) as period_type,
    cast(period_year as 
    float
) as period_year,
    cast(quarter_num as 
    float
) as quarter_num,
    cast(nullif(creation_date, '') as 
    timestamp with time zone
) as creation_date,
    cast(enterprise_id as 
    float
) as enterprise_id,
    cast(nullif(attribute_date1, '') as 
    date
) as attribute_date1,
    cast(nullif(attribute_date2, '') as 
    date
) as attribute_date2,
    cast(nullif(attribute_date3, '') as 
    date
) as attribute_date3,
    cast(nullif(attribute_date4, '') as 
    date
) as attribute_date4,
    cast(nullif(attribute_date5, '') as 
    date
) as attribute_date5,
    cast(last_updated_by as 
    varchar
) as last_updated_by,
    cast(period_set_name as 
    varchar
) as period_set_name,
    cast(nullif(year_start_date, '') as 
    date
) as year_start_date,
    cast(nullif(last_update_date, '') as 
    timestamp with time zone
) as last_update_date,
    cast(global_attribute1 as 
    varchar
) as global_attribute1,
    cast(global_attribute2 as 
    varchar
) as global_attribute2,
    cast(global_attribute3 as 
    varchar
) as global_attribute3,
    cast(global_attribute4 as 
    varchar
) as global_attribute4,
    cast(global_attribute5 as 
    varchar
) as global_attribute5,
    cast(global_attribute6 as 
    varchar
) as global_attribute6,
    cast(global_attribute7 as 
    varchar
) as global_attribute7,
    cast(global_attribute8 as 
    varchar
) as global_attribute8,
    cast(global_attribute9 as 
    varchar
) as global_attribute9,
    cast(last_update_login as 
    varchar
) as last_update_login,
    cast(attribute_category as 
    varchar
) as attribute_category,
    cast(global_attribute10 as 
    varchar
) as global_attribute10,
    cast(global_attribute11 as 
    varchar
) as global_attribute11,
    cast(global_attribute12 as 
    varchar
) as global_attribute12,
    cast(global_attribute13 as 
    varchar
) as global_attribute13,
    cast(global_attribute14 as 
    varchar
) as global_attribute14,
    cast(global_attribute15 as 
    varchar
) as global_attribute15,
    cast(global_attribute16 as 
    varchar
) as global_attribute16,
    cast(global_attribute17 as 
    varchar
) as global_attribute17,
    cast(global_attribute18 as 
    varchar
) as global_attribute18,
    cast(global_attribute19 as 
    varchar
) as global_attribute19,
    cast(global_attribute20 as 
    varchar
) as global_attribute20,
    cast(nullif(quarter_start_date, '') as 
    date
) as quarter_start_date,
    cast(confirmation_status as 
    varchar
) as confirmation_status,
    cast(entered_period_name as 
    varchar
) as entered_period_name,
cast(adjustment_period_flag as 
    varchar
) as adjustment_period_flag,
    cast(nullif(global_attribute_date1, '') as 
    date
) as global_attribute_date1,
    cast(nullif(global_attribute_date2, '') as 
    date
) as global_attribute_date2,
    cast(nullif(global_attribute_date3, '') as 
    date
) as global_attribute_date3,
    cast(nullif(global_attribute_date4, '') as 
    date
) as global_attribute_date4,
    cast(nullif(global_attribute_date5, '') as 
    date
) as global_attribute_date5,
cast(global_attribute_category as 
    varchar
) as global_attribute_category,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__gl_periods_ab1
-- gl_periods
where 1 = 1 ), __dbt__cte__gl_periods_ab3 as (
select
    md5(cast(coalesce(cast(end_date as 
    varchar
), '') || '-' || coalesce(cast(attribute1 as 
    varchar
), '') || '-' || coalesce(cast(attribute2 as 
    varchar
), '') || '-' || coalesce(cast(attribute3 as 
    varchar
), '') || '-' || coalesce(cast(attribute4 as 
    varchar
), '') || '-' || coalesce(cast(attribute5 as 
    varchar
), '') || '-' || coalesce(cast(attribute6 as 
    varchar
), '') || '-' || coalesce(cast(attribute7 as 
    varchar
), '') || '-' || coalesce(cast(attribute8 as 
    varchar
), '') || '-' || coalesce(cast(created_by as 
    varchar
), '') || '-' || coalesce(cast(period_num as 
    varchar
), '') || '-' || coalesce(cast(start_date as 
    varchar
), '') || '-' || coalesce(cast(description as 
    varchar
), '') || '-' || coalesce(cast(fiscal_year as 
    varchar
), '') || '-' || coalesce(cast(period_name as 
    varchar
), '') || '-' || coalesce(cast(period_type as 
    varchar
), '') || '-' || coalesce(cast(period_year as 
    varchar
), '') || '-' || coalesce(cast(quarter_num as 
    varchar
), '') || '-' || coalesce(cast(creation_date as 
    varchar
), '') || '-' || coalesce(cast(enterprise_id as 
    varchar
), '') || '-' || coalesce(cast(attribute_date1 as 
    varchar
), '') || '-' || coalesce(cast(attribute_date2 as 
    varchar
), '') || '-' || coalesce(cast(attribute_date3 as 
    varchar
), '') || '-' || coalesce(cast(attribute_date4 as 
    varchar
), '') || '-' || coalesce(cast(attribute_date5 as 
    varchar
), '') || '-' || coalesce(cast(last_updated_by as 
    varchar
), '') || '-' || coalesce(cast(period_set_name as 
    varchar
), '') || '-' || coalesce(cast(year_start_date as 
    varchar
), '') || '-' || coalesce(cast(last_update_date as 
    varchar
), '') || '-' || '-' || coalesce(cast(global_attribute1 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute2 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute3 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute4 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute5 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute6 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute7 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute8 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute9 as 
    varchar
), '') || '-' || coalesce(cast(last_update_login as 
    varchar
), '') || '-' || coalesce(cast(attribute_category as 
    varchar
), '') || '-' || coalesce(cast(global_attribute10 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute11 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute12 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute13 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute14 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute15 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute16 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute17 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute18 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute19 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute20 as 
    varchar
), '') || '-' || coalesce(cast(quarter_start_date as 
    varchar
), '') || '-' || coalesce(cast(confirmation_status as 
    varchar
), '') || '-' || coalesce(cast(entered_period_name as 
    varchar
), '') || '-' || coalesce(cast(adjustment_period_flag as 
    varchar
), '') || '-' || coalesce(cast(global_attribute_date1 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute_date2 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute_date3 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute_date4 as 
    varchar
), '') || '-' || coalesce(cast(global_attribute_date5 as 
    varchar
), '') ||'-' || coalesce(cast(global_attribute_category as 
    varchar
), '') as 
    varchar
)) as _airbyte_gl_periods_hashid,
    tmp.*
from __dbt__cte__gl_periods_ab2 tmp
-- gl_periods
where 1 = 1)
select
    end_date,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    attribute6,
    attribute7,
    attribute8,
    created_by,
    period_num,
    start_date,
    description,
    fiscal_year,
    period_name,
    period_type,
    period_year,
    quarter_num,
    creation_date,
    enterprise_id,
    attribute_date1,
    attribute_date2,
    attribute_date3,
    attribute_date4,
    attribute_date5,
    last_updated_by,
    period_set_name,
    year_start_date,
    last_update_date,
    global_attribute1,
    global_attribute2,
    global_attribute3,
    global_attribute4,
    global_attribute5,
    global_attribute6,
    global_attribute7,
    global_attribute8,
    global_attribute9,
    last_update_login,
    attribute_category,
    global_attribute10,
    global_attribute11,
    global_attribute12,
    global_attribute13,
    global_attribute14,
    global_attribute15,
    global_attribute16,
    global_attribute17,
    global_attribute18,
    global_attribute19,
    global_attribute20,
    quarter_start_date,
    confirmation_status,
    entered_period_name,
    adjustment_period_flag,
    global_attribute_date1,
    global_attribute_date2,
    global_attribute_date3,
    global_attribute_date4,
    global_attribute_date5,
    global_attribute_category,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_gl_periods_hashid
from __dbt__cte__gl_periods_ab3
-- gl_periods from "airbyte".airbyte._airbyte_raw_gl_periods
where 1 = 1