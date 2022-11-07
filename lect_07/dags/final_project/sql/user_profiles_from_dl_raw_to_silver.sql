delete from `{{ params.project_id }}.silver.user_profiles`
where true
;

insert `{{ params.project_id }}.silver.user_profiles` (
    email,
    full_name,
    first_name,
    last_name,
    state,
    birth_date,
    phone_number,
    _id,
    _logical_dt,
    _job_start_dt
)
select
    email,
    full_name,
    REGEXP_EXTRACT(full_name, r'(.*?)(?: |$)') first_name,
    REGEXP_EXTRACT(full_name, r' (.*)') last_name,
    state,
    birth_date,
    phone_number,
    GENERATE_UUID() as _id,
    CAST('{{ dag_run.logical_date }}' as timestamp) as _logical_dt,
    CAST('{{ dag_run.start_date }}' as timestamp) as _job_start_dt
from user_profiles_jsonl
;