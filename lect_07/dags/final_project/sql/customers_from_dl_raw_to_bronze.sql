delete from `{{ params.project_id }}.bronze.customers`
where date(_logical_dt) = "{{ dag_run.logical_date }}"
;

insert `{{ params.project_id }}.bronze.customers` (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,
    _id,
    _logical_dt,
    _job_start_dt
)
select
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,
    GENERATE_UUID() as _id,
    CAST('{{ dag_run.logical_date }}' as timestamp) as _logical_dt,
    CAST('{{ dag_run.start_date }}' as timestamp) as _job_start_dt
from customers_csv
;