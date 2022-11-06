delete from `{{ params.project_id }}.bronze.sales`
where date(_logical_dt) = "{{ ds }}"
;

insert `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    _id,
    _logical_dt,
    _job_start_dt
)
select
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    GENERATE_UUID() as _id,
    CAST('{{ dag_run.logical_date }}' as timestamp) as _logical_dt,
    CAST('{{ dag_run.start_date }}' as timestamp) as _job_start_dt
from sales_csv
;