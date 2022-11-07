merge `{{ params.project_id }}.silver.customers` as s
using (
  /*
    Видалимо дублікати. рамках одного дня.
  */
  select
      Id as client_id,
      FirstName as first_name,
      LastName as last_name,
      Email as email,
      cast(RegistrationDate as date) as registration_date,
      State as state,
      max(_id) as _id,
      _logical_dt,
      _job_start_dt
  from `{{ params.project_id }}.bronze.customers`
  where date(_logical_dt) = date('{{ ds }}')
  group by 
    Id, 
    FirstName, 
    LastName, 
    Email, 
    cast(RegistrationDate as date),
    State,
    _logical_dt,
    _job_start_dt
) as b
on s.client_id = b.client_id
when matched and s._logical_dt < b._logical_dt then
  /*
    Якщо клієнт знадений в silver і bronze данних + у bronze дата процесінгу більше, 
    то обновимо всю строчку в silver, на випадок, якщо клієнт обновив свої данні.
    Якщо дата процесінгу більше у silver, то нічого не робимо, вважаємо, що в системі уже обновлені данні.
  */
  update set 
    s.first_name = b.first_name,
    s.last_name = b.last_name,
    s.email = b.email,
    s.registration_date = b.registration_date,
    s.state = b.state,
    s.deleted = FALSE,
    s._id = b._id,
    s._logical_dt = b._logical_dt,
    s._job_start_dt = b._job_start_dt
when not matched then
  /*
    Якщо данних не має в оригінальній таблиці потрібно їх додати.
  */
  insert (
      client_id,
      first_name,
      last_name,
      email,
      registration_date,
      state,
      deleted,
      _id,
      _logical_dt,
      _job_start_dt
  ) values(
      client_id,
      first_name,
      last_name,
      email,
      registration_date,
      state,
      FALSE,
      _id,
      _logical_dt,
      _job_start_dt
  )
when not matched by source and date(s._logical_dt) <= date('{{ ds }}') then
  /*
    Якщо данних не має в новій таблиці, то відмітимо користувача як видаленого.
  */
  update set deleted = TRUE