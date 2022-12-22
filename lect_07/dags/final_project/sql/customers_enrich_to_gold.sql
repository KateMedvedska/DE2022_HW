merge `{{ params.project_id }}.gold.user_profiles_enriched` as g
using (
  /*
    Виберемо данні для збагаченої таблиці
  */
  select 
    c.client_id as client_id,
    up.first_name as first_name,
    up.last_name as last_name,
    c.email as email,
    c.registration_date as registration_date,
    up.state as state,
    up.birth_date as birth_date,
    up.phone_number as phone_number,
    c.deleted as deleted,
    c._id as _id,
    c._logical_dt as _logical_dt,
    c._job_start_dt as _job_start_dt
  from `{{ params.project_id }}.silver.customers` as c
  left join `{{ params.project_id }}.silver.user_profiles` as up on c.email = up.email
) as s
on s.client_id = g.client_id
when matched and g._logical_dt < s._logical_dt then
  /*
    Якщо клієнт знадений в silver і gold данних + у silver дата процесінгу більше, 
    то обновимо всю строчку в gold, на випадок, якщо клієнт обновив свої данні.
    Якщо дата процесінгу більше у gold, то нічого не робимо, вважаємо, що в системі уже обновлені данні.
  */
  update set 
    g.first_name = s.first_name,
    g.last_name = s.last_name,
    g.email = s.email,
    g.registration_date = s.registration_date,
    g.state = s.state,
    g.birth_date = s.birth_date,
    g.phone_number = s.phone_number,
    g.deleted = s.deleted,
    g._id = s._id,
    g._logical_dt = s._logical_dt,
    g._job_start_dt = s._job_start_dt
when not matched then
  /*
    Якщо данних не має в gold таблиці потрібно їх додати.
  */
  insert (
      client_id,
      first_name,
      last_name,
      email,
      registration_date,
      state,
      birth_date,
      phone_number,
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
      birth_date,
      phone_number,
      deleted,
      _id,
      _logical_dt,
      _job_start_dt
  )