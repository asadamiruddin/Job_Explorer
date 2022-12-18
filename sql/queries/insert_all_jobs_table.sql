insert or ignore into {0} (
    job_id,
    job_title,
    publication_date,
    job_description,
    location,
    job_category,
    job_level,
    company_id,
    company_name,
    job_type,
    job_reference_page,
    insert_date
)
values (?,?,?,?,?,?,?,?,?,?,?,?);