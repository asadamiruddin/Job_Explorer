create table if not exists {0} (
    job_id integer,
    job_title varchar,
    publication_date datetime,
    job_description text,
    location text,
    job_category text,
    job_level text,
    company_id integer,
    company_name varchar,
    job_type varchar,
    job_reference_page varchar,
    insert_date datetime,
    primary key (job_title, company_id)  
);