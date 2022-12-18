Create table {0} as 
Select all_titles, count(*) as num_occurences, group_concat(job_description, ',') as descriptions from (
select * from (
select *, row_number() over (partition by all_titles, company_id order by job_id desc) as row_num2 from (
Select * from (
Select *, row_number() over (partition by job_id order by length(t2.all_titles) desc) row_num from (
(select job_id, job_title, job_description, company_id from {1}) t1 left join 
(select * from {2} where length(all_titles)>5) t2
on instr(upper(t1.job_title), upper(t2.all_titles)) > 0))
where row_num = 1))
where row_num2 <= 3)
where all_titles is not null
group by all_titles;