delete from {0}
where publication_date <= datetime('now', '-3 months');