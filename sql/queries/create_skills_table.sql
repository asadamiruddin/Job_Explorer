create table if not exists {0} (
    all_titles varchar,
    num_occurences integer,
    Top1 varchar,
    Top2 varchar,
    Top3 varchar,
    Top4 varchar,
    Top5 varchar,
    Top6 varchar,
    Top7 varchar,
    Top8 varchar,
    Top9 varchar,
    Top10 varchar,
    confidence_level real,
    primary key (all_titles)  
);