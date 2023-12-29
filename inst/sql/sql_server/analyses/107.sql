-- 107	Length of observation (days) of first observation period by age decile
--HINT DISTRIBUTE_ON_KEY(age_decile)
CREATE GLOBAL TEMPORARY TABLE temp.rawData(age_decile NUMERIC,count_value NUMERIC);
INSERT INTO temp.rawData(age_decile,count_value)
select
floor((YEAR(op.OBSERVATION_PERIOD_START_DATE) - p.YEAR_OF_BIRTH)/10) as age_decile,
       (CAST(op.observation_period_end_date AS DATE) - CAST(op.observation_period_start_date AS DATE)) as count_value
FROM
    (
select
person_id,
               op.observation_period_start_date,
               op.observation_period_end_date,
               ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
from @cdmDatabaseSchema.observation_period op
    ) op
        JOIN @cdmDatabaseSchema.person p on op.person_id = p.person_id
where op.rn = 1;

CREATE GLOBAL TEMPORARY TABLE temp.overallStats(age_decile NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(age_decile,avg_value,stdev_value,min_value,max_value,total)
select
age_decile,
       CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
       min(count_value) as min_value,
       max(count_value) as max_value,
       COUNT(*) as total
from temp.rawData
group by age_decile;

CREATE GLOBAL TEMPORARY TABLE temp.statsView(age_decile NUMERIC,count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView(age_decile,count_value,total,rn)
select
age_decile,
       count_value,
       COUNT(*) as total,
       row_number() over (order by count_value) as rn
FROM temp.rawData
group by age_decile, count_value;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(age_decile NUMERIC,count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(age_decile,count_value,total,accumulated)
select
s.age_decile, s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView s
         join temp.statsView p on s.age_decile = p.age_decile and p.rn <= s.rn
group by s.age_decile, s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_107(analysis_id INT,age_decile NUMERIC,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_107(analysis_id,age_decile,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
       107 as analysis_id,
       CAST(o.age_decile AS VARCHAR(255)) as age_decile,
       o.total as count_value,
       o.min_value,
       o.max_value,
       o.avg_value,
       o.stdev_value,
       MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
       MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
       MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
       MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
       MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
FROM
       temp.priorStats p
           join temp.overallStats o on p.age_decile = o.age_decile
   GROUP BY o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(stratum_1)
select analysis_id, age_decile as stratum_1, 
cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_107
FROM temp.tempResults_107
;

truncate table temp.tempResults_107;
drop table temp.tempResults_107;
DROP TABLE temp.rawData;
DROP TABLE temp.overallStats;
DROP TABLE temp.statsView;
DROP TABLE temp.priorStats;

