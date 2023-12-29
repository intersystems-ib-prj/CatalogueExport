-- 106	Length of observation (days) of first observation period by gender
--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
CREATE GLOBAL TEMPORARY TABLE temp.rawData_106(gender_concept_id INT,count_value NUMERIC);
INSERT INTO temp.rawData_106(gender_concept_id,count_value)
SELECT
       p.gender_concept_id, op.count_value
FROM
       (
select
person_id, (CAST(op.observation_period_end_date AS DATE) - CAST(op.observation_period_start_date AS DATE)) as count_value,
                  ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
from @cdmDatabaseSchema.observation_period op
       ) op
           JOIN @cdmDatabaseSchema.person p on op.person_id = p.person_id
   where op.rn = 1
;

--HINT DISTRIBUTE_ON_KEY(gender_concept_id)
CREATE GLOBAL TEMPORARY TABLE temp.overallStats(gender_concept_id INT,avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(gender_concept_id,avg_value,stdev_value,min_value,max_value,total)
select
gender_concept_id,
       CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
       min(count_value) as min_value,
       max(count_value) as max_value,
       COUNT(*) as total
FROM temp.rawData_106
group by gender_concept_id;

CREATE GLOBAL TEMPORARY TABLE temp.statsView(gender_concept_id INT,count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView(gender_concept_id,count_value,total,rn)
select
gender_concept_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
FROM temp.rawData_106
group by gender_concept_id, count_value;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(gender_concept_id INT,count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(gender_concept_id,count_value,total,accumulated)
select
s.gender_concept_id, s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView s
         join temp.statsView p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
group by s.gender_concept_id, s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_106(analysis_id INT,gender_concept_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_106(analysis_id,gender_concept_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
       106 as analysis_id,
       CAST(o.gender_concept_id AS VARCHAR(255)) as gender_concept_id,
       o.total as count_value,
       o.min_value,
       o.max_value,
       o.avg_value,
       o.stdev_value,
       MIN(case when p.accumulated >= .50 * o.total then count_value end) as median_value,
       MIN(case when p.accumulated >= .10 * o.total then count_value end) as p10_value,
       MIN(case when p.accumulated >= .25 * o.total then count_value end) as p25_value,
       MIN(case when p.accumulated >= .75 * o.total then count_value end) as p75_value,
       MIN(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
FROM
       temp.priorStats p
           join temp.overallStats o on p.gender_concept_id = o.gender_concept_id
   GROUP BY o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;


select analysis_id, gender_concept_id as stratum_1, 
cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_106
FROM  temp.tempResults_106
;

truncate table temp.rawData_106;
drop table temp.rawData_106;
truncate table temp.tempResults_106;
drop table temp.tempResults_106;

DROP TABLE temp.overallStats;
DROP TABLE temp.statsView;
DROP TABLE temp.priorStats;
