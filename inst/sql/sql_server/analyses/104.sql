-- 104	Distribution of age at first observation period by gender
--HINT DISTRIBUTE_ON_KEY(stratum_1)

CREATE GLOBAL TEMPORARY TABLE temp.rawData(gender_concept_id INT,age_value NUMERIC);
INSERT INTO temp.rawData(gender_concept_id,age_value)
select
p.gender_concept_id, MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
from @cdmDatabaseSchema.person p
         JOIN @cdmDatabaseSchema.observation_period op on p.person_id = op.person_id
group by p.person_id,p.gender_concept_id, p.year_of_birth;


CREATE GLOBAL TEMPORARY TABLE temp.overallStats(gender_concept_id INT,avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(gender_concept_id,avg_value,stdev_value,min_value,max_value,total)
select
gender_concept_id,
       CAST(avg(1.0 * age_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(age_value) AS NUMERIC) as stdev_value,
       min(age_value) as min_value,
       max(age_value) as max_value,
       COUNT(*) as total
FROM temp.rawData
group by gender_concept_id;


CREATE GLOBAL TEMPORARY TABLE temp.ageStats(gender_concept_id INT,age_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.ageStats(gender_concept_id,age_value,total,rn)
select
gender_concept_id, age_value, COUNT(*) as total, row_number() over (order by age_value) as rn
FROM temp.rawData
group by gender_concept_id, age_value;

CREATE GLOBAL TEMPORARY TABLE temp.ageStatsPrior(gender_concept_id INT,age_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.ageStatsPrior(gender_concept_id,age_value,total,accumulated)
select
s.gender_concept_id, s.age_value, s.total, sum(p.total) as accumulated
from temp.ageStats s
         join temp.ageStats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
group by s.gender_concept_id, s.age_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_104(analysis_id INT,stratum_1 VARCHAR(255),count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_104(analysis_id,stratum_1,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
       104 as analysis_id,
       CAST(o.gender_concept_id AS VARCHAR(255)) as stratum_1,
       o.total as count_value,
       o.min_value,
       o.max_value,
       o.avg_value,
       o.stdev_value,
       MIN(case when p.accumulated >= .50 * o.total then age_value end) as median_value,
       MIN(case when p.accumulated >= .10 * o.total then age_value end) as p10_value,
       MIN(case when p.accumulated >= .25 * o.total then age_value end) as p25_value,
       MIN(case when p.accumulated >= .75 * o.total then age_value end) as p75_value,
       MIN(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
FROM
       temp.ageStatsPrior p
           join temp.overallStats o on p.gender_concept_id = o.gender_concept_id
   GROUP BY o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

select analysis_id, stratum_1, 
cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_104
from temp.tempResults_104
;


-- truncate table temp.tempResults_104;
DROP TABLE temp.rawData;
DROP TABLE temp.overallStats;
DROP TABLE temp.ageStats;
DROP TABLE temp.ageStatsPrior;
drop table temp.tempResults_104;
