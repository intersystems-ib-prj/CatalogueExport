-- 103	Distribution of age at first observation period
CREATE GLOBAL TEMPORARY TABLE temp.rawData(person_id INT,age_value NUMERIC);
INSERT INTO temp.rawData(person_id,age_value)
select
p.person_id,
       MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH as age_value
from @cdmDatabaseSchema.person p
         JOIN @cdmDatabaseSchema.observation_period op on p.person_id = op.person_id
group by p.person_id, p.year_of_birth;

CREATE GLOBAL TEMPORARY TABLE temp.overallStats(avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(avg_value,stdev_value,min_value,max_value,total)
select
CAST(avg(1.0 * age_value) AS NUMERIC) as avg_value,
          CAST(STDDEV(age_value) AS NUMERIC) as stdev_value,
          min(age_value) as min_value,
          max(age_value) as max_value,
          COUNT(*) as total
FROM temp.rawData;

CREATE GLOBAL TEMPORARY TABLE temp.ageStats(age_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.ageStats(age_value,total,rn)
select
age_value, COUNT(*) as total, row_number() over (order by age_value) as rn
from temp.rawData
   group by age_value;

CREATE GLOBAL TEMPORARY TABLE temp.ageStatsPrior(age_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.ageStatsPrior(age_value,total,accumulated)
select
s.age_value, s.total, sum(p.total) as accumulated
from temp.ageStats s
            join temp.ageStats p on p.rn <= s.rn
   group by s.age_value, s.total, s.rn;


CREATE GLOBAL TEMPORARY TABLE temp.tempResults(analysis_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC,tempResults NUMERIC);
INSERT INTO temp.tempResults(analysis_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value,tempResults)
select
103 as analysis_id,
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
          --INTO #tempResults
from temp.ageStatsPrior p
            CROSS JOIN temp.overallStats o
   GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value;


select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5, 
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_103
from temp.tempResults
;

DROP TABLE temp.rawData;
DROP TABLE temp.tempResults;
DROP TABLE temp.overallStats;
DROP TABLE temp.ageStats;
DROP TABLE temp.ageStatsPrior;
