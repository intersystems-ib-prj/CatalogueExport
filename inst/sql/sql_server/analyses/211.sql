-- 211 (213 ACHILLES!!!)	Distribution of length of stay by visit_concept_id
-- restrict to visits inside observation period
--HINT DISTRIBUTE_ON_KEY(stratum_id)
CREATE GLOBAL TEMPORARY TABLE temp.rawData (stratum_id INT,count_value NUMERIC);
INSERT INTO temp.rawData(stratum_id, count_value)
select
visit_concept_id AS stratum_id, (CAST(visit_end_date AS DATE) - CAST(visit_start_date AS DATE)) as count_value
from @cdmDatabaseSchema.visit_occurrence vo inner join
     @cdmDatabaseSchema.observation_period op on vo.person_id = op.person_id
     -- only include events that occur during observation period
where vo.visit_start_date >= op.observation_period_start_date and
        COALESCE(vo.visit_end_date,vo.visit_start_date) <= op.observation_period_end_date;

CREATE GLOBAL TEMPORARY TABLE temp.overallStats(stratum_id INT,avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(stratum_id,avg_value,stdev_value,min_value,max_value,total)
select
stratum_id,
       CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
       min(count_value) as min_value,
       max(count_value) as max_value,
       COUNT(*) as total
FROM temp.rawData
group by stratum_id;

CREATE GLOBAL TEMPORARY TABLE temp.statsView(stratum_id INT,count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView(stratum_id,count_value,total,rn)
select
stratum_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
FROM temp.rawData
group by stratum_id, count_value;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(stratum_id INT,count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(stratum_id,count_value,total,accumulated)
select
s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView s
         join temp.statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
group by s.stratum_id, s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_213(analysis_id INT,stratum_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_213(analysis_id,stratum_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
    213 as analysis_id,
    CAST(o.stratum_id AS VARCHAR(255)) AS stratum_id,
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
        join temp.overallStats o on p.stratum_id = o.stratum_id
GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;

--HINT DISTRIBUTE_ON_KEY(stratum_1) 
select analysis_id, stratum_id as stratum_1, 
cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_211
from temp.tempResults_213
;


truncate table temp.tempResults_213;
drop table temp.tempResults_213;
DROP TABLE temp.rawData;
DROP TABLE temp.overallStats;
DROP TABLE temp.statsView;
DROP TABLE temp.priorStats;
