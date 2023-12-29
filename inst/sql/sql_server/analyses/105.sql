-- 105	Length of observation (days) of first observation period

CREATE GLOBAL TEMPORARY TABLE tempObs_105(count_value NUMERIC,rn NUMERIC);
INSERT INTO tempObs_105(count_value,rn)
SELECT
count_value, rn
FROM
       (
select
(CAST(op.observation_period_end_date AS DATE) - CAST(op.observation_period_start_date AS DATE)) as count_value,
                  ROW_NUMBER() over (PARTITION by op.person_id order by op.observation_period_start_date asc) as rn
from @cdmDatabaseSchema.observation_period op
       ) A
   where rn = 1;

CREATE GLOBAL TEMPORARY TABLE temp.statsView_105(count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView_105(count_value,total,rn)
SELECT
    count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
FROM
    tempObs_105
group by count_value;

--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE GLOBAL TEMPORARY TABLE temp.overallStats(avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(avg_value,stdev_value,min_value,max_value,total)
select
CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
       min(count_value) as min_value,
       max(count_value) as max_value,
       COUNT(*) as total
from tempObs_105;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(count_value,total,accumulated)
select
s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView_105 s
         join temp.statsView_105 p on p.rn <= s.rn
group by s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_105(analysis_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_105(analysis_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
    105 as analysis_id,
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
        CROSS JOIN temp.overallStats o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;


select analysis_id,
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5, count_value,
min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_105
from temp.tempResults_105
;

truncate table tempObs_105;
drop table tempObs_105;
truncate table temp.statsView_105;
drop table temp.statsView_105;
truncate table temp.tempResults_105;
drop table temp.tempResults_105;

DROP TABLE temp.priorStats;
DROP TABLE temp.overallStats;