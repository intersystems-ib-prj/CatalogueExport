-- 803	Number of distinct observation occurrence concepts per person
--HINT DISTRIBUTE_ON_KEY(count_value)
CREATE GLOBAL TEMPORARY TABLE temp.rawData (count_value NUMERIC);
INSERT INTO temp.rawData(count_value)
SELECT
    COUNT(DISTINCT o.observation_concept_id) AS count_value
FROM
    @cdmDatabaseSchema.observation o
        JOIN
    @cdmDatabaseSchema.observation_period op
    ON
                o.person_id = op.person_id
            AND
                o.observation_date >= op.observation_period_start_date
            AND
                o.observation_date <= op.observation_period_end_date
GROUP BY
    o.person_id;

CREATE GLOBAL TEMPORARY TABLE temp.overallStats(avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(avg_value,stdev_value,min_value,max_value,total)
select
CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
       CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
       min(count_value) as min_value,
       max(count_value) as max_value,
       COUNT(*) as total
from temp.rawData;

CREATE GLOBAL TEMPORARY TABLE temp.statsView(count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView(count_value,total,rn)
select
count_value,
       COUNT(*) as total,
       row_number() over (order by count_value) as rn
FROM temp.rawData
group by count_value;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(count_value,total,accumulated)
select
s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView s
         join temp.statsView p on p.rn <= s.rn
group by s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_803(analysis_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_803(analysis_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
    803 as analysis_id,
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
        CROSS JOIN temp.overallStats o
GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
;


select analysis_id, 
cast(null as varchar(255)) as stratum_1, cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_803
from temp.tempResults_803
;

truncate table temp.tempResults_803;
drop table temp.tempResults_803;
DROP TABLE temp.rawData;
DROP TABLE temp.overallStats;
DROP TABLE temp.statsView;
DROP TABLE temp.priorStats;
