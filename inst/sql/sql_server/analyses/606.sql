-- 606	Distribution of age by procedure_concept_id
--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE GLOBAL TEMPORARY TABLE temp.rawData_606(subject_id INT,gender_concept_id INT,count_value NUMERIC);
INSERT INTO temp.rawData_606(subject_id,gender_concept_id,count_value)
SELECT
    po.procedure_concept_id AS subject_id,
    p.gender_concept_id,
    po.procedure_start_year - p.year_of_birth AS count_value
FROM
    @cdmDatabaseSchema.person p
    JOIN (
SELECT
    po.person_id,
    po.procedure_concept_id,
    MIN(YEAR(po.procedure_date)) AS procedure_start_year
FROM
    @cdmDatabaseSchema.procedure_occurrence po
    JOIN
    @cdmDatabaseSchema.observation_period op
    ON
    po.person_id = op.person_id
    AND
    po.procedure_date >= op.observation_period_start_date
    AND
    po.procedure_date <= op.observation_period_end_date
    GROUP BY
    po.person_id,
    po.procedure_concept_id
    ) po
    ON
    p.person_id = po.person_id
        ;

--HINT DISTRIBUTE_ON_KEY(stratum1_id)
CREATE GLOBAL TEMPORARY TABLE temp.overallStats(stratum1_id INT,stratum2_id INT,avg_value NUMERIC,stdev_value NUMERIC,min_value NUMERIC,max_value NUMERIC,total NUMERIC);
INSERT INTO temp.overallStats(stratum1_id,stratum2_id,avg_value,stdev_value,min_value,max_value,total)
select
subject_id as stratum1_id,
    gender_concept_id as stratum2_id,
    CAST(avg(1.0 * count_value) AS NUMERIC) as avg_value,
    CAST(STDDEV(count_value) AS NUMERIC) as stdev_value,
    min(count_value) as min_value,
    max(count_value) as max_value,
    COUNT(*) as total
FROM temp.rawData_606
    group by subject_id, gender_concept_id;

CREATE GLOBAL TEMPORARY TABLE temp.statsView(stratum1_id INT,stratum2_id INT,count_value NUMERIC,total NUMERIC,rn NUMERIC);
INSERT INTO temp.statsView(stratum1_id,stratum2_id,count_value,total,rn)
select
subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
FROM temp.rawData_606
    group by subject_id, gender_concept_id, count_value;

CREATE GLOBAL TEMPORARY TABLE temp.priorStats(stratum1_id INT,stratum2_id INT,count_value NUMERIC,total NUMERIC,accumulated NUMERIC);
INSERT INTO temp.priorStats(stratum1_id,stratum2_id,count_value,total,accumulated)
select
s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
from temp.statsView s
    join temp.statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
    group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn;

CREATE GLOBAL TEMPORARY TABLE temp.tempResults_606(analysis_id INT,stratum1_id INT,stratum2_id INT,count_value NUMERIC,min_value NUMERIC,max_value NUMERIC,avg_value NUMERIC,stdev_value NUMERIC,median_value NUMERIC,p10_value NUMERIC,p25_value NUMERIC,p75_value NUMERIC,p90_value NUMERIC);
INSERT INTO temp.tempResults_606(analysis_id,stratum1_id,stratum2_id,count_value,min_value,max_value,avg_value,stdev_value,median_value,p10_value,p25_value,p75_value,p90_value)
SELECT
    606 as analysis_id,
    CAST(o.stratum1_id AS VARCHAR(255)) AS stratum1_id,
    CAST(o.stratum2_id AS VARCHAR(255)) AS stratum2_id,
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
    join temp.overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
    GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
        ;

  
select analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_dist_606
from temp.tempResults_606
;        

    truncate table temp.tempResults_606;
    drop table temp.tempResults_606;
    truncate table temp.rawData_606;
    drop table temp.rawData_606;
    DROP TABLE temp.overallStats;
    DROP TABLE temp.statsView;
    DROp TABLE temp.priorStats;
