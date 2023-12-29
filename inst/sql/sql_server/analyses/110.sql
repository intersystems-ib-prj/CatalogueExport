-- 110	Number of persons with continuous observation in each month
CREATE GLOBAL TEMPORARY TABLE @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_110(analysis_id INT,stratum_1 VARCHAR(255),stratum_2 VARCHAR(255),stratum_3 VARCHAR(255),stratum_4 VARCHAR(255),stratum_5 VARCHAR(255),count_value NUMERIC);
INSERT INTO @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_110(analysis_id,stratum_1,stratum_2,stratum_3,stratum_4,stratum_5,count_value)
SELECT
       110 as analysis_id,
       CAST(t1.obs_month AS VARCHAR(255)) as stratum_1,
       cast(null as varchar(255)) as stratum_2, cast(null as varchar(255)) as stratum_3, cast(null as varchar(255)) as stratum_4, cast(null as varchar(255)) as stratum_5,
       COUNT(distinct op1.PERSON_ID) as count_value
FROM
       @cdmDatabaseSchema.observation_period op1
           join
       (
SELECT
DISTINCT YEAR(observation_period_start_date)*100 + MONTH(observation_period_start_date) AS obs_month,
               TO_DATE(TO_CHAR(YEAR(observation_period_start_date),'FM0000')||'-'||TO_CHAR(MONTH(observation_period_start_date),'FM00')||'-'||TO_CHAR(1,'FM00'), 'YYYY-MM-DD') AS obs_month_start,
               LAST_DAY(observation_period_start_date) AS obs_month_end
FROM @cdmDatabaseSchema.observation_period
       ) t1 on	op1.observation_period_start_date <= t1.obs_month_start
           and	op1.observation_period_end_date >= t1.obs_month_end
   group by t1.obs_month;

