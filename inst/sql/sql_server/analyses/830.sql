-- 830	(NO ACHILLES CORRESPONDENCE) Number of descendant observation occurrence records,by observation_concept_id
CREATE GLOBAL TEMPORARY TABLE temp.rawData_830 (CONCEPT_ID NUMERIC,DRC NUMERIC);
INSERT INTO temp.rawData_830 (CONCEPT_ID,DRC)
SELECT
	ca.ANCESTOR_CONCEPT_ID AS CONCEPT_ID,
	COUNT(*) AS DRC
FROM
	@cdmDatabaseSchema.observation co
JOIN @cdmDatabaseSchema.CONCEPT_ANCESTOR ca
	ON ca.DESCENDANT_CONCEPT_ID = co.observation_concept_id
GROUP BY
	ca.ANCESTOR_CONCEPT_ID
;

SELECT 830 as analysis_id,
  CAST(co.observation_concept_id AS VARCHAR(255)) AS stratum_1,
  cast(null as varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  floor((c.DRC+99)/100)*100 as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_830  
FROM @cdmDatabaseSchema.observation co
	JOIN temp.rawData_830 c
		ON c.CONCEPT_ID = co.observation_concept_id
GROUP BY co.observation_concept_id, c.DRC
;

drop table temp.rawData_830;
