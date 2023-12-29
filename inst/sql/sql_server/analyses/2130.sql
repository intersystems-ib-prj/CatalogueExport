-- zzzzz2130	(NO ACHILLES CORRESPONDENCE) Number of descendant device exposure records,by device_concept_id

CREATE GLOBAL TEMPORARY TABLE temp.rawData_2130 (CONCEPT_ID NUMERIC,DRC NUMERIC);
INSERT INTO temp.rawData_2130 (CONCEPT_ID,DRC)
SELECT
	ca.ANCESTOR_CONCEPT_ID AS CONCEPT_ID,
	COUNT(*) AS DRC
FROM
	@cdmDatabaseSchema.device_exposure co
JOIN @cdmDatabaseSchema.CONCEPT_ANCESTOR ca
	ON ca.DESCENDANT_CONCEPT_ID = co.device_concept_id
GROUP BY
	ca.ANCESTOR_CONCEPT_ID
;

SELECT 2130 as analysis_id,
  CAST(co.device_concept_id AS VARCHAR(255)) AS stratum_1,
  cast(null as varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  floor((c.DRC+99)/100)*100 as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_2130  
FROM @cdmDatabaseSchema.device_exposure co
	JOIN temp.rawData_2130 c
		ON c.CONCEPT_ID = co.device_concept_id
GROUP BY co.device_concept_id, c.DRC
;

drop table temp.rawData_2130;
