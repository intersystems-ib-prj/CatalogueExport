-- 430 (NO ACHILLES CORRESPONDENCE)	Number of descendant condition occurrence records,by condition_concept_id

CREATE GLOBAL TEMPORARY TABLE temp.rawData_430 (CONCEPT_ID NUMERIC,DRC NUMERIC);
INSERT INTO temp.rawData_430 (CONCEPT_ID,DRC)
SELECT
	ca.ANCESTOR_CONCEPT_ID AS CONCEPT_ID,
	COUNT(*) AS DRC
FROM
	@cdmDatabaseSchema.CONDITION_OCCURRENCE co
JOIN @cdmDatabaseSchema.CONCEPT_ANCESTOR ca
	ON ca.DESCENDANT_CONCEPT_ID = co.CONDITION_CONCEPT_ID
GROUP BY
	ca.ANCESTOR_CONCEPT_ID
;

SELECT  430 as analysis_id,
  CAST(co.CONDITION_CONCEPT_ID AS VARCHAR(255)) AS stratum_1,
  cast(null as varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  floor((c.DRC+99)/100)*100 as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_430  
FROM @cdmDatabaseSchema.CONDITION_OCCURRENCE co
	JOIN temp.rawData_430 c
		ON c.CONCEPT_ID = co.CONDITION_CONCEPT_ID
GROUP BY co.CONDITION_CONCEPT_ID, c.DRC
;

drop table temp.rawData_430;
