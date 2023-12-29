-- 1830	(NO ACHILLES CORRESPONDENCE) Number of descendant measurement occurrence records,by measurement_concept_id

CREATE GLOBAL TEMPORARY TABLE temp.rawData_1830 (CONCEPT_ID NUMERIC,DRC NUMERIC);
INSERT INTO temp.rawData_1830 (CONCEPT_ID,DRC)
SELECT
	ca.ANCESTOR_CONCEPT_ID AS CONCEPT_ID,
	COUNT(*) AS DRC
FROM
	@cdmDatabaseSchema.measurement co
JOIN @cdmDatabaseSchema.CONCEPT_ANCESTOR ca
	ON ca.DESCENDANT_CONCEPT_ID = co.measurement_concept_id
GROUP BY
	ca.ANCESTOR_CONCEPT_ID
;

SELECT 1830 as analysis_id,
  CAST(co.measurement_concept_id AS VARCHAR(255)) AS stratum_1,
  cast(null as varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  floor((c.DRC+99)/100)*100 as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_1830  
FROM @cdmDatabaseSchema.measurement co
	JOIN temp.rawData_1830 c
		ON c.CONCEPT_ID = co.measurement_concept_id
GROUP BY co.measurement_concept_id, c.DRC
;

drop table temp.rawData_1830;