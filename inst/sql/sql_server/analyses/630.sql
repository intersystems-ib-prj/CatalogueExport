-- 630 (NO ACHILLES CORRESPONDENCE)	Number of descendant procedure occurrence records,by procedure_concept_id


CREATE GLOBAL TEMPORARY TABLE temp.rawData_630 (CONCEPT_ID NUMERIC,DRC NUMERIC);
INSERT INTO temp.rawData_630 (CONCEPT_ID,DRC)
SELECT
	ca.ANCESTOR_CONCEPT_ID AS CONCEPT_ID,
	COUNT(*) AS DRC
FROM
	@cdmDatabaseSchema.procedure_occurrence co
JOIN @cdmDatabaseSchema.CONCEPT_ANCESTOR ca
	ON ca.DESCENDANT_CONCEPT_ID = co.procedure_concept_id
GROUP BY
	ca.ANCESTOR_CONCEPT_ID
;

SELECT 630 as analysis_id,
  CAST(co.procedure_concept_id AS VARCHAR(255)) AS stratum_1,
  cast(null as varchar(255)) AS stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  floor((c.DRC+99)/100)*100 as count_value
into @scratchDatabaseSchema@schemaDelim@tempAchillesPrefix_630  
FROM @cdmDatabaseSchema.procedure_occurrence co
	JOIN temp.rawData_630 c
		ON c.CONCEPT_ID = co.procedure_concept_id
GROUP BY co.procedure_concept_id, c.DRC
;

drop table temp.rawData_630;
