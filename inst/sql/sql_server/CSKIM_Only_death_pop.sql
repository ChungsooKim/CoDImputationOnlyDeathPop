SELECT person_id, MAX_VISIT_END as start_date, MAX_VISIT_END as end_date
INTO #final_cohort
FROM (SELECT VO.person_id, MIN(VISIT_START_DATE) AS MIN_VISIT_START, MAX(VISIT_END_DATE) AS MAX_VISIT_END, 
      LAST_VISIT_START_DATE = (SELECT MAX(VISIT_START_DATE) FROM @cdm_database_schema.VISIT_OCCURRENCE),
      D.death_date
      FROM @cdm_database_schema.VISIT_OCCURRENCE VO
      JOIN @cdm_database_schema.DEATH D
      ON VO.person_id = D.person_id
      GROUP BY VO.person_id, D.death_date) X
WHERE MAX_VISIT_END <= DATEADD(YEAR, -1, LAST_VISIT_START_DATE) AND DATEDIFF(DAY,MIN_VISIT_START,MAX_VISIT_END)>=365
AND DATEDIFF(DAY, DEATH_DATE, MAX_VISIT_END) <= 60;


DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
SELECT @target_cohort_id as cohort_definition_id, person_id, start_date, end_date 
FROM #final_cohort CO
;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;