WITH sofa_3others_day1_to_day4 AS

(

WITH
    t1f_day1 AS (
    SELECT
      patientunitstayid,
      physicalexamoffset,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/eyes%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_eyes,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/verbal%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_verbal,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/motor%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_motor
    FROM
      `physionet-data.eicu_crd.physicalexam` pe
    WHERE
      (LOWER(physicalexampath) LIKE '%gcs/eyes%'
        OR LOWER(physicalexampath) LIKE '%gcs/verbal%'
        OR LOWER(physicalexampath) LIKE '%gcs/motor%')
      AND physicalexamoffset BETWEEN -1440
      AND 1440
    GROUP BY
      patientunitstayid,
      physicalexamoffset ),
    t1_day1 AS (
    SELECT
      patientunitstayid,
      MIN(coalesce(gcs_eyes,
          4) + coalesce(gcs_verbal,
          5) + coalesce(gcs_motor,
          6)) AS gcs
    FROM
      t1f_day1
    GROUP BY
      patientunitstayid ),
    t2_day1 AS (
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN LOWER(labname) LIKE 'total bili%' THEN labresult
          ELSE NULL END) AS bili,
      MIN(CASE
          WHEN LOWER(labname) LIKE 'platelet%' THEN labresult
          ELSE NULL END) AS plt
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      `physionet-data.eicu_crd.lab` lb
    ON
      pt.patientunitstayid=lb.patientunitstayid
    WHERE
      labresultoffset BETWEEN -1440
      AND 1440
    GROUP BY
      pt.patientunitstayid ),

t1f_day2 AS (
    SELECT
      patientunitstayid,
      physicalexamoffset,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/eyes%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_eyes,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/verbal%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_verbal,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/motor%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_motor
    FROM
      `physionet-data.eicu_crd.physicalexam` pe
    WHERE
      (LOWER(physicalexampath) LIKE '%gcs/eyes%'
        OR LOWER(physicalexampath) LIKE '%gcs/verbal%'
        OR LOWER(physicalexampath) LIKE '%gcs/motor%')
      AND physicalexamoffset BETWEEN 1440
      AND 1440*2
    GROUP BY
      patientunitstayid,
      physicalexamoffset ),
    t1_day2 AS (
    SELECT
      patientunitstayid,
      MIN(coalesce(gcs_eyes,
          4) + coalesce(gcs_verbal,
          5) + coalesce(gcs_motor,
          6)) AS gcs
    FROM
      t1f_day2
    GROUP BY
      patientunitstayid ),
    t2_day2 AS (
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN LOWER(labname) LIKE 'total bili%' THEN labresult
          ELSE NULL END) AS bili,
      MIN(CASE
          WHEN LOWER(labname) LIKE 'platelet%' THEN labresult
          ELSE NULL END) AS plt
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      `physionet-data.eicu_crd.lab` lb
    ON
      pt.patientunitstayid=lb.patientunitstayid
    WHERE
      labresultoffset BETWEEN 1440
      AND 1440*2
    GROUP BY
      pt.patientunitstayid ),

t1f_day3 AS (
    SELECT
      patientunitstayid,
      physicalexamoffset,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/eyes%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_eyes,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/verbal%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_verbal,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/motor%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_motor
    FROM
      `physionet-data.eicu_crd.physicalexam` pe
    WHERE
      (LOWER(physicalexampath) LIKE '%gcs/eyes%'
        OR LOWER(physicalexampath) LIKE '%gcs/verbal%'
        OR LOWER(physicalexampath) LIKE '%gcs/motor%')
      AND physicalexamoffset BETWEEN 1440*2
      AND 1440*3
    GROUP BY
      patientunitstayid,
      physicalexamoffset ),
    t1_day3 AS (
    SELECT
      patientunitstayid,
      MIN(coalesce(gcs_eyes,
          4) + coalesce(gcs_verbal,
          5) + coalesce(gcs_motor,
          6)) AS gcs
    FROM
      t1f_day3
    GROUP BY
      patientunitstayid ),
    t2_day3 AS (
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN LOWER(labname) LIKE 'total bili%' THEN labresult
          ELSE NULL END) AS bili,
      MIN(CASE
          WHEN LOWER(labname) LIKE 'platelet%' THEN labresult
          ELSE NULL END) AS plt
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      `physionet-data.eicu_crd.lab` lb
    ON
      pt.patientunitstayid=lb.patientunitstayid
    WHERE
      labresultoffset BETWEEN 1440*2
      AND 1440*3
    GROUP BY
      pt.patientunitstayid ),
      

t1f_day4 AS (
    SELECT
      patientunitstayid,
      physicalexamoffset,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/eyes%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_eyes,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/verbal%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_verbal,
      MIN(CASE
          WHEN LOWER(physicalexampath) LIKE '%gcs/motor%' THEN CAST(physicalexamvalue AS INT64)
          ELSE NULL END) AS gcs_motor
    FROM
      `physionet-data.eicu_crd.physicalexam` pe
    WHERE
      (LOWER(physicalexampath) LIKE '%gcs/eyes%'
        OR LOWER(physicalexampath) LIKE '%gcs/verbal%'
        OR LOWER(physicalexampath) LIKE '%gcs/motor%')
      AND physicalexamoffset BETWEEN 1440*3
      AND 1440*4
    GROUP BY
      patientunitstayid,
      physicalexamoffset ),
    t1_day4 AS (
    SELECT
      patientunitstayid,
      MIN(coalesce(gcs_eyes,
          4) + coalesce(gcs_verbal,
          5) + coalesce(gcs_motor,
          6)) AS gcs
    FROM
      t1f_day4
    GROUP BY
      patientunitstayid ),
    t2_day4 AS (
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN LOWER(labname) LIKE 'total bili%' THEN labresult
          ELSE NULL END) AS bili,
      MIN(CASE
          WHEN LOWER(labname) LIKE 'platelet%' THEN labresult
          ELSE NULL END) AS plt
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      `physionet-data.eicu_crd.lab` lb
    ON
      pt.patientunitstayid=lb.patientunitstayid
    WHERE
      labresultoffset BETWEEN 1440*3
      AND 1440*4
    GROUP BY
      pt.patientunitstayid )
      
      
  SELECT
    DISTINCT pt.patientunitstayid,
    
    
    MIN(t1_day1.gcs) AS gcs_day1,
    MAX(t2_day1.bili) AS bili_day1,
    MIN(t2_day1.plt) AS plt_day1,
    MAX(CASE
        WHEN t2_day1.plt<20 THEN 4
        WHEN t2_day1.plt<50 THEN 3
        WHEN t2_day1.plt<100 THEN 2
        WHEN t2_day1.plt<150 THEN 1
        ELSE 0 END) AS sofacoag_day1,
    MAX(CASE
        WHEN t2_day1.bili>12 THEN 4
        WHEN t2_day1.bili>6 THEN 3
        WHEN t2_day1.bili>2 THEN 2
        WHEN t2_day1.bili>1.2 THEN 1
        ELSE 0 END) AS sofaliver_day1,
    MAX(CASE
        WHEN t1_day1.gcs=15 THEN 0
        WHEN t1_day1.gcs>=13 THEN 1
        WHEN t1_day1.gcs>=10 THEN 2
        WHEN t1_day1.gcs>=6 THEN 3
        WHEN t1_day1.gcs>=3 THEN 4
        ELSE 0 END) AS sofacns_day1,
        
    
    MIN(t1_day2.gcs) AS gcs_day2,
    MAX(t2_day2.bili) AS bili_day2,
    MIN(t2_day2.plt) AS plt_day2,
    MAX(CASE
        WHEN t2_day2.plt<20 THEN 4
        WHEN t2_day2.plt<50 THEN 3
        WHEN t2_day2.plt<100 THEN 2
        WHEN t2_day2.plt<150 THEN 1
        ELSE 0 END) AS sofacoag_day2,
    MAX(CASE
        WHEN t2_day2.bili>12 THEN 4
        WHEN t2_day2.bili>6 THEN 3
        WHEN t2_day2.bili>2 THEN 2
        WHEN t2_day2.bili>1.2 THEN 1
        ELSE 0 END) AS sofaliver_day2,
    MAX(CASE
        WHEN t1_day2.gcs=15 THEN 0
        WHEN t1_day2.gcs>=13 THEN 1
        WHEN t1_day2.gcs>=10 THEN 2
        WHEN t1_day2.gcs>=6 THEN 3
        WHEN t1_day2.gcs>=3 THEN 4
        ELSE 0 END) AS sofacns_day2,
        
    MIN(t1_day3.gcs) AS gcs_day3,
    MAX(t2_day3.bili) AS bili_day3,
    MIN(t2_day3.plt) AS plt_day3,
    MAX(CASE
        WHEN t2_day3.plt<20 THEN 4
        WHEN t2_day3.plt<50 THEN 3
        WHEN t2_day3.plt<100 THEN 2
        WHEN t2_day3.plt<150 THEN 1
        ELSE 0 END) AS sofacoag_day3,
    MAX(CASE
        WHEN t2_day3.bili>12 THEN 4
        WHEN t2_day3.bili>6 THEN 3
        WHEN t2_day3.bili>2 THEN 2
        WHEN t2_day3.bili>1.2 THEN 1
        ELSE 0 END) AS sofaliver_day3,
    MAX(CASE
        WHEN t1_day3.gcs=15 THEN 0
        WHEN t1_day3.gcs>=13 THEN 1
        WHEN t1_day3.gcs>=10 THEN 2
        WHEN t1_day3.gcs>=6 THEN 3
        WHEN t1_day3.gcs>=3 THEN 4
        ELSE 0 END) AS sofacns_day3,
        
    
    MIN(t1_day4.gcs) AS gcs_day4,
    MAX(t2_day4.bili) AS bili_day4,
    MIN(t2_day4.plt) AS plt_day4,
    MAX(CASE
        WHEN t2_day4.plt<20 THEN 4
        WHEN t2_day4.plt<50 THEN 3
        WHEN t2_day4.plt<100 THEN 2
        WHEN t2_day4.plt<150 THEN 1
        ELSE 0 END) AS sofacoag_day4,
    MAX(CASE
        WHEN t2_day4.bili>12 THEN 4
        WHEN t2_day4.bili>6 THEN 3
        WHEN t2_day4.bili>2 THEN 2
        WHEN t2_day4.bili>1.2 THEN 1
        ELSE 0 END) AS sofaliver_day4,
    MAX(CASE
        WHEN t1_day4.gcs=15 THEN 0
        WHEN t1_day4.gcs>=13 THEN 1
        WHEN t1_day4.gcs>=10 THEN 2
        WHEN t1_day4.gcs>=6 THEN 3
        WHEN t1_day4.gcs>=3 THEN 4
        ELSE 0 END) AS sofacns_day4
        
  FROM
    `physionet-data.eicu_crd.patient` pt
    
  LEFT OUTER JOIN
    t1_day1
  ON
    t1_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day1
  ON
    t2_day1.patientunitstayid=pt.patientunitstayid
    
    LEFT OUTER JOIN
    t1_day2
  ON
    t1_day2.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day2
  ON
    t2_day2.patientunitstayid=pt.patientunitstayid
    
    LEFT OUTER JOIN
    t1_day3
  ON
    t1_day3.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day3
  ON
    t2_day3.patientunitstayid=pt.patientunitstayid
    
    LEFT OUTER JOIN
    t1_day4
  ON
    t1_day4.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day4
  ON
    t2_day4.patientunitstayid=pt.patientunitstayid
    
  GROUP BY
    pt.patientunitstayid,
    t1_day1.gcs,
    t2_day1.bili,
    t2_day1.plt,
    
    t1_day2.gcs,
    t2_day2.bili,
    t2_day2.plt,
    
    t1_day3.gcs,
    t2_day3.bili,
    t2_day3.plt,
    
    t1_day4.gcs,
    t2_day4.bili,
    t2_day4.plt
  ORDER BY
    pt.patientunitstayid),
    
 --aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
 sofa_cv_day1_to_day4 AS (
    WITH
------------------VARS day1------------------------
    t1_day1 AS (
    WITH
      tt1 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN noninvasivemean IS NOT NULL THEN noninvasivemean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalaperiodic`
      WHERE
        observationoffset BETWEEN -1440 AND 1440
      GROUP BY
        patientunitstayid ),
      tt2 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN systemicmean IS NOT NULL THEN systemicmean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalperiodic`
      WHERE
        observationoffset BETWEEN -1440 AND 1440
      GROUP BY
        patientunitstayid )
    SELECT
      pt.patientunitstayid,
      CASE
        WHEN tt1.map IS NOT NULL THEN tt1.map
        WHEN tt2.map IS NOT NULL THEN tt2.map
        ELSE NULL
      END AS map
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      tt1
    ON
      tt1.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
      tt2
    ON
      tt2.patientunitstayid=pt.patientunitstayid
    ORDER BY
      pt.patientunitstayid ),
    t2_day1 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(
        CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' THEN ROUND(CAST(drugrate AS INT64)/3,3) -- rate in ml/h * 1600 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS dopa
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dopamine%'
      AND infusionoffset BETWEEN -1440 AND 1440
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t3_day1 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/300,3) -- rate in ml/h * 16 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/80,3)-- divide by 80 kg
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS norepi
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%epinephrine%'
      AND infusionoffset BETWEEN -1440 AND 1440
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'-- this regex will capture norepi as well
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t4_day1 AS (
    SELECT
      DISTINCT patientunitstayid,
      1 AS dobu
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dobutamin%'
      AND drugrate <>''
      AND drugrate<>'.'
      AND drugrate <>'0'
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND infusionoffset BETWEEN -1440 AND 1440
    ORDER BY
      patientunitstayid ),
------------------VARS day2------------------------
    t1_day2 AS (
    WITH
      tt1 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN noninvasivemean IS NOT NULL THEN noninvasivemean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalaperiodic`
      WHERE
        observationoffset BETWEEN 1440 AND 1440*2
      GROUP BY
        patientunitstayid ),
      tt2 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN systemicmean IS NOT NULL THEN systemicmean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalperiodic`
      WHERE
        observationoffset BETWEEN 1440 AND 1440*2
      GROUP BY
        patientunitstayid )
    SELECT
      pt.patientunitstayid,
      CASE
        WHEN tt1.map IS NOT NULL THEN tt1.map
        WHEN tt2.map IS NOT NULL THEN tt2.map
        ELSE NULL
      END AS map
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      tt1
    ON
      tt1.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
      tt2
    ON
      tt2.patientunitstayid=pt.patientunitstayid
    ORDER BY
      pt.patientunitstayid ),
    t2_day2 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(
        CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' THEN ROUND(CAST(drugrate AS INT64)/3,3) -- rate in ml/h * 1600 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS dopa
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dopamine%'
      AND infusionoffset BETWEEN 1440 AND 1440*2
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t3_day2 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/300,3) -- rate in ml/h * 16 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/80,3)-- divide by 80 kg
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS norepi
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%epinephrine%'
      AND infusionoffset BETWEEN 1440 AND 1440*2
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'-- this regex will capture norepi as well
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t4_day2 AS (
    SELECT
      DISTINCT patientunitstayid,
      1 AS dobu
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dobutamin%'
      AND drugrate <>''
      AND drugrate<>'.'
      AND drugrate <>'0'
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND infusionoffset BETWEEN 1440 AND 1440*2
    ORDER BY
      patientunitstayid ),      
------------------VARS day3------------------------
    t1_day3 AS (
    WITH
      tt1 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN noninvasivemean IS NOT NULL THEN noninvasivemean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalaperiodic`
      WHERE
        observationoffset BETWEEN 1440*2 AND 1440*3
      GROUP BY
        patientunitstayid ),
      tt2 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN systemicmean IS NOT NULL THEN systemicmean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalperiodic`
      WHERE
        observationoffset BETWEEN 1440*2 AND 1440*3
      GROUP BY
        patientunitstayid )
    SELECT
      pt.patientunitstayid,
      CASE
        WHEN tt1.map IS NOT NULL THEN tt1.map
        WHEN tt2.map IS NOT NULL THEN tt2.map
        ELSE NULL
      END AS map
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      tt1
    ON
      tt1.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
      tt2
    ON
      tt2.patientunitstayid=pt.patientunitstayid
    ORDER BY
      pt.patientunitstayid ),
    t2_day3 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(
        CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' THEN ROUND(CAST(drugrate AS INT64)/3,3) -- rate in ml/h * 1600 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS dopa
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dopamine%'
      AND infusionoffset BETWEEN 1440*2 AND 1440*3
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t3_day3 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/300,3) -- rate in ml/h * 16 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/80,3)-- divide by 80 kg
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS norepi
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%epinephrine%'
      AND infusionoffset BETWEEN 1440*2 AND 1440*3
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'-- this regex will capture norepi as well
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t4_day3 AS (
    SELECT
      DISTINCT patientunitstayid,
      1 AS dobu
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dobutamin%'
      AND drugrate <>''
      AND drugrate<>'.'
      AND drugrate <>'0'
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND infusionoffset BETWEEN 1440*2 AND 1440*3
    ORDER BY
      patientunitstayid ),         
------------------VARS day4------------------------
    t1_day4 AS (
    WITH
      tt1 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN noninvasivemean IS NOT NULL THEN noninvasivemean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalaperiodic`
      WHERE
        observationoffset BETWEEN 1440*3 AND 1440*4
      GROUP BY
        patientunitstayid ),
      tt2 AS (
      SELECT
        patientunitstayid,
        MIN(
          CASE
            WHEN systemicmean IS NOT NULL THEN systemicmean
            ELSE NULL END) AS map
      FROM
        `physionet-data.eicu_crd.vitalperiodic`
      WHERE
        observationoffset BETWEEN 1440*3 AND 1440*4
      GROUP BY
        patientunitstayid )
    SELECT
      pt.patientunitstayid,
      CASE
        WHEN tt1.map IS NOT NULL THEN tt1.map
        WHEN tt2.map IS NOT NULL THEN tt2.map
        ELSE NULL
      END AS map
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      tt1
    ON
      tt1.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
      tt2
    ON
      tt2.patientunitstayid=pt.patientunitstayid
    ORDER BY
      pt.patientunitstayid ),
    t2_day4 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(
        CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' THEN ROUND(CAST(drugrate AS INT64)/3,3) -- rate in ml/h * 1600 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS dopa
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dopamine%'
      AND infusionoffset BETWEEN 1440*3 AND 1440*4
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t3_day4 AS (
    SELECT
      DISTINCT patientunitstayid,
      MAX(CASE
          WHEN LOWER(drugname) LIKE '%(ml/hr)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/300,3) -- rate in ml/h * 16 mcg/ml / 80 kg / 60 min, to convert in mcg/kg/min
          WHEN LOWER(drugname) LIKE '%(mcg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN ROUND(CAST(drugrate AS INT64)/80,3)-- divide by 80 kg
          WHEN LOWER(drugname) LIKE '%(mcg/kg/min)%' AND drugrate<>'' AND drugrate<>'.' THEN CAST(drugrate AS INT64)
          ELSE NULL
        END ) AS norepi
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%epinephrine%'
      AND infusionoffset BETWEEN 1440*3 AND 1440*4
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND drugrate<>''
      AND drugrate<>'.'-- this regex will capture norepi as well
    GROUP BY
      patientunitstayid
    ORDER BY
      patientunitstayid ),
    t4_day4 AS (
    SELECT
      DISTINCT patientunitstayid,
      1 AS dobu
    FROM
      `physionet-data.eicu_crd.infusiondrug` id
    WHERE
      LOWER(drugname) LIKE '%dobutamin%'
      AND drugrate <>''
      AND drugrate<>'.'
      AND drugrate <>'0'
      AND REGEXP_CONTAINS(drugrate, '^[0-9]{0,5}$')
      AND infusionoffset BETWEEN 1440*3 AND 1440*4
    ORDER BY
      patientunitstayid )          
  SELECT
    pt.patientunitstayid,
    ------------------VARS day1------------------------
    t1_day1.map AS map_day1,
    t2_day1.dopa AS dopa_day1,
    t3_day1.norepi AS norepi_day1,
    t4_day1.dobu AS dobu_day1,
    (CASE
        WHEN t2_day1.dopa >= 15 OR t3_day1.norepi >0.1 THEN 4
        WHEN t2_day1.dopa > 5 OR (t3_day1.norepi > 0 AND t3_day1.norepi <= 0.1) THEN 3
        WHEN t2_day1.dopa <= 5 OR t4_day1.dobu > 0 THEN 2 WHEN t1_day1.map < 70 THEN 1 
        ELSE 0
     END) AS SOFA_cv_day1, 
------------------VARS day2------------------------
    t1_day2.map AS map_day2,
    t2_day2.dopa AS dopa_day2,
    t3_day2.norepi AS norepi_day2,
    t4_day2.dobu AS dobu_day2,
    (CASE
        WHEN t2_day2.dopa >= 15 OR t3_day2.norepi >0.1 THEN 4
        WHEN t2_day2.dopa > 5 OR (t3_day2.norepi > 0 AND t3_day2.norepi <= 0.1) THEN 3
        WHEN t2_day2.dopa <= 5 OR t4_day2.dobu > 0 THEN 2 WHEN t1_day2.map < 70 THEN 1 
        ELSE 0
     END) AS SOFA_cv_day2,  
------------------VARS day3------------------------
    t1_day3.map AS map_day3,
    t2_day3.dopa AS dopa_day3,
    t3_day3.norepi AS norepi_day3,
    t4_day3.dobu AS dobu_day3,
    (CASE
        WHEN t2_day3.dopa >= 15 OR t3_day3.norepi >0.1 THEN 4
        WHEN t2_day3.dopa > 5 OR (t3_day3.norepi > 0 AND t3_day3.norepi <= 0.1) THEN 3
        WHEN t2_day3.dopa <= 5 OR t4_day3.dobu > 0 THEN 2 WHEN t1_day3.map < 70 THEN 1 
        ELSE 0
     END) AS SOFA_cv_day3,      
------------------VARS day4------------------------
    t1_day4.map AS map_day4,
    t2_day4.dopa AS dopa_day4,
    t3_day4.norepi AS norepi_day4,
    t4_day4.dobu AS dobu_day4,
    (CASE
        WHEN t2_day4.dopa >= 15 OR t3_day4.norepi >0.1 THEN 4
        WHEN t2_day4.dopa > 5 OR (t3_day4.norepi > 0 AND t3_day4.norepi <= 0.1) THEN 3
        WHEN t2_day4.dopa <= 5 OR t4_day4.dobu > 0 THEN 2 WHEN t1_day4.map < 70 THEN 1 
        ELSE 0
     END) AS SOFA_cv_day4       
  FROM
    `physionet-data.eicu_crd.patient` pt
  ------------------VARS day1------------------------  
  LEFT OUTER JOIN
    t1_day1
  ON
    t1_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day1
  ON
    t2_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t3_day1
  ON
    t3_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t4_day1
  ON
    t4_day1.patientunitstayid=pt.patientunitstayid
  ------------------VARS day2------------------------     
 LEFT OUTER JOIN
    t1_day2
  ON
    t1_day2.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day2
  ON
    t2_day2.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t3_day2
  ON
    t3_day2.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t4_day2
  ON
    t4_day2.patientunitstayid=pt.patientunitstayid    
  ------------------VARS day3------------------------     
 LEFT OUTER JOIN
    t1_day3
  ON
    t1_day3.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day3
  ON
    t2_day3.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t3_day3
  ON
    t3_day3.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t4_day3
  ON
    t4_day3.patientunitstayid=pt.patientunitstayid      
    ------------------VARS day4------------------------     
 LEFT OUTER JOIN
    t1_day4
  ON
    t1_day4.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day4
  ON
    t2_day4.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t3_day4
  ON
    t3_day4.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t4_day4
  ON
    t4_day4.patientunitstayid=pt.patientunitstayid  
  
  
  ORDER BY
    pt.patientunitstayid

    ),
 --aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa    
 sofa_renal_day1_to_day4 AS(
    WITH
    t1_day1 AS (
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN LOWER(labname) LIKE 'creatin%' THEN labresult
          ELSE NULL END) AS creat
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      `physionet-data.eicu_crd.lab` lb
    ON
      pt.patientunitstayid=lb.patientunitstayid
    WHERE
      labresultoffset BETWEEN -1440
      AND 1440
    GROUP BY
      pt.patientunitstayid ),
    t2_day1 AS (
    WITH
      uotemp AS (
      SELECT
        patientunitstayid,
        CASE
          WHEN dayz=1 THEN SUM(outputtotal)
          ELSE NULL
        END AS uod1
      FROM (
        SELECT
          DISTINCT patientunitstayid,
          intakeoutputoffset,
          outputtotal,
          (CASE
              WHEN (intakeoutputoffset) BETWEEN -1440 AND 1440 THEN 1
              ELSE NULL END) AS dayz
        FROM
          `physionet-data.eicu_crd.intakeoutput`
          -- what does this account for?
        WHERE
          intakeoutputoffset BETWEEN -1440
          AND 1440
        ORDER BY
          patientunitstayid,
          intakeoutputoffset ) AS temp
      GROUP BY
        patientunitstayid,
        temp.dayz )
    SELECT
      pt.patientunitstayid,
      MAX(CASE
          WHEN uod1 IS NOT NULL THEN uod1
          ELSE NULL END) AS UO
    FROM
      `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
      uotemp
    ON
      uotemp.patientunitstayid=pt.patientunitstayid
    GROUP BY
      pt.patientunitstayid ),
      
-- ------------ day2 --------------
    t1_day2 AS (
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN LOWER(labname) LIKE 'creatin%' THEN labresult
            ELSE NULL END) AS creat
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        `physionet-data.eicu_crd.lab` lb
      ON
        pt.patientunitstayid=lb.patientunitstayid
      WHERE
        labresultoffset BETWEEN 1440
        AND 1440*2
      GROUP BY
        pt.patientunitstayid ),
    t2_day2 AS (
      WITH
        uotemp AS (
        SELECT
          patientunitstayid,
          CASE
            WHEN dayz=1 THEN SUM(outputtotal)
            ELSE NULL
          END AS uod1
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            intakeoutputoffset,
            outputtotal,
            (CASE
                WHEN (intakeoutputoffset) BETWEEN 1440 AND 1440*2 THEN 1
                ELSE NULL END) AS dayz
          FROM
            `physionet-data.eicu_crd.intakeoutput`
          WHERE
            intakeoutputoffset BETWEEN 1440
            AND 1440*2
          ORDER BY
            patientunitstayid,
            intakeoutputoffset ) AS temp
        GROUP BY
          patientunitstayid,
          temp.dayz )
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN uod1 IS NOT NULL THEN uod1
            ELSE NULL END) AS UO
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        uotemp
      ON
        uotemp.patientunitstayid=pt.patientunitstayid
      GROUP BY
        pt.patientunitstayid ),

-- ----------- day3 -------------        
 
     t1_day3 AS (
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN LOWER(labname) LIKE 'creatin%' THEN labresult
            ELSE NULL END) AS creat
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        `physionet-data.eicu_crd.lab` lb
      ON
        pt.patientunitstayid=lb.patientunitstayid
      WHERE
        labresultoffset BETWEEN 1440*2
        AND 1440*3
      GROUP BY
        pt.patientunitstayid ),
    t2_day3 AS (
      WITH
        uotemp AS (
        SELECT
          patientunitstayid,
          CASE
            WHEN dayz=1 THEN SUM(outputtotal)
            ELSE NULL
          END AS uod1
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            intakeoutputoffset,
            outputtotal,
            (CASE
                WHEN (intakeoutputoffset) BETWEEN 1440*2 AND 1440*3 THEN 1
                ELSE NULL END) AS dayz
          FROM
            `physionet-data.eicu_crd.intakeoutput`
          WHERE
            intakeoutputoffset BETWEEN 1440*2
            AND 1440*3
          ORDER BY
            patientunitstayid,
            intakeoutputoffset ) AS temp
        GROUP BY
          patientunitstayid,
          temp.dayz )
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN uod1 IS NOT NULL THEN uod1
            ELSE NULL END) AS UO
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        uotemp
      ON
        uotemp.patientunitstayid=pt.patientunitstayid
      GROUP BY
        pt.patientunitstayid ),


-- ----------- day3 -------------        
 
     t1_day4 AS (
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN LOWER(labname) LIKE 'creatin%' THEN labresult
            ELSE NULL END) AS creat
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        `physionet-data.eicu_crd.lab` lb
      ON
        pt.patientunitstayid=lb.patientunitstayid
      WHERE
        labresultoffset BETWEEN 1440*3
        AND 1440*4
      GROUP BY
        pt.patientunitstayid ),
    t2_day4 AS (
      WITH
        uotemp AS (
        SELECT
          patientunitstayid,
          CASE
            WHEN dayz=1 THEN SUM(outputtotal)
            ELSE NULL
          END AS uod1
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            intakeoutputoffset,
            outputtotal,
            (CASE
                WHEN (intakeoutputoffset) BETWEEN 1440*3 AND 1440*4 THEN 1
                ELSE NULL END) AS dayz
          FROM
            `physionet-data.eicu_crd.intakeoutput`
          WHERE
            intakeoutputoffset BETWEEN 1440*3
            AND 1440*4
          ORDER BY
            patientunitstayid,
            intakeoutputoffset ) AS temp
        GROUP BY
          patientunitstayid,
          temp.dayz )
      SELECT
        pt.patientunitstayid,
        MAX(CASE
            WHEN uod1 IS NOT NULL THEN uod1
            ELSE NULL END) AS UO
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        uotemp
      ON
        uotemp.patientunitstayid=pt.patientunitstayid
      GROUP BY
        pt.patientunitstayid )
        
SELECT
    pt.patientunitstayid,
    -- t1.creat, t2.uo,
    (CASE
        WHEN t2_day1.uo <200 OR t1_day1.creat>5 THEN 4
        WHEN t2_day1.uo <500
      OR t1_day1.creat >3.5 THEN 3
        WHEN t1_day1.creat BETWEEN 2 AND 3.5 THEN 2
        WHEN t1_day1.creat BETWEEN 1.2
      AND 2 THEN 1
        ELSE 0 END) AS sofarenal_day1,
        
    (CASE
        WHEN t2_day2.uo <200 OR t1_day2.creat>5 THEN 4
        WHEN t2_day2.uo <500
      OR t1_day2.creat >3.5 THEN 3
        WHEN t1_day2.creat BETWEEN 2 AND 3.5 THEN 2
        WHEN t1_day2.creat BETWEEN 1.2
      AND 2 THEN 1
        ELSE 0 END) AS sofarenal_day2,
    (CASE
        WHEN t2_day3.uo <200 OR t1_day3.creat>5 THEN 4
        WHEN t2_day3.uo <500
      OR t1_day3.creat >3.5 THEN 3
        WHEN t1_day3.creat BETWEEN 2 AND 3.5 THEN 2
        WHEN t1_day3.creat BETWEEN 1.2
      AND 2 THEN 1
        ELSE 0 END) AS sofarenal_day3,
    (CASE
        WHEN t2_day4.uo <200 OR t1_day4.creat>5 THEN 4
        WHEN t2_day4.uo <500
      OR t1_day4.creat >3.5 THEN 3
        WHEN t1_day4.creat BETWEEN 2 AND 3.5 THEN 2
        WHEN t1_day4.creat BETWEEN 1.2
      AND 2 THEN 1
        ELSE 0 END) AS sofarenal_day4
  FROM
    `physionet-data.eicu_crd.patient` pt
  LEFT OUTER JOIN
    t1_day1
  ON
    t1_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day1
  ON
    t2_day1.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t1_day2
  ON
    t1_day2.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day2
  ON
    t2_day2.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
    t1_day3
  ON
    t1_day3.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day3
  ON
    t2_day3.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
    t1_day4
  ON
    t1_day4.patientunitstayid=pt.patientunitstayid
  LEFT OUTER JOIN
    t2_day4
  ON
    t2_day4.patientunitstayid=pt.patientunitstayid
  ORDER BY
    pt.patientunitstayid
   ),
    
     --aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    sofa_respi_day1_to_day4 AS
    (
     WITH
    tempo2_day1 AS (
    WITH
      tempo1_day1 AS (
      WITH
        t1_day1 AS (
        SELECT
          *
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CAST(respchartvalue AS INT64)) AS rcfio2
            -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
          FROM
            `physionet-data.eicu_crd.respiratorycharting`
          WHERE
            respchartoffset BETWEEN -120
            AND 1440
            AND respchartvalue <> ''
            AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
          GROUP BY
            patientunitstayid ) AS tempo
        WHERE
          rcfio2 >20 -- many values are liters per minute!
        ORDER BY
          patientunitstayid ),
        t2_day1 AS (
        SELECT
          DISTINCT patientunitstayid,
          MAX(CAST(nursingchartvalue AS INT64)) AS ncfio2
        FROM
          `physionet-data.eicu_crd.nursecharting` nc
        WHERE
          LOWER(nursingchartcelltypevallabel) LIKE '%fio2%'
          AND REGEXP_CONTAINS(nursingchartvalue, '^[0-9]{0,2}$')
          AND nursingchartentryoffset BETWEEN -1440 AND 1440
        GROUP BY
          patientunitstayid ),
        t3_day1 AS (
        SELECT
          patientunitstayid,
          MIN(
            CASE
              WHEN sao2 IS NOT NULL THEN sao2
              ELSE NULL END) AS sao2
        FROM
          `physionet-data.eicu_crd.vitalperiodic`
        WHERE
          observationoffset BETWEEN -1440
          AND 1440
        GROUP BY
          patientunitstayid ),
        t4_day1 AS (
        SELECT
          patientunitstayid,
          MIN(CASE
              WHEN LOWER(labname) LIKE 'pao2%' THEN labresult
              ELSE NULL END) AS pao2
        FROM
          `physionet-data.eicu_crd.lab`
        WHERE
          labresultoffset BETWEEN -1440
          AND 1440
        GROUP BY
          patientunitstayid ),
        t5_day1 AS (
        WITH
          t1_day1 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN airwaytype IN ('Oral ETT', 'Nasal ETT', 'Tracheostomy') THEN 1
                ELSE NULL END) AS airway  -- either invasive airway or NULL
          FROM
            `physionet-data.eicu_crd.respiratorycare`
          WHERE
            respcarestatusoffset BETWEEN -1440
            AND 1440
          GROUP BY
            patientunitstayid-- , respcarestatusoffset
            -- order by patientunitstayid-- , respcarestatusoffset
            ),
          t2_day1 AS (
          SELECT
            DISTINCT patientunitstayid,
            1 AS ventilator
          FROM
            `physionet-data.eicu_crd.respiratorycharting` rc
          WHERE
            respchartvalue LIKE '%ventilator%'
            OR respchartvalue LIKE '%vent%'
            OR respchartvalue LIKE '%bipap%'
            OR respchartvalue LIKE '%840%'
            OR respchartvalue LIKE '%cpap%'
            OR respchartvalue LIKE '%drager%'
            OR respchartvalue LIKE 'mv%'
            OR respchartvalue LIKE '%servo%'
            OR respchartvalue LIKE '%peep%'
            AND respchartoffset BETWEEN -1440
            AND 1440
          GROUP BY
            patientunitstayid
            -- order by patientunitstayid
            ),
          t3_day1 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN treatmentstring IN ('pulmonary|ventilation and oxygenation|mechanical ventilation',  'pulmonary|ventilation and oxygenation|tracheal suctioning',  'pulmonary|ventilation and oxygenation|ventilator weaning',  'pulmonary|ventilation and oxygenation|mechanical ventilation|assist controlled',  'pulmonary|radiologic procedures / bronchoscopy|endotracheal tube',  'pulmonary|ventilation and oxygenation|oxygen therapy (> 60%)',  'pulmonary|ventilation and oxygenation|mechanical ventilation|tidal volume 6-10 ml/kg',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume controlled',  'surgery|pulmonary therapies|mechanical ventilation',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy',  'pulmonary|ventilation and oxygenation|mechanical ventilation|synchronized intermittent',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|performed during current admission for ventilatory support',  'pulmonary|ventilation and oxygenation|ventilator weaning|active',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|ventilator weaning|slow',  'surgery|pulmonary therapies|ventilator weaning',  'surgery|pulmonary therapies|tracheal suctioning',  'pulmonary|radiologic procedures / bronchoscopy|reintubation',  'pulmonary|ventilation and oxygenation|lung recruitment maneuver',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|planned',  'surgery|pulmonary therapies|ventilator weaning|rapid',  'pulmonary|ventilation and oxygenation|prone position',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|conventional',  'pulmonary|ventilation and oxygenation|mechanical ventilation|permissive hypercapnea',  'surgery|pulmonary therapies|mechanical ventilation|synchronized intermittent',  'pulmonary|medications|neuromuscular blocking agent',  'surgery|pulmonary therapies|mechanical ventilation|assist controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume assured',  'surgery|pulmonary therapies|mechanical ventilation|tidal volume 6-10 ml/kg',  'surgery|pulmonary therapies|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|nasal mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|face mask',  'surgery|pulmonary therapies|non-invasive ventilation',  'surgery|pulmonary therapies|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation|face mask' ) THEN 1
                ELSE NULL END) AS interface   -- either ETT/NiV or NULL
          FROM
            `physionet-data.eicu_crd.treatment`
          WHERE
            treatmentoffset BETWEEN -1440
            AND 1440
          GROUP BY
            patientunitstayid-- , treatmentoffset, interface
          ORDER BY
            patientunitstayid-- , treatmentoffset
            )
        SELECT
          pt.patientunitstayid,
          CASE
            WHEN t1_day1.airway IS NOT NULL OR t2_day1.ventilator IS NOT NULL OR t3_day1.interface IS NOT NULL THEN 1
            ELSE NULL
          END AS mechvent
        FROM
          `physionet-data.eicu_crd.patient` pt
        LEFT OUTER JOIN
          t1_day1
        ON
          t1_day1.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t2_day1
        ON
          t2_day1.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t3_day1
        ON
          t3_day1.patientunitstayid=pt.patientunitstayid
        ORDER BY
          pt.patientunitstayid )
      SELECT
        pt.patientunitstayid,
        t3_day1.sao2,
        t4_day1.pao2,
        (CASE
            WHEN t1_day1.rcfio2>20 THEN t1_day1.rcfio2
            WHEN t2_day1.ncfio2 >20 THEN t2_day1.ncfio2
            WHEN t1_day1.rcfio2=1 OR t2_day1.ncfio2=1 THEN 100
            ELSE NULL END) AS fio2,
        t5_day1.mechvent
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        t1_day1
      ON
        t1_day1.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t2_day1
      ON
        t2_day1.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t3_day1
      ON
        t3_day1.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t4_day1
      ON
        t4_day1.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t5_day1
      ON
        t5_day1.patientunitstayid=pt.patientunitstayid
        -- order by pt.patientunitstayid
        )
    SELECT
      *,
      -- coalesce(fio2,nullif(fio2,0),21) as fn, nullif(fio2,0) as nullifzero, coalesce(coalesce(nullif(fio2,0),21),fio2,21) as ifzero21 ,
      coalesce(pao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS pf,
      coalesce(sao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS sf
    FROM
      tempo1_day1
      -- order by fio2
      ),
      
-- -------------- day 2 -----------------------
    tempo2_day2 AS (
    WITH
      tempo1_day2 AS (
      WITH
        t1_day2 AS (
        SELECT
          *
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CAST(respchartvalue AS INT64)) AS rcfio2
            -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
          FROM
            `physionet-data.eicu_crd.respiratorycharting`
          WHERE
            respchartoffset BETWEEN 1440
            AND 1440*2
            AND respchartvalue <> ''
            AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
          GROUP BY
            patientunitstayid ) AS tempo
        WHERE
          rcfio2 >20 -- many values are liters per minute!
        ORDER BY
          patientunitstayid ),
        t2_day2 AS (
        SELECT
          DISTINCT patientunitstayid,
          MAX(CAST(nursingchartvalue AS INT64)) AS ncfio2
        FROM
          `physionet-data.eicu_crd.nursecharting` nc
        WHERE
          LOWER(nursingchartcelltypevallabel) LIKE '%fio2%'
          AND REGEXP_CONTAINS(nursingchartvalue, '^[0-9]{0,2}$')
          AND nursingchartentryoffset BETWEEN 1440 AND 1440*2
        GROUP BY
          patientunitstayid ),
        t3_day2 AS (
        SELECT
          patientunitstayid,
          MIN(
            CASE
              WHEN sao2 IS NOT NULL THEN sao2
              ELSE NULL END) AS sao2
        FROM
          `physionet-data.eicu_crd.vitalperiodic`
        WHERE
          observationoffset BETWEEN 1440 AND 1440*2
        GROUP BY
          patientunitstayid ),
        t4_day2 AS (
        SELECT
          patientunitstayid,
          MIN(CASE
              WHEN LOWER(labname) LIKE 'pao2%' THEN labresult
              ELSE NULL END) AS pao2
        FROM
          `physionet-data.eicu_crd.lab`
        WHERE
          labresultoffset BETWEEN 1440 AND 1440*2
        GROUP BY
          patientunitstayid ),
        t5_day2 AS (
        WITH
          t1_day2 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN airwaytype IN ('Oral ETT', 'Nasal ETT', 'Tracheostomy') THEN 1
                ELSE NULL END) AS airway  -- either invasive airway or NULL
          FROM
            `physionet-data.eicu_crd.respiratorycare`
          WHERE
            respcarestatusoffset BETWEEN 1440 AND 1440*2
          GROUP BY
            patientunitstayid-- , respcarestatusoffset
            -- order by patientunitstayid-- , respcarestatusoffset
            ),
          t2_day2 AS (
          SELECT
            DISTINCT patientunitstayid,
            1 AS ventilator
          FROM
            `physionet-data.eicu_crd.respiratorycharting` rc
          WHERE
            respchartvalue LIKE '%ventilator%'
            OR respchartvalue LIKE '%vent%'
            OR respchartvalue LIKE '%bipap%'
            OR respchartvalue LIKE '%840%'
            OR respchartvalue LIKE '%cpap%'
            OR respchartvalue LIKE '%drager%'
            OR respchartvalue LIKE 'mv%'
            OR respchartvalue LIKE '%servo%'
            OR respchartvalue LIKE '%peep%'
            AND respchartoffset BETWEEN 1440 AND 1440*2
          GROUP BY
            patientunitstayid
            -- order by patientunitstayid
            ),
          t3_day2 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN treatmentstring IN ('pulmonary|ventilation and oxygenation|mechanical ventilation',  'pulmonary|ventilation and oxygenation|tracheal suctioning',  'pulmonary|ventilation and oxygenation|ventilator weaning',  'pulmonary|ventilation and oxygenation|mechanical ventilation|assist controlled',  'pulmonary|radiologic procedures / bronchoscopy|endotracheal tube',  'pulmonary|ventilation and oxygenation|oxygen therapy (> 60%)',  'pulmonary|ventilation and oxygenation|mechanical ventilation|tidal volume 6-10 ml/kg',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume controlled',  'surgery|pulmonary therapies|mechanical ventilation',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy',  'pulmonary|ventilation and oxygenation|mechanical ventilation|synchronized intermittent',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|performed during current admission for ventilatory support',  'pulmonary|ventilation and oxygenation|ventilator weaning|active',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|ventilator weaning|slow',  'surgery|pulmonary therapies|ventilator weaning',  'surgery|pulmonary therapies|tracheal suctioning',  'pulmonary|radiologic procedures / bronchoscopy|reintubation',  'pulmonary|ventilation and oxygenation|lung recruitment maneuver',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|planned',  'surgery|pulmonary therapies|ventilator weaning|rapid',  'pulmonary|ventilation and oxygenation|prone position',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|conventional',  'pulmonary|ventilation and oxygenation|mechanical ventilation|permissive hypercapnea',  'surgery|pulmonary therapies|mechanical ventilation|synchronized intermittent',  'pulmonary|medications|neuromuscular blocking agent',  'surgery|pulmonary therapies|mechanical ventilation|assist controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume assured',  'surgery|pulmonary therapies|mechanical ventilation|tidal volume 6-10 ml/kg',  'surgery|pulmonary therapies|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|nasal mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|face mask',  'surgery|pulmonary therapies|non-invasive ventilation',  'surgery|pulmonary therapies|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation|face mask' ) THEN 1
                ELSE NULL END) AS interface   -- either ETT/NiV or NULL
          FROM
            `physionet-data.eicu_crd.treatment`
          WHERE
            treatmentoffset BETWEEN 1440 AND 1440*2
          GROUP BY
            patientunitstayid-- , treatmentoffset, interface
          ORDER BY
            patientunitstayid-- , treatmentoffset
            )
        SELECT
          pt.patientunitstayid,
          CASE
            WHEN t1_day2.airway IS NOT NULL OR t2_day2.ventilator IS NOT NULL OR t3_day2.interface IS NOT NULL THEN 1
            ELSE NULL
          END AS mechvent
        FROM
          `physionet-data.eicu_crd.patient` pt
        LEFT OUTER JOIN
          t1_day2
        ON
          t1_day2.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t2_day2
        ON
          t2_day2.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t3_day2
        ON
          t3_day2.patientunitstayid=pt.patientunitstayid
        ORDER BY
          pt.patientunitstayid )
      SELECT
        pt.patientunitstayid,
        t3_day2.sao2,
        t4_day2.pao2,
        (CASE
            WHEN t1_day2.rcfio2>20 THEN t1_day2.rcfio2
            WHEN t2_day2.ncfio2 >20 THEN t2_day2.ncfio2
            WHEN t1_day2.rcfio2=1 OR t2_day2.ncfio2=1 THEN 100
            ELSE NULL END) AS fio2,
        t5_day2.mechvent
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        t1_day2
      ON
        t1_day2.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t2_day2
      ON
        t2_day2.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t3_day2
      ON
        t3_day2.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t4_day2
      ON
        t4_day2.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t5_day2
      ON
        t5_day2.patientunitstayid=pt.patientunitstayid
        -- order by pt.patientunitstayid
        )
    SELECT
      *,
      -- coalesce(fio2,nullif(fio2,0),21) as fn, nullif(fio2,0) as nullifzero, coalesce(coalesce(nullif(fio2,0),21),fio2,21) as ifzero21 ,
      coalesce(pao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS pf,
      coalesce(sao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS sf
    FROM
      tempo1_day2
      -- order by fio2
      ),
      
-- -------------- day 3 -----------------------      
    tempo2_day3 AS (
    WITH
      tempo1_day3 AS (
      WITH
        t1_day3 AS (
        SELECT
          *
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CAST(respchartvalue AS INT64)) AS rcfio2
            -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
          FROM
            `physionet-data.eicu_crd.respiratorycharting`
          WHERE
            respchartoffset BETWEEN 1440*2
            AND 1440*3
            AND respchartvalue <> ''
            AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
          GROUP BY
            patientunitstayid ) AS tempo
        WHERE
          rcfio2 >20 -- many values are liters per minute!
        ORDER BY
          patientunitstayid ),
        t2_day3 AS (
        SELECT
          DISTINCT patientunitstayid,
          MAX(CAST(nursingchartvalue AS INT64)) AS ncfio2
        FROM
          `physionet-data.eicu_crd.nursecharting` nc
        WHERE
          LOWER(nursingchartcelltypevallabel) LIKE '%fio2%'
          AND REGEXP_CONTAINS(nursingchartvalue, '^[0-9]{0,2}$')
          AND nursingchartentryoffset BETWEEN 1440*2 AND 1440*3
        GROUP BY
          patientunitstayid ),
        t3_day3 AS (
        SELECT
          patientunitstayid,
          MIN(
            CASE
              WHEN sao2 IS NOT NULL THEN sao2
              ELSE NULL END) AS sao2
        FROM
          `physionet-data.eicu_crd.vitalperiodic`
        WHERE
          observationoffset BETWEEN 1440*2
          AND 1440*3
        GROUP BY
          patientunitstayid ),
        t4_day3 AS (
        SELECT
          patientunitstayid,
          MIN(CASE
              WHEN LOWER(labname) LIKE 'pao2%' THEN labresult
              ELSE NULL END) AS pao2
        FROM
          `physionet-data.eicu_crd.lab`
        WHERE
          labresultoffset BETWEEN 1440*2
          AND 1440*3
        GROUP BY
          patientunitstayid ),
        t5_day3 AS (
        WITH
          t1_day3 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN airwaytype IN ('Oral ETT', 'Nasal ETT', 'Tracheostomy') THEN 1
                ELSE NULL END) AS airway  -- either invasive airway or NULL
          FROM
            `physionet-data.eicu_crd.respiratorycare`
          WHERE
            respcarestatusoffset BETWEEN 1440*2
            AND 1440*3
          GROUP BY
            patientunitstayid-- , respcarestatusoffset
            -- order by patientunitstayid-- , respcarestatusoffset
            ),
          t2_day3 AS (
          SELECT
            DISTINCT patientunitstayid,
            1 AS ventilator
          FROM
            `physionet-data.eicu_crd.respiratorycharting` rc
          WHERE
            respchartvalue LIKE '%ventilator%'
            OR respchartvalue LIKE '%vent%'
            OR respchartvalue LIKE '%bipap%'
            OR respchartvalue LIKE '%840%'
            OR respchartvalue LIKE '%cpap%'
            OR respchartvalue LIKE '%drager%'
            OR respchartvalue LIKE 'mv%'
            OR respchartvalue LIKE '%servo%'
            OR respchartvalue LIKE '%peep%'
            AND respchartoffset BETWEEN 1440*2
            AND 1440*3
          GROUP BY
            patientunitstayid
            -- order by patientunitstayid
            ),
          t3_day3 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN treatmentstring IN ('pulmonary|ventilation and oxygenation|mechanical ventilation',  'pulmonary|ventilation and oxygenation|tracheal suctioning',  'pulmonary|ventilation and oxygenation|ventilator weaning',  'pulmonary|ventilation and oxygenation|mechanical ventilation|assist controlled',  'pulmonary|radiologic procedures / bronchoscopy|endotracheal tube',  'pulmonary|ventilation and oxygenation|oxygen therapy (> 60%)',  'pulmonary|ventilation and oxygenation|mechanical ventilation|tidal volume 6-10 ml/kg',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume controlled',  'surgery|pulmonary therapies|mechanical ventilation',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy',  'pulmonary|ventilation and oxygenation|mechanical ventilation|synchronized intermittent',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|performed during current admission for ventilatory support',  'pulmonary|ventilation and oxygenation|ventilator weaning|active',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|ventilator weaning|slow',  'surgery|pulmonary therapies|ventilator weaning',  'surgery|pulmonary therapies|tracheal suctioning',  'pulmonary|radiologic procedures / bronchoscopy|reintubation',  'pulmonary|ventilation and oxygenation|lung recruitment maneuver',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|planned',  'surgery|pulmonary therapies|ventilator weaning|rapid',  'pulmonary|ventilation and oxygenation|prone position',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|conventional',  'pulmonary|ventilation and oxygenation|mechanical ventilation|permissive hypercapnea',  'surgery|pulmonary therapies|mechanical ventilation|synchronized intermittent',  'pulmonary|medications|neuromuscular blocking agent',  'surgery|pulmonary therapies|mechanical ventilation|assist controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume assured',  'surgery|pulmonary therapies|mechanical ventilation|tidal volume 6-10 ml/kg',  'surgery|pulmonary therapies|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|nasal mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|face mask',  'surgery|pulmonary therapies|non-invasive ventilation',  'surgery|pulmonary therapies|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation|face mask' ) THEN 1
                ELSE NULL END) AS interface   -- either ETT/NiV or NULL
          FROM
            `physionet-data.eicu_crd.treatment`
          WHERE
            treatmentoffset BETWEEN 1440*2
            AND 1440*3
          GROUP BY
            patientunitstayid-- , treatmentoffset, interface
          ORDER BY
            patientunitstayid-- , treatmentoffset
            )
        SELECT
          pt.patientunitstayid,
          CASE
            WHEN t1_day3.airway IS NOT NULL OR t2_day3.ventilator IS NOT NULL OR t3_day3.interface IS NOT NULL THEN 1
            ELSE NULL
          END AS mechvent
        FROM
          `physionet-data.eicu_crd.patient` pt
        LEFT OUTER JOIN
          t1_day3
        ON
          t1_day3.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t2_day3
        ON
          t2_day3.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t3_day3
        ON
          t3_day3.patientunitstayid=pt.patientunitstayid
        ORDER BY
          pt.patientunitstayid )
      SELECT
        pt.patientunitstayid,
        t3_day3.sao2,
        t4_day3.pao2,
        (CASE
            WHEN t1_day3.rcfio2>20 THEN t1_day3.rcfio2
            WHEN t2_day3.ncfio2 >20 THEN t2_day3.ncfio2
            WHEN t1_day3.rcfio2=1 OR t2_day3.ncfio2=1 THEN 100
            ELSE NULL END) AS fio2,
        t5_day3.mechvent
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        t1_day3
      ON
        t1_day3.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t2_day3
      ON
        t2_day3.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t3_day3
      ON
        t3_day3.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t4_day3
      ON
        t4_day3.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t5_day3
      ON
        t5_day3.patientunitstayid=pt.patientunitstayid
        -- order by pt.patientunitstayid
        )
    SELECT
      *,
      -- coalesce(fio2,nullif(fio2,0),21) as fn, nullif(fio2,0) as nullifzero, coalesce(coalesce(nullif(fio2,0),21),fio2,21) as ifzero21 ,
      coalesce(pao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS pf,
      coalesce(sao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS sf
    FROM
      tempo1_day3
      -- order by fio2
      ),

-- -------------- day 4 ----------------------- 
    tempo2_day4 AS (
    WITH
      tempo1_day4 AS (
      WITH
        t1_day4 AS (
        SELECT
          *
        FROM (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CAST(respchartvalue AS INT64)) AS rcfio2
            -- , max(case when respchartvaluelabel = 'FiO2' then respchartvalue else null end) as fiO2
          FROM
            `physionet-data.eicu_crd.respiratorycharting`
          WHERE
            respchartoffset BETWEEN 1440*3
            AND 1440*4
            AND respchartvalue <> ''
            AND REGEXP_CONTAINS(respchartvalue, '^[0-9]{0,2}$')
          GROUP BY
            patientunitstayid ) AS tempo
        WHERE
          rcfio2 >20 -- many values are liters per minute!
        ORDER BY
          patientunitstayid ),
        t2_day4 AS (
        SELECT
          DISTINCT patientunitstayid,
          MAX(CAST(nursingchartvalue AS INT64)) AS ncfio2
        FROM
          `physionet-data.eicu_crd.nursecharting` nc
        WHERE
          LOWER(nursingchartcelltypevallabel) LIKE '%fio2%'
          AND REGEXP_CONTAINS(nursingchartvalue, '^[0-9]{0,2}$')
          AND nursingchartentryoffset BETWEEN 1440*3
          AND 1440*4
        GROUP BY
          patientunitstayid ),
        t3_day4 AS (
        SELECT
          patientunitstayid,
          MIN(
            CASE
              WHEN sao2 IS NOT NULL THEN sao2
              ELSE NULL END) AS sao2
        FROM
          `physionet-data.eicu_crd.vitalperiodic`
        WHERE
          observationoffset BETWEEN 1440*3
            AND 1440*4
        GROUP BY
          patientunitstayid ),
        t4_day4 AS (
        SELECT
          patientunitstayid,
          MIN(CASE
              WHEN LOWER(labname) LIKE 'pao2%' THEN labresult
              ELSE NULL END) AS pao2
        FROM
          `physionet-data.eicu_crd.lab`
        WHERE
          labresultoffset BETWEEN 1440*3
            AND 1440*4
        GROUP BY
          patientunitstayid ),
        t5_day4 AS (
        WITH
          t1_day4 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN airwaytype IN ('Oral ETT', 'Nasal ETT', 'Tracheostomy') THEN 1
                ELSE NULL END) AS airway  -- either invasive airway or NULL
          FROM
            `physionet-data.eicu_crd.respiratorycare`
          WHERE
            respcarestatusoffset BETWEEN -1440*3
            AND 1440*4
          GROUP BY
            patientunitstayid-- , respcarestatusoffset
            -- order by patientunitstayid-- , respcarestatusoffset
            ),
          t2_day4 AS (
          SELECT
            DISTINCT patientunitstayid,
            1 AS ventilator
          FROM
            `physionet-data.eicu_crd.respiratorycharting` rc
          WHERE
            respchartvalue LIKE '%ventilator%'
            OR respchartvalue LIKE '%vent%'
            OR respchartvalue LIKE '%bipap%'
            OR respchartvalue LIKE '%840%'
            OR respchartvalue LIKE '%cpap%'
            OR respchartvalue LIKE '%drager%'
            OR respchartvalue LIKE 'mv%'
            OR respchartvalue LIKE '%servo%'
            OR respchartvalue LIKE '%peep%'
            AND respchartoffset BETWEEN 1440*3
            AND 1440*4
          GROUP BY
            patientunitstayid
            -- order by patientunitstayid
            ),
          t3_day4 AS (
          SELECT
            DISTINCT patientunitstayid,
            MAX(CASE
                WHEN treatmentstring IN ('pulmonary|ventilation and oxygenation|mechanical ventilation',  'pulmonary|ventilation and oxygenation|tracheal suctioning',  'pulmonary|ventilation and oxygenation|ventilator weaning',  'pulmonary|ventilation and oxygenation|mechanical ventilation|assist controlled',  'pulmonary|radiologic procedures / bronchoscopy|endotracheal tube',  'pulmonary|ventilation and oxygenation|oxygen therapy (> 60%)',  'pulmonary|ventilation and oxygenation|mechanical ventilation|tidal volume 6-10 ml/kg',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume controlled',  'surgery|pulmonary therapies|mechanical ventilation',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy',  'pulmonary|ventilation and oxygenation|mechanical ventilation|synchronized intermittent',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|performed during current admission for ventilatory support',  'pulmonary|ventilation and oxygenation|ventilator weaning|active',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|ventilator weaning|slow',  'surgery|pulmonary therapies|ventilator weaning',  'surgery|pulmonary therapies|tracheal suctioning',  'pulmonary|radiologic procedures / bronchoscopy|reintubation',  'pulmonary|ventilation and oxygenation|lung recruitment maneuver',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|planned',  'surgery|pulmonary therapies|ventilator weaning|rapid',  'pulmonary|ventilation and oxygenation|prone position',  'pulmonary|surgery / incision and drainage of thorax|tracheostomy|conventional',  'pulmonary|ventilation and oxygenation|mechanical ventilation|permissive hypercapnea',  'surgery|pulmonary therapies|mechanical ventilation|synchronized intermittent',  'pulmonary|medications|neuromuscular blocking agent',  'surgery|pulmonary therapies|mechanical ventilation|assist controlled',  'pulmonary|ventilation and oxygenation|mechanical ventilation|volume assured',  'surgery|pulmonary therapies|mechanical ventilation|tidal volume 6-10 ml/kg',  'surgery|pulmonary therapies|mechanical ventilation|pressure support',  'pulmonary|ventilation and oxygenation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|non-invasive ventilation|nasal mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|face mask',  'surgery|pulmonary therapies|non-invasive ventilation',  'surgery|pulmonary therapies|non-invasive ventilation|face mask',  'pulmonary|ventilation and oxygenation|mechanical ventilation|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|non-invasive ventilation|nasal mask',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation',  'surgery|pulmonary therapies|mechanical ventilation|non-invasive ventilation|face mask' ) THEN 1
                ELSE NULL END) AS interface   -- either ETT/NiV or NULL
          FROM
            `physionet-data.eicu_crd.treatment`
          WHERE
            treatmentoffset BETWEEN 1440*3
            AND 1440*4
          GROUP BY
            patientunitstayid-- , treatmentoffset, interface
          ORDER BY
            patientunitstayid-- , treatmentoffset
            )
        SELECT
          pt.patientunitstayid,
          CASE
            WHEN t1_day4.airway IS NOT NULL OR t2_day4.ventilator IS NOT NULL OR t3_day4.interface IS NOT NULL THEN 1
            ELSE NULL
          END AS mechvent
        FROM
          `physionet-data.eicu_crd.patient` pt
        LEFT OUTER JOIN
          t1_day4
        ON
          t1_day4.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t2_day4
        ON
          t2_day4.patientunitstayid=pt.patientunitstayid
        LEFT OUTER JOIN
          t3_day4
        ON
          t3_day4.patientunitstayid=pt.patientunitstayid
        ORDER BY
          pt.patientunitstayid )
      SELECT
        pt.patientunitstayid,
        t3_day4.sao2,
        t4_day4.pao2,
        (CASE
            WHEN t1_day4.rcfio2>20 THEN t1_day4.rcfio2
            WHEN t2_day4.ncfio2 >20 THEN t2_day4.ncfio2
            WHEN t1_day4.rcfio2=1 OR t2_day4.ncfio2=1 THEN 100
            ELSE NULL END) AS fio2,
        t5_day4.mechvent
      FROM
        `physionet-data.eicu_crd.patient` pt
      LEFT OUTER JOIN
        t1_day4
      ON
        t1_day4.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t2_day4
      ON
        t2_day4.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t3_day4
      ON
        t3_day4.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t4_day4
      ON
        t4_day4.patientunitstayid=pt.patientunitstayid
      LEFT OUTER JOIN
        t5_day4
      ON
        t5_day4.patientunitstayid=pt.patientunitstayid
        -- order by pt.patientunitstayid
        )
    SELECT
      *,
      -- coalesce(fio2,nullif(fio2,0),21) as fn, nullif(fio2,0) as nullifzero, coalesce(coalesce(nullif(fio2,0),21),fio2,21) as ifzero21 ,
      coalesce(pao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS pf,
      coalesce(sao2,
        100)/coalesce(coalesce(nullif(fio2,
            0),
          21),
        fio2,
        21) AS sf
    FROM
      tempo1_day4
      -- order by fio2
      )
SELECT
    pt.patientunitstayid,
    (CASE
        WHEN tempo2_day1.pf <1 OR tempo2_day1.sf <0.67 THEN 4
        WHEN tempo2_day1.pf BETWEEN 1
      AND 2
      OR tempo2_day1.sf BETWEEN 0.67
      AND 1.41 THEN 3
        WHEN tempo2_day1.pf BETWEEN 2 AND 3 OR tempo2_day1.sf BETWEEN 1.42 AND 2.2 THEN 2
        WHEN tempo2_day1.pf BETWEEN 3
      AND 4
      OR tempo2_day1.sf BETWEEN 2.21
      AND 3.01 THEN 1
        WHEN tempo2_day1.pf > 4 OR tempo2_day1.sf> 3.01 THEN 0
        ELSE 0
      END ) AS SOFA_respi_day1,
      
      
      (CASE
        WHEN tempo2_day2.pf <1 OR tempo2_day2.sf <0.67 THEN 4
        WHEN tempo2_day2.pf BETWEEN 1
      AND 2
      OR tempo2_day2.sf BETWEEN 0.67
      AND 1.41 THEN 3
        WHEN tempo2_day2.pf BETWEEN 2 AND 3 OR tempo2_day2.sf BETWEEN 1.42 AND 2.2 THEN 2
        WHEN tempo2_day2.pf BETWEEN 3
      AND 4
      OR tempo2_day2.sf BETWEEN 2.21
      AND 3.01 THEN 1
        WHEN tempo2_day2.pf > 4 OR tempo2_day2.sf> 3.01 THEN 0
        ELSE 0
      END ) AS SOFA_respi_day2,
      
      
      (CASE
        WHEN tempo2_day3.pf <1 OR tempo2_day3.sf <0.67 THEN 4
        WHEN tempo2_day3.pf BETWEEN 1
      AND 2
      OR tempo2_day3.sf BETWEEN 0.67
      AND 1.41 THEN 3
        WHEN tempo2_day3.pf BETWEEN 2 AND 3 OR tempo2_day3.sf BETWEEN 1.42 AND 2.2 THEN 2
        WHEN tempo2_day3.pf BETWEEN 3
      AND 4
      OR tempo2_day3.sf BETWEEN 2.21
      AND 3.01 THEN 1
        WHEN tempo2_day3.pf > 4 OR tempo2_day3.sf> 3.01 THEN 0
        ELSE 0
      END ) AS SOFA_respi_day3,
  
      (CASE
        WHEN tempo2_day4.pf <1 OR tempo2_day4.sf <0.67 THEN 4
        WHEN tempo2_day4.pf BETWEEN 1
      AND 2
      OR tempo2_day4.sf BETWEEN 0.67
      AND 1.41 THEN 3
        WHEN tempo2_day4.pf BETWEEN 2 AND 3 OR tempo2_day4.sf BETWEEN 1.42 AND 2.2 THEN 2
        WHEN tempo2_day4.pf BETWEEN 3
      AND 4
      OR tempo2_day4.sf BETWEEN 2.21
      AND 3.01 THEN 1
        WHEN tempo2_day4.pf > 4 OR tempo2_day4.sf> 3.01 THEN 0
        ELSE 0
      END ) AS SOFA_respi_day4
  FROM
    `physionet-data.eicu_crd.patient` pt
    LEFT OUTER JOIN
    tempo2_day1 
    ON 
    tempo2_day1.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
    tempo2_day2 
    ON 
    tempo2_day2.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
    tempo2_day3 
    ON 
    tempo2_day3.patientunitstayid=pt.patientunitstayid
    LEFT OUTER JOIN
    tempo2_day4 
    ON 
    tempo2_day4.patientunitstayid=pt.patientunitstayid
  ORDER BY
    pt.patientunitstayid

    )
    
    
 SELECT patient.patientunitstayid,

max(sofa_cv_day1_to_day4.sofa_cv_day1 + sofa_respi_day1_to_day4.sofa_respi_day1 + sofa_renal_day1_to_day4.sofarenal_day1 + sofa_3others_day1_to_day4.sofacoag_day1 + sofa_3others_day1_to_day4.sofaliver_day1 + sofa_3others_day1_to_day4.sofacns_day1) AS sofatotal_day1,

max(sofa_cv_day1_to_day4.sofa_cv_day2 + sofa_respi_day1_to_day4.sofa_respi_day2 + sofa_renal_day1_to_day4.sofarenal_day2 + sofa_3others_day1_to_day4.sofacoag_day2 + sofa_3others_day1_to_day4.sofaliver_day2 + sofa_3others_day1_to_day4.sofacns_day2) AS sofatotal_day2,

max(sofa_cv_day1_to_day4.sofa_cv_day3 + sofa_respi_day1_to_day4.sofa_respi_day3 + sofa_renal_day1_to_day4.sofarenal_day3 + sofa_3others_day1_to_day4.sofacoag_day3 + sofa_3others_day1_to_day4.sofaliver_day3 + sofa_3others_day1_to_day4.sofacns_day3) AS sofatotal_day3,

max(sofa_cv_day1_to_day4.sofa_cv_day4 + sofa_respi_day1_to_day4.sofa_respi_day4 + sofa_renal_day1_to_day4.sofarenal_day4 + sofa_3others_day1_to_day4.sofacoag_day4 + sofa_3others_day1_to_day4.sofaliver_day4 + sofa_3others_day1_to_day4.sofacns_day4) AS sofatotal_day4

FROM
patient
INNER JOIN sofa_cv_day1_to_day4 ON  patient.patientunitstayid = sofa_cv_day1_to_day4.patientunitstayid
INNER JOIN sofa_respi_day1_to_day4  ON patient.patientunitstayid = sofa_respi_day1_to_day4.patientunitstayid
INNER JOIN sofa_renal_day1_to_day4  ON patient.patientunitstayid = sofa_renal_day1_to_day4.patientunitstayid
INNER JOIN sofa_3others_day1_to_day4  ON patient.patientunitstayid = sofa_3others_day1_to_day4.patientunitstayid
GROUP BY patient.patientunitstayid
ORDER BY patient.patientunitstayid
