WITH
patient as (
select *
from `physionet-data.eicu_crd.patient`
),

treatment as (
select *
from `physionet-data.eicu_crd.treatment`
),

diagnosis as (
select *
from `physionet-data.eicu_crd.diagnosis`
),

lab as (
select *
from `physionet-data.eicu_crd.lab`
),

apachepatientresult as (
select *
from `physionet-data.eicu_crd.apachepatientresult`
),

physicalexam as (select*
from `physionet-data.eicu_crd.physicalexam`
),


vitalperiodic as (select*
from `physionet-data.eicu_crd.vitalperiodic`
),

vitalaperiodic as (select*
from `physionet-data.eicu_crd.vitalaperiodic`
),

infusiondrug as (select*
from `physionet-data.eicu_crd.infusiondrug`
),

intakeoutput as (select*
from `physionet-data.eicu_crd.intakeoutput`
),

respiratorycharting as (select*
from `physionet-data.eicu_crd.respiratorycharting`
),

nursecharting as (select*
from `physionet-data.eicu_crd.nursecharting`
),

respiratorycare as (select*
from `physionet-data.eicu_crd.respiratorycare`
),



Reliable_ICUs as(
SELECT *
FROM patient
WHERE
(wardID IN(259,261,267,273,285,286,307,317,324,337,338,345,347,362,369,376,377,384,391,394,
408,413,417,425,428,429,430,431,434,445,464,451,487,489,491,495,498,504,506,512,513,594,601,
602,607,608,609,611,613,619,622,628,829,831,809,814,840,841,991,876,962,953,966,984,1017,1021,
1020,1030,1035,1026,1027,1029,1037,1032,1039,1041,1048,1053,1043,1087,1074)
AND hospitaldischargeyear =2016)
OR
(wardID IN(261,286,307,273,290,285,259,267,384,347,394,317,362,369,337,402,345,413,408,335,377,
417,391,376,427,428,425,431,430,429,445,464,434,451,491,498,489,506,601,607,609,608,602,619,628,
622,611,613,809,829,772,831,822,814,841,840,876,983,991,962,966,953,968,1020,1017,1021,1030,1032,
1027,1035,1037,1026,1025,1048,1053,1043,1087,1074)
AND hospitaldischargeyear =2015)
OR
(wardID IN(261,286,307,290,256,285,273,259,267,347,384,317,394,362,369,402,337,413,408,345,335,377,
386,364,417,376,391,425,428,431,430,445,451,434,464,489,609,607,601,608,602,619,622,822,829,809,804,
766,814,888,841,876,953,996,1020,1017,1021,1026,1039,1027,1029,1037,1032,1035,1025,1053,1043,1087,1074)
AND hospitaldischargeyear =2014)
OR
(wardID IN(384,347,317,394,362,369,402,345,337,413,408,386,391,376,698,809,814,888,841,876,1087,1074)
AND hospitaldischargeyear =2013)
OR
(wardID IN(809,831,888,841,876)
AND hospitaldischargeyear =2012)

ORDER BY hospitalID),


sq as (select distinct patientunitstayid, uniquepid
from Reliable_ICUs
left join diagnosis using (patientunitstayid)
where patientUnitStayID
not in (
SELECT DISTINCT patientUnitStayID
FROM diagnosis
WHERE
(LOWER(diagnosisString) like '%hemorrhage%') 
OR
(LOWER(diagnosisString) like '%blood loss%') 
OR
(LOWER(diagnosisString) Like '%bleed%' and not 
LOWER(diagnosisString) Like '%bleeding and red blood cell disorders%') )),

trsfsn as 
(
select distinct patientunitstayid, treatmentstring, treatmentoffset,
ROW_NUMBER() OVER (PARTITION BY patientunitstayid ORDER BY treatmentoffset  ASC) AS rntr
from treatment
where (lower(treatmentstring) like '%transfusion%' or lower(treatmentstring) like '%packed red blood cell%')
),

sq2 as (select patientunitstayid,uniquepid,
case when patientunitstayid in
(select patientunitstayid
from trsfsn) then 1 else 0 end as trsfmark
from sq
left join trsfsn using (patientunitstayid)),

negativegroup as (select *,
ROW_NUMBER() OVER (PARTITION BY uniquepid ORDER BY patientunitstayid  ASC) AS rn
from sq2
where trsfmark = 0
and uniquepid not in 
(
select uniquepid
from sq2
where trsfmark = 1
)),

negativelist as (select *
from negativegroup
where rn = 1),

hgbrecord as (select patientunitstayid
,MIN(CASE WHEN (labresultoffset between 0 AND unitDischargeOffset) AND labresult IS NOT NULL THEN labresult END) AS hgbmin
from patient
left join lab using (patientunitstayid)
where lower(labname) like '%hgb%'
group by patientunitstayid),

septiclist as (select distinct patientunitstayid
from diagnosis
where lower(diagnosisstring) like '%sepsis%' or lower(diagnosisstring) like '%septic%'),

ischemialist as (
select distinct patientunitstayid
from diagnosis
where (lower(diagnosisstring) like '%ischemia%' or lower(diagnosisstring) like '%ischemic%')
and (lower(diagnosisstring) not like '%chronic%')
),

activeischemialist as
(select distinct patientunitstayid
from diagnosis
where lower(diagnosisstring) like '%myocardial ischemia%' or lower(diagnosisstring) like '%myocardial infarction%' or  lower(diagnosisstring) like '%acute coronary syndrome%'
),

cirrhosislist as 
 (select distinct patientunitstayid
from diagnosis
where  lower(diagnosisstring) like '%cirrhosis%' 
),

chflist as 
 (select distinct patientunitstayid
from diagnosis
where  lower(diagnosisstring) like '%congestive heart failure%'
),

ckdlist as 
 (select distinct patientunitstayid
from diagnosis
where  lower(diagnosisstring) like '%chronic kidney disease%'
),


surgerylist as 
 (select distinct patientunitstayid
from diagnosis
where  diagnosisstring like '%surgery%' 
),

hypovolist as 
 (select distinct patientunitstayid
from diagnosis
where  lower(diagnosisstring) like '%hypovolemia%' or 
lower(diagnosisstring) like '%blood loss%'
),

traumalist as 
 (select distinct patientunitstayid
from diagnosis
where  lower(diagnosisstring) like '%trauma%'
),



tr as
(
  select
    patientunitstayid
   , treatmentoffset as chartoffset
   , max(case when treatmentstring in
   (
     'toxicology|drug overdose|vasopressors|vasopressin' --                                                                   |    23
   , 'toxicology|drug overdose|vasopressors|phenylephrine (Neosynephrine)' --                                                 |    21
   , 'toxicology|drug overdose|vasopressors|norepinephrine > 0.1 micrograms/kg/min' --                                        |    62
   , 'toxicology|drug overdose|vasopressors|norepinephrine <= 0.1 micrograms/kg/min' --                                       |    29
   , 'toxicology|drug overdose|vasopressors|epinephrine > 0.1 micrograms/kg/min' --                                           |     6
   , 'toxicology|drug overdose|vasopressors|epinephrine <= 0.1 micrograms/kg/min' --                                          |     2
   , 'toxicology|drug overdose|vasopressors|dopamine 5-15 micrograms/kg/min' --                                               |     7
   , 'toxicology|drug overdose|vasopressors|dopamine >15 micrograms/kg/min' --                                                |     3
   , 'toxicology|drug overdose|vasopressors' --                                                                               |    30
   , 'surgery|cardiac therapies|vasopressors|vasopressin' --                                                                  |   356
   , 'surgery|cardiac therapies|vasopressors|phenylephrine (Neosynephrine)' --                                                |  1000
   , 'surgery|cardiac therapies|vasopressors|norepinephrine > 0.1 micrograms/kg/min' --                                       |   390
   , 'surgery|cardiac therapies|vasopressors|norepinephrine <= 0.1 micrograms/kg/min' --                                      |   347
   , 'surgery|cardiac therapies|vasopressors|epinephrine > 0.1 micrograms/kg/min' --                                          |   117
   , 'surgery|cardiac therapies|vasopressors|epinephrine <= 0.1 micrograms/kg/min' --                                         |   178
   , 'surgery|cardiac therapies|vasopressors|dopamine  5-15 micrograms/kg/min' --                                             |   274
   , 'surgery|cardiac therapies|vasopressors|dopamine >15 micrograms/kg/min' --                                               |    23
   , 'surgery|cardiac therapies|vasopressors' --                                                                              |   596
   , 'renal|electrolyte correction|treatment of hypernatremia|vasopressin' --                                                 |     7
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|phenylephrine (Neosynephrine)' --           |   321
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|norepinephrine > 0.1 micrograms/kg/min' --  |   348
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|norepinephrine <= 0.1 micrograms/kg/min' -- |   374
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|epinephrine > 0.1 micrograms/kg/min' --     |    21
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|epinephrine <= 0.1 micrograms/kg/min' --    |   199
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|dopamine 5-15 micrograms/kg/min' --         |   277
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors|dopamine > 15 micrograms/kg/min' --         |    20
   , 'neurologic|therapy for controlling cerebral perfusion pressure|vasopressors' --                                         |   172
   , 'gastrointestinal|medications|hormonal therapy (for varices)|vasopressin' --                                             |   964
   , 'cardiovascular|shock|vasopressors|vasopressin' --                                                                       | 11082
   , 'cardiovascular|shock|vasopressors|phenylephrine (Neosynephrine)' --                                                     | 13189
   , 'cardiovascular|shock|vasopressors|norepinephrine > 0.1 micrograms/kg/min' --                                            | 24174
   , 'cardiovascular|shock|vasopressors|norepinephrine <= 0.1 micrograms/kg/min' --                                           | 17467
   , 'cardiovascular|shock|vasopressors|epinephrine > 0.1 micrograms/kg/min' --                                               |  2410
   , 'cardiovascular|shock|vasopressors|epinephrine <= 0.1 micrograms/kg/min' --                                              |  2384
   , 'cardiovascular|shock|vasopressors|dopamine  5-15 micrograms/kg/min' --                                                  |  4822
   , 'cardiovascular|shock|vasopressors|dopamine >15 micrograms/kg/min' --                                                    |  1102
   , 'cardiovascular|shock|vasopressors' --                                                                                   |  9335
   , 'toxicology|drug overdose|agent specific therapy|beta blockers overdose|dopamine' --                             |    66
   , 'cardiovascular|ventricular dysfunction|inotropic agent|norepinephrine > 0.1 micrograms/kg/min' --                       |   537
   , 'cardiovascular|ventricular dysfunction|inotropic agent|norepinephrine <= 0.1 micrograms/kg/min' --                      |   411
   , 'cardiovascular|ventricular dysfunction|inotropic agent|epinephrine > 0.1 micrograms/kg/min' --                          |   274
   , 'cardiovascular|ventricular dysfunction|inotropic agent|epinephrine <= 0.1 micrograms/kg/min' --                         |   456
   , 'cardiovascular|shock|inotropic agent|norepinephrine > 0.1 micrograms/kg/min' --                                         |  1940
   , 'cardiovascular|shock|inotropic agent|norepinephrine <= 0.1 micrograms/kg/min' --                                        |  1262
   , 'cardiovascular|shock|inotropic agent|epinephrine > 0.1 micrograms/kg/min' --                                            |   477
   , 'cardiovascular|shock|inotropic agent|epinephrine <= 0.1 micrograms/kg/min' --                                           |   505
   , 'cardiovascular|shock|inotropic agent|dopamine <= 5 micrograms/kg/min' --                                        |  1103
   , 'cardiovascular|shock|inotropic agent|dopamine  5-15 micrograms/kg/min' --                                       |  1156
   , 'cardiovascular|shock|inotropic agent|dopamine >15 micrograms/kg/min' --                                         |   144
   , 'surgery|cardiac therapies|inotropic agent|dopamine <= 5 micrograms/kg/min' --                                   |   171
   , 'surgery|cardiac therapies|inotropic agent|dopamine  5-15 micrograms/kg/min' --                                  |    93
   , 'surgery|cardiac therapies|inotropic agent|dopamine >15 micrograms/kg/min' --                                    |     3
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|norepinephrine > 0.1 micrograms/kg/min' --              |   688
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|norepinephrine <= 0.1 micrograms/kg/min' --             |   670
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|epinephrine > 0.1 micrograms/kg/min' --                 |   381
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|epinephrine <= 0.1 micrograms/kg/min' --                |   357
   , 'cardiovascular|ventricular dysfunction|inotropic agent|dopamine <= 5 micrograms/kg/min' --                      |   886
   , 'cardiovascular|ventricular dysfunction|inotropic agent|dopamine  5-15 micrograms/kg/min' --                     |   649
   , 'cardiovascular|ventricular dysfunction|inotropic agent|dopamine >15 micrograms/kg/min' --                       |    86
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|dopamine <= 5 micrograms/kg/min' --             |   346
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|dopamine  5-15 micrograms/kg/min' --            |   520
   , 'cardiovascular|myocardial ischemia / infarction|inotropic agent|dopamine >15 micrograms/kg/min' --              |    54
  ) then 1 else 0 end) as vasopressor
  from treatment
  group by patientunitstayid, treatmentoffset
),
 
 sofalist as (
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
      physicalexam pe
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
      patient pt
    LEFT OUTER JOIN
      lab lb
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
      physicalexam pe
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
      patient pt
    LEFT OUTER JOIN
      lab lb
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
      physicalexam pe
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
      patient pt
    LEFT OUTER JOIN
      lab lb
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
      physicalexam pe
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
      patient pt
    LEFT OUTER JOIN
      lab lb
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
    patient pt
    
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
        vitalaperiodic
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
        vitalperiodic
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
      patient pt
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
      infusiondrug id
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
      infusiondrug id
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
      infusiondrug id
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
        vitalaperiodic
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
        vitalperiodic
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
      patient pt
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
      infusiondrug id
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
      infusiondrug id
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
      infusiondrug id
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
        vitalaperiodic
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
        vitalperiodic
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
      patient pt
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
      infusiondrug id
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
      infusiondrug id
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
      infusiondrug id
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
        vitalaperiodic
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
        vitalperiodic
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
      patient pt
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
      infusiondrug id
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
      infusiondrug id
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
      infusiondrug id
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
    patient pt
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
      patient pt
    LEFT OUTER JOIN
      lab lb
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
          intakeoutput
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
      patient pt
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
        patient pt
      LEFT OUTER JOIN
        lab lb
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
            intakeoutput
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
        patient pt
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
        patient pt
      LEFT OUTER JOIN
        lab lb
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
            intakeoutput
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
        patient pt
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
        patient pt
      LEFT OUTER JOIN
        lab lb
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
            intakeoutput
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
        patient pt
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
    patient pt
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
            respiratorycharting
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
          nursecharting nc
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
          vitalperiodic
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
          lab
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
            respiratorycare
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
            respiratorycharting rc
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
            treatment
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
          patient pt
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
        patient pt
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
            respiratorycharting
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
          nursecharting nc
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
          vitalperiodic
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
          lab
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
            respiratorycare
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
            respiratorycharting rc
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
            treatment
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
          patient pt
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
        patient pt
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
            respiratorycharting
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
          nursecharting nc
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
          vitalperiodic
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
          lab
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
            respiratorycare
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
            respiratorycharting rc
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
            treatment
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
          patient pt
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
        patient pt
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
            respiratorycharting
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
          nursecharting nc
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
          vitalperiodic
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
          lab
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
            respiratorycare
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
            respiratorycharting rc
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
            treatment
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
          patient pt
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
        patient pt
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
    patient pt
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

 )
 
 
 

select distinct patientunitstayid, negativelist.uniquepid, unabridgedUnitLOS, unabridgedHospLOS, unitType
, case when (age like '%> 89%' )then '89' else age end as age
, case when (lower(gender) not like '%male%') then null else gender end as gender 
, ethnicity,apacheScore
,unitDischargeStatus, hgbmin
,case when unabridgedActualVentdays is null then 0 else 1 end as ventmarker
,case when (patientunitstayid in
(select *
from septiclist)
)then 1 else 0 end as septicflag
,case when (patientunitstayid in
(select *
from ischemialist)
)then 1 else 0 end as anyacuteischemiccondition
,case when (patientunitstayid in
(select *
from activeischemialist)
)then 1 else 0 end as activecardiacischemia
,case when (patientunitstayid in
(select *
from cirrhosislist)
)then 1 else 0 end as cirrhosisflag
,case when (patientunitstayid in
(select *
from chflist)
)then 1 else 0 end as chfflag
,case when (patientunitstayid in
(select *
from ckdlist)
)then 1 else 0 end as ckdflag
,case when (patientunitstayid in
(select *
from hypovolist)
)then 1 else 0 end hypovoflag
,case when (patientunitstayid in
(select *
from traumalist)
)then 1 else 0 end as traumaflag
,case when (patientunitstayid in
(select *
from surgerylist)
)then 1 else 0 end as surgeryflag
,case when (patientunitstayid in(
select patientunitstayid
from tr
where vasopressor = 1) )then 1 else 0 end as vasopressor
,case when lower(unitDischargeStatus) like '%expired%' then 1 else 0 end as expiremarker
,sofatotal_day1
,sofatotal_day2
,sofatotal_day3
,sofatotal_day4
from negativelist
left join patient using (patientUnitStayID) 
left join apachepatientresult using (patientUnitStayID) 
left join hgbrecord using (patientUnitStayID) 
left join sofalist using (patientunitstayid)
where unabridgedUnitLOS is not null
and hgbmin is not null
and gender is not null
