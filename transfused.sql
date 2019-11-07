WITH --create tables to make it easier copy and paste to jupyter note
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

hospital as(
select *
from `physionet-data.eicu_crd.hospital`
),

--pickup reliable ICUs
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

-- join diagnosis and exclude bleeding patients
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

positivegroup as (select *,
ROW_NUMBER() OVER (PARTITION BY uniquepid ORDER BY patientunitstayid  ASC) AS rn
from sq2
where trsfmark = 1),

positivelist as (select *
from positivegroup
where rn = 1),

hgbrecord as (select patientunitstayid
,MIN(CASE WHEN (labresultoffset between -12*60+treatmentoffset AND treatmentoffset) AND labresult IS NOT NULL THEN labresult END) AS hgbmin
from trsfsn
left join lab using (patientunitstayid)
where lower(labname) like '%hgb%'
and rntr=1
group by patientunitstayid),

septiclist as (select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
where lower(diagnosisstring) like '%sepsis%' or lower(diagnosisstring) like '%septic%'),

ischemialist as (
select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
where (lower(diagnosisstring) like '%ischemia%' or lower(diagnosisstring) like '%ischemic%')
and (lower(diagnosisstring) not like '%chronic%')
),

activeischemialist as
(select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
where lower(diagnosisstring) like '%myocardial ischemia%' or lower(diagnosisstring) like '%myocardial infarction%' or  lower(diagnosisstring) like '%acute coronary syndrome%'
),

cirrhosislist as 
 (select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
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
from patient
left join diagnosis using (patientunitstayid)
where  diagnosisstring like '%surgery%' 
),

hypovolist as 
 (select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
where  lower(diagnosisstring) like '%hypovolemia%'
),

traumalist as 
 (select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
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
)

select distinct patientunitstayid, positivelist.uniquepid, unabridgedUnitLOS, unabridgedHospLOS, unitType
, case when (age like '%> 89%' )then '89' else age end as age
, case when hospitalid in
(
select hospitalid
from hospital
where teachingstatus is true
)then 1 else 0 end as teachingflag
, case when (lower(gender) not like '%male%') then null else gender end as gender 
,ethnicity,apacheScore
,hgbmin
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
from positivelist
left join patient using (patientUnitStayID) 
left join apachepatientresult using (patientUnitStayID) 
left join hgbrecord using (patientUnitStayID) 
where unabridgedUnitLOS is not null
and hgbmin is not null
and gender is not null
