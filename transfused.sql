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
where  lower(diagnosisstring) like '%hypovolemia%' or 
lower(diagnosisstring) like '%blood loss%'
),

traumalist as 
 (select distinct patientunitstayid
from patient
left join diagnosis using (patientunitstayid)
where  lower(diagnosisstring) like '%trauma%'
)

select distinct patientunitstayid, positivelist.uniquepid, unabridgedUnitLOS, unabridgedHospLOS, unitType,age, gender, ethnicity,apacheScore
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
from hypovolist)
)then 1 else 0 end hypovoflag
,case when (patientunitstayid in
(select *
from traumalist)
)then 1 else 0 end as traumaflag
,case when (patientunitstayid in
(select *
from surgerylist)
)then 1 else 0 end as gisflag
,case when lower(unitDischargeStatus) like '%expired%' then 1 else 0 end as expiremarker
from positivelist
left join patient using (patientUnitStayID) 
left join apachepatientresult using (patientUnitStayID) 
left join hgbrecord using (patientUnitStayID) 
where unabridgedUnitLOS is not null
and hgbmin is not null
