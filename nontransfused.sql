with sq as (select distinct patientunitstayid, uniquepid
from `physionet-data.eicu_crd.patient`
left join `physionet-data.eicu_crd.diagnosis` using (patientunitstayid)
where patientUnitStayID
not in (
SELECT DISTINCT patientUnitStayID
FROM `physionet-data.eicu_crd.diagnosis`
WHERE 
(LOWER(diagnosisString) like '%hemorrhage%') 

OR
(LOWER(diagnosisString) Like '%bleed%' and not 
LOWER(diagnosisString) Like '%bleeding and red blood cell disorders%') )),

trsfsn as 
(
select distinct patientunitstayid
from `physionet-data.eicu_crd.treatment`
),

sq2 as (select patientunitstayid,uniquepid,
case when patientunitstayid in
(select *
from trsfsn) then 1 else 0 end as trsfmark
from sq
left join trsfsn using (patientunitstayid))

select *
from sq2
where trsfmark = 0
and uniquepid not in 
(
select uniquepid
from sq2
where trsfmark = 1
)
