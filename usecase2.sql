-- exect 1 
with employ_dept_avg as (
SELECT  lp.department_id,
        COUNT(1) AS cant_employ,
        AVG(COUNT(1)) OVER () AS media_cant_employ
FROM `local-snow-414715.globant_raw_tables.hired_employees` lp
WHERE FORMAT_DATE('%Y', IF(lp.datetime = "", NULL, DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', lp.datetime)))) = '2021'
GROUP BY lp.department_id )
select mt.department_id as id,
       lpt.name         as department,
       mt.cant_employ   as hired
  from employ_dept_avg  mt
  left join `local-snow-414715.globant_raw_tables.departments` lpt
      on mt.department_id = lpt.id
where lpt.name is not null
  and mt.cant_employ >= mt.media_cant_employ
  order by 3 desc;