 -- exce 2
 with info_base as (   
  select   lp.department_id as department
          ,jbs.job   as job
          ,EXTRACT( QUARTER FROM IF(lp.datetime = "", NULL,DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', lp.datetime)))) as QTR
          ,count(1)  as cantidad
    from `local-snow-414715.globant_raw_tables.hired_employees` lp
    LEFT JOIN `local-snow-414715.globant_raw_tables.departments` dept 
           ON dept.id = lp.department_id  
    LEFT JOIN `local-snow-414715.globant_raw_tables.jobs` jbs
          ON jbs.id = lp.job_id
    where FORMAT_DATE('%Y' ,IF(lp.datetime = "", NULL,DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', lp.datetime)))) = '2021' 
      and dept.id is not null
     group by 1,2,3
     order by 1 ,2)
     
  select (select dtp.name from `local-snow-414715.globant_raw_tables.departments` dtp where ft.department = dtp.id ) as department,
         ft.job,
         sum(case when ft.QTR = 1 then ft.cantidad else 0 end)  as Q1,
         sum(case when ft.QTR = 2 then ft.cantidad else 0 end)  as Q2,
         sum(case when ft.QTR = 3 then ft.cantidad else 0 end)  as Q3,
         sum(case when ft.QTR = 4 then ft.cantidad else 0 end)  as Q4
    from info_base ft
    group by 1,2
    order by 1,2 ;
