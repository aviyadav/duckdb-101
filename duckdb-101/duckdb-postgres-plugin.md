## Duckdb - How to connect and use postgresql

```
INSTALL postgres;
LOAD postgres;

ATTACH 'dbname=demodb user=demouser password=password host=127.0.0.1' AS pg (TYPE postgres, READ_ONLY);


select count(*) from pg.employee_information ei ;

select * from pg.employee_information ei order by id desc;

select first_name, count(first_name) from pg.employee_information group by first_name order by 2 desc;
```
