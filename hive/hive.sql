# You sql follows
#Create and fill the table schools
create table schools (region string, district string, city string, id int, name string, level string, series int) row format delimited fields terminated by ',';
load data inpath '/cristian/examBigData/hive/escuelasPR.csv' into table schools;

#Create and fill the table students
create table students (region string, district string, schId int, name string, level string, sex string, id int) row format delimited fields terminated by ',';
load data inpath '/cristian/examBigData/hive/studentsPR.csv' into table students;

# 1) Find the total number of schools in each region, and city.
select count(*), region, city from schools group by region, city;

# 2) Find the total number of students per school.
select count(*), name from students group by name;

# 3) Find all the students that go to school in the city of Ponce or in Cabo Rojo.
select stu.* from students as stu, schools as sch where stu.schId = sch.id and sch.city in ('Ponce', 'Cabo Rojo');

# 4) Find the total number of students per region and city.
select count(*), sch.region, sch.city from students as stu, schools as sch where stu.schId = sch.id group by sch.region, sch.city;

# Find the number of students per region
select count(*), sch.region from students as stu, schools as sch where stu.schId = sch.id group by sch.region;

# Find the number of students per city
select count(*), sch.city from students as stu, schools as sch where stu.schId = sch.id group by sch.city;