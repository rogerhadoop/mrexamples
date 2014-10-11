mrexamples
==========

There are 2 tables:

SQL> select * from emp;
     
    EMPNO ENAME       JOB        MGR   HIREDATE     SAL    COMM   DEPTNO
---------- ---------- --------- ------ ---------- ------ ----- ---------

    7369, SMITH,      CLERK,     7902, 1980-12-17,   800,     0,  20
    7499, ALLEN,      SALESMAN,  7698, 1981-02-20,  1600,   300,  30
    7521, WARD,       SALESMAN,  7698, 1981-02-22,  1250,   500,  30
    7566, JONES,      MANAGER,   7839, 1981-04-02,  2975,     0,  20
    7654, MARTIN,     SALESMAN,  7698, 1981-09-28,  1250,  1400,  30
    7698, BLAKE,      MANAGER,   7839, 1981-05-01,  2850,     0,  30
    7782, CLARK,      MANAGER,   7839, 1981-06-09,  2450,     0,  10
    7839, KING,       PRESIDENT,     , 1981-11-17,  5000,     0,  10
    7844, TURNER,     SALESMAN,  7698, 1981-09-08,  1500,     0,  30
    7900, JAMES,      CLERK,     7698, 1981-12-03,   950,     0,  30
    7902, FORD,       ANALYST,   7566, 1981-12-03,  3000,     0,  20
    7934, MILLER,     CLERK,     7782, 1982-01-23,  1300,     0,  10

    
    
SQL> select * from dept;

    DEPTNO DNAME          LOC
---------- -------------- -------------
        10 ACCOUNTING     NEW YORK
        20 RESEARCH       DALLAS
        30 SALES          CHICAGO
        40 OPERATIONS     BOSTON
        
==================================================================================================
Git: 
https://help.github.com/articles/adding-an-existing-project-to-github-using-the-command-line/
https://help.github.com/articles/changing-a-remote-s-url/
(1) Create a new repository on GitHub.
(2) In the Command prompt, change the current working directory to your local project.
(3) Initialize the local directory as a Git repository.
    $ git init

(4) Add the files in your new local repository. This stages them for the first commit.
    $ git add .
    # Adds the files in the local repository and stages them for commit

(5) Commit the files that you've staged in your local repository.
    git commit -m 'First commit'
    # Commits the tracked changes and prepares them to be pushed to a remote repository

(6) Copy remote repository URL fieldIn your GitHub repository, in the right sidebar, copy the remote repository URL.

(7) In the Command prompt, add the URL for the remote repository where your local repostory will be pushed.
    $ git remote add origin <remote repository URL>
    # Sets the new remote
    $ git remote -v
    # Verifies the new remote URL

(8) Push the changes in your local repository to GitHub.
    $ git push origin master
    # Pushes the changes in your local repository up to the remote repository you specified as the origin
==================================================================================================

Exercise:

(Part 1)
Import the data into HDFS (2 files).
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt
hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt


Create hive table:

/*  To create an external table, use this:
CREATE EXTERNAL TABLE emp2(EMPNO INT, ENAME STRING, JOB STRING, MGR INT, HIREDATE STRING, SAL INT, COMM INT default 0, DEPTNO INT) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE
              LOCATION '/user/cloudera/data/emp/';
*/

drop table emp;
drop table dept;

CREATE TABLE emp(EMPNO INT, ENAME STRING, JOB STRING, MGR INT, HIREDATE DATE, SAL INT, COMM INT, DEPTNO INT) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE;

LOAD DATA local INPATH '/home/cloudera/IdeaProjects/mrexamples/test-file/emp.txt' INTO TABLE emp;


CREATE TABLE dept(DEPTNO INT, DNAME STRING, LOC STRING) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE;

LOAD DATA local INPATH '/home/cloudera/IdeaProjects/mrexamples/test-file/dept.txt' INTO TABLE dept;


(Part 2)
Write MapReduce program to:

(1) list total salary for each dept.
select deptno, sum(sal) from emp group by deptno;
select dname, sum(sal) from emp join dept on emp.deptno=dept.deptno group by dname;


(2) list total number of employee and average salary for each dept.
select deptno, count(empno), avg(sal) from emp group by deptno;
select dname, count(empno), avg(sal) from emp join dept on emp.deptno=dept.deptno group by dname;


(3) list the first hired employee's name for each dept.
select e.deptno, e.ename, e.hiredate from (select deptno, min(hiredate) as mindate from emp group by deptno) a join emp e on e.hiredate=a.mindate AND e.deptno=a.deptno;
select e.deptno, e.ename, e.hiredate from (select deptno, min(hiredate) as mindate from emp group by deptno) a, emp e where e.hiredate=a.mindate AND e.deptno=a.deptno;


(4) list total employee salary for each city.
select  loc, sum(sal) from emp join dept on emp.deptno=dept.deptno group by loc;


(5) list employee's name and salary whose salary is higher than their manager
SELECT e.ename, e.sal FROM emp e JOIN emp e2 ON e.mgr = e2.empno WHERE e.sal > e2.sal;


(6) list employee's name and salary whose salary is higher than average salary of whole company
select b.ename, b.sal from (select avg(sal) AS avgsal from emp) a CROSS JOIN emp b where b.sal > a.avgsal order by b.sal;


(7) list employee's name and dept name whose name start with "J"
select emp.ename, dept.dname from emp JOIN dept on emp.deptno=dept.deptno and emp.ename like 'J%';

select ename, dname from emp, dept where emp.deptno=dept.deptno and ename like 'J%';


(8) list 3 employee's name and salary with highest salary
select ename, sal from emp order by sal desc limit 3;


(9) sort employee by total income (salary+comm), list name and total income.
select ename, sal+comm as income from emp order by income;


(10) If an person can only communicates with his direct manager, or people directly managed by him, or people in the
     same dept, find the number of inter-person required for communicates between any 2 person
    


01 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.TotalSalary01               hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o1
02 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.AverageSalaryAndHeadcount02 hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o2
03 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.TenureEmployeePerDept03     hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o3
05 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.HigherSalaryThanManager05   hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o5
06 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.HigherSalaryThanAverage06   hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o6
08 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.Top3SalaryEmployee08        hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o8
09 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.EmployeeListByIncome09      hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o9
10 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.MiddleCounts                hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o10

04 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.TotalSalaryPerCity04        hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o4
07 yarn jar /home/cloudera/IdeaProjects/mrexamples/target/empquery-1.0-SNAPSHOT.jar hadoop2.examples.JEmployeeAndDept07          hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/emp.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/dept.txt hdfs://quickstart.cloudera:8020/user/cloudera/data/emp/yarn-result/o7

