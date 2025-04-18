select user from dual;

DROP USER DEVELOPER CASCADE;
DROP USER PRODUCTION CASCADE;


CREATE USER DEVELOPER IDENTIFIED BY admin;
GRANT ALL PRIVILEGES TO DEVELOPER;
ALTER SESSION SET CURRENT_SCHEMA = DEVELOPER;

CREATE USER PRODUCTION IDENTIFIED BY admin;
GRANT ALL PRIVILEGES TO PRODUCTION;
ALTER SESSION SET CURRENT_SCHEMA = PRODUCTION;

CREATE TABLE DEVELOPER.TEST1 (
    id CHAR(10) PRIMARY KEY,         
    name VARCHAR2(255),             
    quantity INTEGER                 
);

CREATE TABLE PRODUCTION.TEST1 (
    id NUMBER PRIMARY KEY,           
    name CLOB,                      
    quantity BINARY_FLOAT            
);

create or REPLACE function DEVELOPER.test_func return number is
begin
    return 'hello';
end;

create or REPLACE function PRODUCTION.test_func return number is
begin
    return 'hello word!';
end;


CREATE TABLE DEVELOPER.department (
    dept_id NUMBER PRIMARY KEY,
    dept_name VARCHAR2(100) NOT NULL,
    manager_id NUMBER
);

CREATE TABLE DEVELOPER.employee (
    emp_id NUMBER PRIMARY KEY,
    emp_name VARCHAR2(100) NOT NULL,
    dept_id NUMBER
);

ALTER TABLE DEVELOPER.department ADD CONSTRAINT fk_dept_department
    FOREIGN KEY (manager_id) REFERENCES DEVELOPER.department(dept_id);














CREATE TABLE DEVELOPER.T1 (
    id NUMBER(10) PRIMARY KEY NOT NULL,
    name VARCHAR2(255),
    c1 NUMBER(20)  
);

CREATE TABLE DEVELOPER.T2 (
    id NUMBER(10) PRIMARY KEY NOT NULL,
    name VARCHAR2(255),
    c1 NUMBER(20) 
);

CREATE TABLE DEVELOPER.T3 (
    id NUMBER(10) PRIMARY KEY NOT NULL,
    name VARCHAR2(255),
    c1 NUMBER(20)    
);


ALTER TABLE DEVELOPER.T3 
ADD CONSTRAINT tab32_c1_fk FOREIGN KEY (c1) REFERENCES DEVELOPER.T2(id);

ALTER TABLE DEVELOPER.T1
ADD CONSTRAINT tab1_c1_fk FOREIGN KEY (c1) REFERENCES DEVELOPER.T3(id);


