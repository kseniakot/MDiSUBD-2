drop table groups;
drop table students;
CREATE TABLE groups (
    group_id NUMBER NOT NULL,
    group_name VARCHAR2(20) NOT NULL,
    C_VAL NUMBER NOT NULL,
    CONSTRAINT group_id_pkey PRIMARY KEY (group_id)
);

CREATE TABLE students (
    student_id NUMBER NOT NULL,
    student_name VARCHAR2(20) NOT NULL,
    gr_id NUMBER NOT NULL,
    CONSTRAINT student_id_pkey PRIMARY KEY (student_id),
    CONSTRAINT students_group_id_fkey FOREIGN KEY (gr_id) REFERENCES groups (group_id)
);

CREATE SEQUENCE GROUP_ID_SEQ START WITH 1;
CREATE SEQUENCE STUDENT_ID_SEQ START WITH 1;

CREATE OR REPLACE TRIGGER groups_id_autoincrement
BEFORE INSERT ON groups
    FOR EACH ROW
BEGIN
    :NEW.group_id := GROUP_ID_SEQ.NEXTVAL;
END;

CREATE OR REPLACE TRIGGER students_id_autoincrement
BEFORE INSERT ON students
    FOR EACH ROW
BEGIN
    :NEW.student_id := STUDENT_ID_SEQ.NEXTVAL;
END;
