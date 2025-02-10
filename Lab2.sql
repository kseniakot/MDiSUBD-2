drop table groups;
drop table students;

CREATE TABLE groups (
    group_id NUMBER NOT NULL,
    group_name VARCHAR2(20) NOT NULL,
    C_VAL NUMBER NOT NULL
);

CREATE TABLE students (
    student_id NUMBER NOT NULL,
    student_name VARCHAR2(20) NOT NULL,
    group_id NUMBER NOT NULL
);

CREATE SEQUENCE GROUP_ID_SEQ START WITH 1;
CREATE SEQUENCE STUDENT_ID_SEQ START WITH 1;

-- AUTOINCREMENT TRIGGERS

CREATE OR REPLACE TRIGGER groups_id_autoincrement
BEFORE INSERT ON groups
    FOR EACH ROW
BEGIN
    IF :NEW.group_id IS NULL THEN
        :NEW.group_id := GROUP_ID_SEQ.NEXTVAL;
    END IF;
END;

CREATE OR REPLACE TRIGGER students_id_autoincrement
BEFORE INSERT ON students
    FOR EACH ROW
BEGIN
    IF :NEW.student_id IS NULL THEN
        :NEW.student_id := STUDENT_ID_SEQ.NEXTVAL;
    END IF;
END;

-- UNIQUE IDs TRIGGERS

CREATE OR REPLACE TRIGGER groups_id_unique
AFTER INSERT OR UPDATE ON groups
DECLARE duplicate_id_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO duplicate_id_count
    FROM (
        SELECT group_id
        FROM groups
        GROUP BY group_id
        HAVING COUNT(*) > 1
    );

    IF duplicate_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Group ID must be unique');
    END IF;

END;

CREATE OR REPLACE TRIGGER students_id_unique
AFTER INSERT OR UPDATE ON students
DECLARE duplicate_id_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO duplicate_id_count
    FROM (
        SELECT student_id
        FROM students
        GROUP BY student_id
        HAVING COUNT(*) > 1
    );

    IF duplicate_id_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Student ID must be unique');
    END IF;

END;

-- UNIQUE GROUP NAMES TRIGGER
CREATE OR REPLACE TRIGGER groups_name_unique
AFTER INSERT OR UPDATE ON groups
DECLARE duplicate_name_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO duplicate_name_count
    FROM (
        SELECT group_name
        FROM groups
        GROUP BY group_name
        HAVING COUNT(*) > 1
    );

    IF duplicate_name_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Group name must be unique');
    END IF;

END;

