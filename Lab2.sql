drop table groups;
drop table students;

CREATE TABLE groups (
    group_id NUMBER NOT NULL,
    group_name VARCHAR2(20) NOT NULL,
    C_VAL NUMBER DEFAULT 0 NOT NULL
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

-- FOREIGN KEY TRIGGERS
CREATE OR REPLACE TRIGGER delete_group_cascade
BEFORE DELETE ON groups
FOR EACH ROW
BEGIN
    DELETE FROM students
    WHERE group_id = :OLD.group_id;
END;

CREATE OR REPLACE TRIGGER check_group_exists
BEFORE INSERT OR UPDATE ON students
FOR EACH ROW
DECLARE group_exists NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO group_exists
    FROM groups
    WHERE group_id = :NEW.group_id;

    IF group_exists = 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Group with ID ' || :NEW.group_id || ' does not exist');
    END IF;
END;

CREATE OR REPLACE TRIGGER prevent_group_id_update
BEFORE UPDATE OF group_id ON groups
FOR EACH ROW
    DECLARE students_exist NUMBER;
BEGIN
    SELECT count(*)
    INTO students_exist
    FROM students
    WHERE group_id = :OLD.group_id;
    IF students_exist > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'This group has students. Group ID cannot be updated');
    END IF;
END;
