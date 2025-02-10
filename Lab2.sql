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
        SELECT LOWER(group_name)
        FROM groups
        GROUP BY LOWER(group_name)
        HAVING COUNT(*) > 1
    );

    IF duplicate_name_count > 0 THEN
        RAISE_APPLICATION_ERROR(-20000, 'Group name must be unique');
    END IF;

END;

-- FOREIGN KEY TRIGGERS

CREATE OR REPLACE PACKAGE global_variables AS
     is_group_delete_cascade BOOLEAN := FALSE;
END global_variables;

CREATE OR REPLACE TRIGGER delete_group_cascade
BEFORE DELETE ON groups
FOR EACH ROW
BEGIN
    global_variables.is_group_delete_cascade := TRUE;
    DELETE FROM students
    WHERE group_id = :OLD.group_id;
    global_variables.is_group_delete_cascade := FALSE;
    EXCEPTION
    WHEN OTHERS THEN
        global_variables.is_group_delete_cascade := FALSE;
        RAISE;
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


CREATE OR REPLACE TRIGGER synchronise_c_val_on_insert
BEFORE INSERT ON students
FOR EACH ROW
BEGIN
    UPDATE groups
    SET c_val = c_val + 1
    WHERE group_id = :NEW.group_id;
END;

CREATE OR REPLACE TRIGGER synchronise_c_val_on_delete
BEFORE DELETE ON students
FOR EACH ROW
BEGIN
    IF NOT global_variables.is_group_delete_cascade THEN
        UPDATE groups
        SET c_val = c_val - 1
        WHERE group_id = :OLD.group_id;
    END IF;
END;

CREATE OR REPLACE TRIGGER synchronise_c_val_on_update
BEFORE UPDATE OF group_id ON students
FOR EACH ROW
BEGIN

    IF :OLD.GROUP_ID != :NEW.GROUP_ID THEN
        UPDATE groups
        SET c_val = c_val - 1
        WHERE group_id = :OLD.group_id;

        UPDATE groups
        SET c_val = c_val + 1
        WHERE group_id = :NEW.group_id;
    END IF;

END;


CREATE TABLE students_logs (
    LOG_ID NUMBER PRIMARY KEY,
    ACTION_TYPE VARCHAR2(10),
    OLD_ID NUMBER,
    NEW_ID NUMBER,
    OLD_NAME VARCHAR2(255),
    NEW_NAME VARCHAR2(255),
    OLD_GROUP_ID NUMBER,
    NEW_GROUP_ID NUMBER,
    ACTION_TIME TIMESTAMP
);

CREATE SEQUENCE STUDENTS_LOGS_SEQ START WITH 1;

CREATE OR REPLACE TRIGGER log_student_changes
AFTER INSERT OR UPDATE OR DELETE ON students
FOR EACH ROW
BEGIN
    IF INSERTING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, NEW_ID, NEW_NAME, NEW_GROUP_ID, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'INSERT', :NEW.student_id, :NEW.student_name, :NEW.group_id, SYSTIMESTAMP);
    ELSIF UPDATING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, OLD_ID, NEW_ID, OLD_NAME, NEW_NAME, OLD_GROUP_ID, NEW_GROUP_ID, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'UPDATE', :OLD.student_id, :NEW.student_id, :OLD.student_name, :NEW.student_name, :OLD.group_id, :NEW.group_id, SYSTIMESTAMP);
    ELSIF DELETING THEN
        INSERT INTO students_logs (LOG_ID, ACTION_TYPE, OLD_ID, OLD_NAME, OLD_GROUP_ID, ACTION_TIME)
        VALUES (STUDENTS_LOGS_SEQ.NEXTVAL, 'DELETE', :OLD.student_id, :OLD.student_name, :OLD.group_id, SYSTIMESTAMP);
    END IF;
END;