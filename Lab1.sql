
--SELECT table_name FROM  user_tables;
--DROP TABLE MyTable;
/*
CREATE TABLE MyTable(
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    val NUMBER
);*/

/*
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO MyTable(val) VALUES(TRUNC(DBMS_RANDOM.VALUE(1, 10000)));
        END LOOP;
END;
*/
--SELECT * FROM MyTable;

CREATE OR REPLACE FUNCTION compare_even_odd RETURN VARCHAR IS
    v_even NUMBER;
    v_odd NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_even FROM MYTABLE WHERE MOD(val, 2) = 0;
    SELECT COUNT(*) INTO v_odd FROM MYTABLE WHERE MOD(val, 2) <> 0;
    case
    when v_even > v_odd then
        return 'Even';
    when v_even < v_odd then
        return 'Odd';
    else
        return 'Equal';
    end case;
END;

SELECT compare_even_odd() from dual;

CREATE OR REPLACE FUNCTION generate_insert_command_dynamic (input_id IN NUMBER, input_val IN NUMBER DEFAULT NULL)
RETURN VARCHAR2 IS
    insert_command VARCHAR2(400);
    existing_id NUMBER;
    duplicate_id EXCEPTION;
BEGIN

    BEGIN
        SELECT id into existing_id
        FROM MyTable
        WHERE id = input_id;
        RAISE duplicate_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        when duplicate_id THEN
            DBMS_OUTPUT.PUT_LINE('ID already exists');
            RETURN 'ID already exists';
    END;
    insert_command := 'INSERT INTO MyTable(id, val) VALUES('||input_id||','|| input_val || ');';
    DBMS_OUTPUT.PUT_LINE(insert_command);
    RETURN insert_command;
END generate_insert_command_dynamic;
/

select generate_insert_command_dynamic(1, 2025) from dual;

CREATE OR REPLACE PROCEDURE INSERT_INTO_TABLE_DYNAMIC (input_value IN NUMBER) IS
    insert_command VARCHAR2(400);
BEGIN
    insert_command := 'INSERT INTO MyTable(val) VALUES(:1)';
    EXECUTE IMMEDIATE insert_command USING input_value;

    DBMS_OUTPUT.PUT_LINE('Запись успешно добавлена с значением: ' || input_value);
END INSERT_INTO_TABLE_DYNAMIC;
/

/*begin
INSERT_INTO_TABLE_DYNAMIC(2025);
end;*/
/
select val from MyTable
order by id desc;

CREATE OR REPLACE PROCEDURE DELETE_FROM_TABLE_DYNAMIC (delete_id IN NUMBER) IS
    delete_command VARCHAR2(400);
BEGIN
    delete_command := 'DELETE FROM MyTable WHERE ID=:1';
    EXECUTE IMMEDIATE delete_command USING delete_id;
    DBMS_OUTPUT.PUT_LINE('Запись c ID=' || delete_id || ' успешно удалена');
END DELETE_FROM_TABLE_DYNAMIC;

begin
DELETE_FROM_TABLE_DYNAMIC(40001);
end;
/

select ID, val from MyTable
order by id desc;