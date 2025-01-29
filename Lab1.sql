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
/*
CREATE OR REPLACE FUNCTION compare_even_odd RETURN VARCHAR IS
    v_even NUMBER;
    v_odd NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_even FROM MYTABLE WHERE MOD(val, 2) = 0;
    SELECT COUNT(*) INTO v_odd FROM MYTABLE WHERE MOD(val, 2) <> 0;
    IF v_even > v_odd THEN
        RETURN 'Even';
    ELSIF v_even < v_odd THEN
        RETURN 'Odd';
    ELSE
        RETURN 'Equal';
    END IF;
END;
*/
SELECT compare_even_odd() from dual;
