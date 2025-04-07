DECLARE
    v_json CLOB := '{
        "queryType": "SELECT",
        "columns": ["id", "name", "age"],
        "tables": ["test_table"]
    }';
    v_cursor orm_processor.ref_cursor;
    v_id NUMBER;
    v_name VARCHAR2(50);
    v_age NUMBER;
BEGIN
    v_cursor := orm_processor.execute_select_query(v_json);
    
    LOOP
        FETCH v_cursor INTO v_id, v_name, v_age;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || ', Age: ' || v_age);
    END LOOP;
    
    CLOSE v_cursor;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
        IF v_cursor%ISOPEN THEN
            CLOSE v_cursor;
        END IF;
END;
/