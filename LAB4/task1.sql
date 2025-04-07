CREATE OR REPLACE PACKAGE BODY orm_processor AS
    FUNCTION execute_select_query(p_json CLOB) RETURN ref_cursor IS
        v_cursor ref_cursor;
        v_sql VARCHAR2(4000);
        v_query_type VARCHAR2(10);
        v_columns VARCHAR2(4000);
        v_tables VARCHAR2(4000);
    BEGIN
        -- Check query type
        v_query_type := JSON_VALUE(p_json, '$.queryType');
        
        IF v_query_type != 'SELECT' THEN
            RAISE_APPLICATION_ERROR(-20001, 'Only SELECT queries are supported');
        END IF;
        
        -- Build columns list
        SELECT LISTAGG(column_value, ', ')
        INTO v_columns
        FROM JSON_TABLE(p_json, '$.columns[*]' COLUMNS (column_value VARCHAR2(100) PATH '$'));
        
        -- Build tables list
        SELECT LISTAGG(table_value, ', ')
        INTO v_tables
        FROM JSON_TABLE(p_json, '$.tables[*]' COLUMNS (table_value VARCHAR2(100) PATH '$'));
        
        -- Construct full SQL
        v_sql := 'SELECT ' || v_columns || ' FROM ' || v_tables;
        
        -- Debug output
        DBMS_OUTPUT.PUT_LINE('Generated SQL: ' || v_sql);
        
        -- Execute and return cursor
        OPEN v_cursor FOR v_sql;
        RETURN v_cursor;
        
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error in execute_select_query: ' || SQLERRM);
            RAISE;
    END execute_select_query;
END orm_processor;
/