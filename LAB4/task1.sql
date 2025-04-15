CREATE OR REPLACE PACKAGE orm_processor AS
    TYPE ref_cursor IS REF CURSOR;
    FUNCTION execute_select_query(p_json CLOB) RETURN ref_cursor;
END orm_processor;


CREATE OR REPLACE PACKAGE BODY orm_processor AS
    FUNCTION execute_select_query(p_json CLOB) RETURN ref_cursor IS
        v_cursor ref_cursor;
        v_sql VARCHAR2(4000);
        v_query_type VARCHAR2(10);
        v_columns VARCHAR2(4000);
        v_tables VARCHAR2(4000);
        v_where VARCHAR2(4000):='';
    BEGIN
        -- Check query type
        v_query_type := JSON_VALUE(p_json, '$.queryType');
        
        IF v_query_type != 'SELECT' THEN
            RAISE_APPLICATION_ERROR(-20001, 'Only SELECT queries are supported');
        END IF;
        
        BEGIN
            -- Build columns list
            SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN '*'  -- Default if no columns specified
                    ELSE LISTAGG(column_value, ', ') 
                END
            INTO v_columns
            FROM JSON_TABLE(p_json, '$.columns[*]' 
                        COLUMNS (column_value VARCHAR2(100) PATH '$'))
            WHERE column_value IS NOT NULL;
            
            -- Additional check for empty result (invalid JSON path)
            IF v_columns IS NULL THEN
                RAISE_APPLICATION_ERROR(-20002, 'Invalid or missing columns specification');
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20002, 'Invalid columns specification: ' || SQLERRM);
        END;
    
        
        BEGIN
            -- Build tables list
            SELECT LISTAGG(table_value, ', ')
            INTO v_tables
            FROM JSON_TABLE(p_json, '$.tables[*]' COLUMNS (table_value VARCHAR2(100) PATH '$'));
        EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20003, 'Invalid tables specification');
        END;

       BEGIN
   
            IF JSON_EXISTS(p_json, '$.where.conditions') THEN
                
                SELECT 'WHERE ' || 
                    LISTAGG(
                        CASE 
                            WHEN condition_num = 1 THEN '' 
                            ELSE ' ' || NVL(logic, 'AND') || ' ' 
                        END || 
                        column_name || ' ' || operator || ' ' || 
                        CASE 
                            WHEN type = 'number' THEN value  -- Без кавычек
                            WHEN type = 'string' THEN '''' || REPLACE(value, '''', '''''') || ''''
                            WHEN type = 'boolean' THEN value  -- true/false без кавычек
                            WHEN type = 'date' THEN 'TO_DATE(''' || value || ''', ''YYYY-MM-DD'')'
                            ELSE '''' || REPLACE(value, '''', '''''') || ''''  -- По умолчанию как строка
                        END,
                        ''
                    ) WITHIN GROUP (ORDER BY condition_num)
                INTO v_where
                FROM JSON_TABLE(
                    p_json, '$.where.conditions[*]' 
                    COLUMNS (
                        condition_num FOR ORDINALITY,
                        logic VARCHAR2(10) PATH '$.logic',
                        column_name VARCHAR2(100) PATH '$.column',
                        operator VARCHAR2(10) PATH '$.operator',
                        value VARCHAR2(4000) PATH '$.value',
                        type VARCHAR2(10) PATH '$.type'
                    )
                );
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE_APPLICATION_ERROR(-20005, 'Invalid where clause: ' || SQLERRM);
    END;
        -- Construct full SQL
        v_sql := 'SELECT ' || v_columns || ' FROM ' || v_tables || ' ' || v_where;
        
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