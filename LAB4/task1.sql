CREATE OR REPLACE PACKAGE SQL_GENERATOR_PKG AS
  -- Main function to handle JSON input and return cursor with query results
  FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR;
  
  -- Helper function to process subqueries
  FUNCTION process_subquery(p_subquery CLOB) RETURN VARCHAR2;
  
  -- Helper function to handle data type conversions
  FUNCTION format_value(p_value VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL) RETURN VARCHAR2;
  
  -- Helper function to process a single query part (for main queries and UNION parts)
  FUNCTION process_query_part(p_query_part CLOB) RETURN VARCHAR2;
  
  -- Functions for DML operations
  FUNCTION json_insert_handler(p_json CLOB) RETURN NUMBER;
  FUNCTION json_update_handler(p_json CLOB) RETURN NUMBER;
  FUNCTION json_delete_handler(p_json CLOB) RETURN NUMBER;
  
  -- Main entry point for any SQL operation (DQL or DML)
  FUNCTION execute_sql(p_json CLOB) RETURN SYS_REFCURSOR;
  PROCEDURE execute_dml(p_json CLOB, p_rows_affected OUT NUMBER);
END SQL_GENERATOR_PKG;
/

CREATE OR REPLACE PACKAGE BODY SQL_GENERATOR_PKG AS
  -- Process subquery JSON into SQL string
  FUNCTION process_subquery(p_subquery CLOB) RETURN VARCHAR2 IS
    v_subquery_sql VARCHAR2(4000);
    v_columns VARCHAR2(1000);
    v_tables VARCHAR2(1000);
    v_where VARCHAR2(2000) := '';
    v_join_clause VARCHAR2(1000) := '';
    v_logical_op VARCHAR2(10) := 'AND';
  BEGIN
    -- Extract columns
    BEGIN
      SELECT LISTAGG(column_name, ', ') 
      INTO v_columns
      FROM JSON_TABLE(p_subquery, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        v_columns := '*';
      WHEN OTHERS THEN
        v_columns := '*';
    END;
    
    -- Extract tables
    BEGIN
      SELECT LISTAGG(table_name, ', ') 
      INTO v_tables
      FROM JSON_TABLE(p_subquery, '$.tables[*]' COLUMNS (table_name VARCHAR2(50) PATH '$'));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        RAISE_APPLICATION_ERROR(-20002, 'Subquery must specify at least one table');
      WHEN OTHERS THEN
        RAISE_APPLICATION_ERROR(-20002, 'Error processing subquery tables: ' || SQLERRM);
    END;
    
    -- Process joins if present
    BEGIN
      SELECT LISTAGG(jt.join_type || ' ' || jt.join_table || ' ON ' || jt.join_condition, ' ') 
      INTO v_join_clause
      FROM JSON_TABLE(p_subquery, '$.joins[*]' 
             COLUMNS (
               join_type VARCHAR2(20) PATH '$.type',
               join_table VARCHAR2(50) PATH '$.table',
               join_condition VARCHAR2(200) PATH '$.on'
             )) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_join_clause := '';
    END;

    -- Get logical operator if specified
    BEGIN
      SELECT NVL(UPPER(jt.operator), 'AND')
      INTO v_logical_op
      FROM JSON_TABLE(p_subquery, '$' COLUMNS (operator VARCHAR2(10) PATH '$.operator')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_logical_op := 'AND';
    END;
    
    -- Process where conditions if present
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_subquery, '$.conditions[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            condition_value_type VARCHAR2(30)  PATH '$.value_type',
            condition_value2     VARCHAR2(100) PATH '$.value2',
            subquery             CLOB          PATH '$.subquery'
          )
        )
      ) LOOP
      

        IF v_where IS NULL THEN
          v_where := ' WHERE ';
        ELSE
          v_where := v_where || ' ' || v_logical_op || ' ';
        END IF;
       
       BEGIN
       
        DBMS_OUTPUT.PUT_LINE('Processing condition: ' || cond.condition_column || ' ' || 
                         cond.condition_operator || ' subquery? ' || 
                         CASE WHEN cond.subquery IS NOT NULL THEN 'YES' ELSE 'NO' END);
                         end;
        IF cond.subquery IS NOT NULL THEN
          -- Handle nested subqueries
          DECLARE
            v_nested_subquery VARCHAR2(4000);
          BEGIN
            v_nested_subquery := process_subquery(cond.subquery);
            
            v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' || 
                      v_nested_subquery;
          END;
        ELSE
          -- Handle regular conditions including BETWEEN
          IF UPPER(cond.condition_operator) = 'BETWEEN' AND cond.condition_value2 IS NOT NULL THEN
            v_where := v_where || cond.condition_column || ' BETWEEN ' || 
                      format_value(cond.condition_value, cond.condition_value_type) || 
                      ' AND ' || 
                      format_value(cond.condition_value2, cond.condition_value_type);
          ELSE
            v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' ||
                      format_value(cond.condition_value, cond.condition_value_type);
          END IF;
        END IF;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing subquery conditions: ' || SQLERRM);
        v_where := '';
    END;
    
    -- Build the subquery
    v_subquery_sql := '(SELECT ' || v_columns || 
                     ' FROM ' || v_tables || 
                     CASE WHEN v_join_clause IS NOT NULL THEN ' ' || v_join_clause ELSE '' END || 
                     v_where || ')';
                     
    RETURN v_subquery_sql;
  END process_subquery;
  
  -- Format value based on data type
  FUNCTION format_value(p_value VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS
    v_result VARCHAR2(4000);
  BEGIN
    IF p_value IS NULL THEN
      RETURN 'NULL';
    ELSIF p_data_type IS NOT NULL THEN
      CASE UPPER(p_data_type)
        WHEN 'NUMBER' THEN
          RETURN p_value;
        WHEN 'DATE' THEN
          RETURN 'TO_DATE(''' || p_value || ''', ''YYYY-MM-DD'')';
        WHEN 'TIMESTAMP' THEN
          RETURN 'TO_TIMESTAMP(''' || p_value || ''', ''YYYY-MM-DD HH24:MI:SS.FF'')';
        WHEN 'BOOLEAN' THEN
          IF UPPER(p_value) IN ('TRUE', 'YES', 'Y', '1') THEN
            RETURN '1';
          ELSE
            RETURN '0';
          END IF;
        WHEN 'IDENTIFIER' THEN 
          RETURN p_value;
        ELSE
          RETURN '''' || REPLACE(p_value, '''', '''''') || '''';
      END CASE;
    ELSIF REGEXP_LIKE(p_value, '^\d+(\.\d+)?$') THEN
      -- Looks like a number
      RETURN p_value;
    ELSE
      -- Default to string
      RETURN '''' || REPLACE(p_value, '''', '''''') || '''';
    END IF;
  END format_value;

  -- Generate WHERE clause for DML statements based on JSON conditions
  FUNCTION generate_where_clause(p_json CLOB) RETURN VARCHAR2 IS
    v_where       VARCHAR2(4000) := '';
    v_logical_op  VARCHAR2(10) := 'AND';
  BEGIN
    -- Get logical operator if specified
    BEGIN
      SELECT NVL(UPPER(jt.operator), 'AND')
      INTO v_logical_op
      FROM JSON_TABLE(p_json, '$' COLUMNS (operator VARCHAR2(10) PATH '$.where.operator')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_logical_op := 'AND';
    END;

    -- Process where conditions
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_json, '$.where.conditions[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            condition_value_type VARCHAR2(30)  PATH '$.value_type',
            condition_value2     VARCHAR2(100) PATH '$.value2',
            n_query              CLOB          FORMAT JSON PATH '$.subquery'
          )
        )
      ) LOOP
        IF v_where IS NOT NULL THEN
          v_where := v_where || ' ' || v_logical_op || ' ';
        END IF;
        
        IF cond.n_query IS NOT NULL THEN
          -- Handle subquery conditions
          DECLARE
            v_subquery_sql VARCHAR2(4000);
          BEGIN
            v_subquery_sql := process_subquery(cond.n_query);
            
            -- Special case for EXISTS and NOT EXISTS
            IF UPPER(cond.condition_column) IN ('EXISTS', 'NOT EXISTS') THEN
              v_where := v_where || cond.condition_column || ' ' || v_subquery_sql;
            ELSE
              v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' || 
                        v_subquery_sql;
            END IF;
          END;
        ELSE
          -- Handle regular conditions including BETWEEN
          IF UPPER(cond.condition_operator) = 'BETWEEN' AND cond.condition_value2 IS NOT NULL THEN
            v_where := v_where || cond.condition_column || ' BETWEEN ' || 
                      format_value(cond.condition_value, cond.condition_value_type) || 
                      ' AND ' || 
                      format_value(cond.condition_value2, cond.condition_value_type);
          ELSE
            v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' ||
                      format_value(cond.condition_value, cond.condition_value_type);
          END IF;
        END IF;
      END LOOP;
      
      IF v_where IS NOT NULL THEN
        v_where := ' WHERE ' || v_where;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing where conditions: ' || SQLERRM);
        v_where := '';
    END;
    
    RETURN v_where;
  END generate_where_clause;

  -- Process a single query part
  FUNCTION process_query_part(p_query_part CLOB) RETURN VARCHAR2 IS
    v_sql         VARCHAR2(4000);
    v_columns     VARCHAR2(1000);
    v_tables      VARCHAR2(1000);
    v_join_clause VARCHAR2(1000) := '';
    v_where       VARCHAR2(4000) := '';
    v_group_by    VARCHAR2(1000) := '';
    v_having      VARCHAR2(2000) := '';
    v_logical_op  VARCHAR2(10) := 'AND';
  BEGIN
    -- Extract columns
    SELECT LISTAGG(column_name, ', ') 
    INTO v_columns
    FROM JSON_TABLE(p_query_part, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));

    -- Extract tables
    SELECT LISTAGG(table_name, ', ') 
    INTO v_tables
    FROM JSON_TABLE(p_query_part, '$.tables[*]' COLUMNS (table_name VARCHAR2(50) PATH '$'));

    -- Process joins if present
    BEGIN
      SELECT LISTAGG(jt.join_type || ' ' || jt.join_table || ' ON ' || jt.join_condition, ' ') 
      INTO v_join_clause
      FROM JSON_TABLE(p_query_part, '$.joins[*]' 
             COLUMNS (
               join_type VARCHAR2(20) PATH '$.type',
               join_table VARCHAR2(50) PATH '$.table',
               join_condition VARCHAR2(200) PATH '$.on'
             )) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_join_clause := '';
    END;

    -- Get logical operator if specified
    BEGIN
      SELECT NVL(UPPER(jt.operator), 'AND')
      INTO v_logical_op
      FROM JSON_TABLE(p_query_part, '$' COLUMNS (operator VARCHAR2(10) PATH '$.where.operator')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_logical_op := 'AND';
    END;

    -- Process where conditions
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_query_part, '$.where.conditions[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            condition_value_type VARCHAR2(30)  PATH '$.value_type',
            condition_value2     VARCHAR2(100) PATH '$.value2',
            n_query              CLOB          FORMAT JSON PATH '$.subquery'
          )
        )
      ) LOOP
        IF v_where IS NOT NULL THEN
          v_where := v_where || ' ' || v_logical_op || ' ';
        END IF;
         DBMS_OUTPUT.PUT_LINE('cond.n_query: ' || cond.n_query);
         DBMS_OUTPUT.PUT_LINE('JSON: ' || p_query_part);
        IF cond.n_query IS NOT NULL THEN
          -- Handle subquery conditions (IN, NOT IN, EXISTS, NOT EXISTS)
          DECLARE
            v_subquery_sql VARCHAR2(4000);
          BEGIN
            v_subquery_sql := process_subquery(cond.n_query);
            
            -- Special case for EXISTS and NOT EXISTS which don't need a column name
            IF UPPER(cond.condition_column) IN ('EXISTS', 'NOT EXISTS') THEN
              v_where := v_where || cond.condition_column || ' ' || v_subquery_sql;
            ELSE
              v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' || 
                        v_subquery_sql;
            END IF;
          END;
        ELSE
          -- Handle regular conditions including BETWEEN
          IF UPPER(cond.condition_operator) = 'BETWEEN' AND cond.condition_value2 IS NOT NULL THEN
            v_where := v_where || cond.condition_column || ' BETWEEN ' || 
                      format_value(cond.condition_value, cond.condition_value_type) || 
                      ' AND ' || 
                      format_value(cond.condition_value2, cond.condition_value_type);
          ELSE
            v_where := v_where || cond.condition_column || ' ' || cond.condition_operator || ' ' ||
                      format_value(cond.condition_value, cond.condition_value_type);
          END IF;
        END IF;
      END LOOP;
      
      IF v_where IS NOT NULL THEN
        v_where := ' WHERE ' || v_where;
        DBMS_OUTPUT.PUT_LINE('v_where: ' || v_where);
       
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing where conditions: ' || SQLERRM);
        v_where := '';
    END;

    -- Process GROUP BY if present
    BEGIN
      SELECT LISTAGG(column_name, ', ')
      INTO v_group_by
      FROM JSON_TABLE(p_query_part, '$.group_by[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));
    EXCEPTION
      WHEN OTHERS THEN
        v_group_by := '';
    END;
    
    -- Process HAVING if present
    BEGIN
      FOR cond IN (
        SELECT *
        FROM JSON_TABLE(p_query_part, '$.having[*]'
          COLUMNS (
            condition_column     VARCHAR2(100) PATH '$.column',
            condition_operator   VARCHAR2(20)  PATH '$.operator',
            condition_value      VARCHAR2(100) PATH '$.value',
            condition_value_type VARCHAR2(30)  PATH '$.value_type'
          )
        )
      ) LOOP
        IF v_having IS NULL THEN
          v_having := ' HAVING ';
        ELSE
          v_having := v_having || ' AND ';
        END IF;
        
        v_having := v_having || cond.condition_column || ' ' || cond.condition_operator || ' ' ||
                   format_value(cond.condition_value, cond.condition_value_type);
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        v_having := '';
    END;

    -- Build the SQL statement for this part
    v_sql := 'SELECT ' || v_columns || 
             ' FROM ' || v_tables || 
             CASE WHEN v_join_clause IS NOT NULL THEN ' ' || v_join_clause ELSE '' END || 
             v_where ||
             CASE WHEN v_group_by IS NOT NULL THEN ' GROUP BY ' || v_group_by ELSE '' END ||
             v_having;
             
    RETURN v_sql;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error in process_query_part: ' || SQLERRM);
      RETURN NULL;
  END process_query_part;

  -- Main function to parse JSON and execute SELECT query
  FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR IS
    v_sql         VARCHAR2(4000);
    v_cur         SYS_REFCURSOR;
    v_main_query  VARCHAR2(4000);
    v_union_all   BOOLEAN := FALSE;
  BEGIN
    -- Check if there are UNION parts in the JSON
    DECLARE
      v_union_parts_json  CLOB;
      v_union_part_sql    VARCHAR2(4000);
      v_union_parts_count NUMBER := 0;
    BEGIN
      -- First, process the main query
      v_main_query := process_query_part(p_json);
      
      -- Check if there are union parts
      BEGIN
        SELECT COUNT(*)
        INTO v_union_parts_count
        FROM JSON_TABLE(p_json, '$.union_parts[*]' COLUMNS (dummy VARCHAR2(1) PATH '$.dummy'));
        DBMS_OUTPUT.PUT_LINE('v_union_parts_count: ' || v_union_parts_count);
      EXCEPTION
        WHEN OTHERS THEN
          v_union_parts_count := 0;
      END;
      
      -- Check if we should use UNION ALL (default is UNION)
      BEGIN
        SELECT UPPER(jt.union_type)
        INTO v_union_part_sql
        FROM JSON_TABLE(p_json, '$' COLUMNS (union_type VARCHAR2(10) PATH '$.union_type')) jt;
        
        IF v_union_part_sql = 'UNION ALL' THEN
          v_union_all := TRUE;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          v_union_all := FALSE;
      END;
      
      -- Start with the main query
      v_sql := v_main_query;
      
      -- Process UNION parts if they exist
      IF v_union_parts_count > 0 THEN
        FOR union_part IN (
          SELECT part_json
          FROM JSON_TABLE(p_json, '$.union_parts[*]' COLUMNS (part_json CLOB FORMAT JSON PATH '$'))
        ) LOOP
          -- Process this UNION part
          v_union_part_sql := process_query_part(union_part.part_json);
          DBMS_OUTPUT.PUT_LINE('v_union_part_sql: ' || v_union_part_sql);
          -- Add to main SQL with appropriate UNION type
          IF v_union_all THEN
            v_sql := v_sql || ' UNION ALL ' || v_union_part_sql;
          ELSE
            v_sql := v_sql || ' UNION ' || v_union_part_sql;
             DBMS_OUTPUT.PUT_LINE('v_sql: ' || v_sql);
          END IF;
        END LOOP;
      END IF;
    END;
    
    DBMS_OUTPUT.PUT_LINE('Generated SQL: ' || v_sql); -- Debugging output
    
    -- Execute the final query and return the cursor
    OPEN v_cur FOR v_sql;
    RETURN v_cur;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20001, 'Error generating SQL query: ' || SQLERRM || '. SQL: ' || v_sql);
  END json_select_handler;
  
  -- Function to handle INSERT operations
  FUNCTION json_insert_handler(p_json CLOB) RETURN NUMBER IS
    v_sql           VARCHAR2(4000);
    v_table_name    VARCHAR2(100);
    v_columns       VARCHAR2(1000);
    v_values_clause VARCHAR2(4000);
    v_rows_affected NUMBER := 0;
    v_query_type    VARCHAR2(20);
    v_select_query  VARCHAR2(4000);
  BEGIN
    -- Extract target table name
    SELECT jt.table_name
    INTO v_table_name
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(100) PATH '$.table')) jt;
    
    -- Extract columns
    SELECT LISTAGG(column_name, ', ') 
    INTO v_columns
    FROM JSON_TABLE(p_json, '$.columns[*]' COLUMNS (column_name VARCHAR2(100) PATH '$'));
    
    -- Check whether we have a VALUES clause or a SELECT subquery
    BEGIN
      SELECT UPPER(jt.values_type)
      INTO v_query_type
      FROM JSON_TABLE(p_json, '$' COLUMNS (values_type VARCHAR2(20) PATH '$.values_type')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_query_type := 'VALUES';  -- Default to VALUES if not specified
    END;
    
    -- Process based on insert type
    IF v_query_type = 'SELECT' THEN
      -- It's an INSERT ... SELECT statement
      DECLARE
        v_select_json CLOB;
      BEGIN
        SELECT jt.select_query
        INTO v_select_json
        FROM JSON_TABLE(p_json, '$' COLUMNS (select_query CLOB FORMAT JSON PATH '$.select_query')) jt;
        
        -- Process the SELECT part
        IF v_select_json IS NOT NULL THEN
          v_select_query := process_query_part(v_select_json);
          
          -- Build the INSERT ... SELECT statement
          v_sql := 'INSERT INTO ' || v_table_name || '(' || v_columns || ') ' || v_select_query;
        ELSE
          RAISE_APPLICATION_ERROR(-20003, 'SELECT query not specified for INSERT ... SELECT operation');
        END IF;
      END;
    ELSE
      -- It's a standard INSERT ... VALUES statement
      DECLARE
        v_values_array VARCHAR2(4000) := '';
        v_value_item   VARCHAR2(4000);
      BEGIN
        -- Collect values for each row
        FOR row_values IN (
          SELECT *
          FROM JSON_TABLE(p_json, '$.values[*]' COLUMNS (
            NESTED PATH '$[*]' COLUMNS (
              value_data      VARCHAR2(1000) PATH '$.value',
              value_type      VARCHAR2(30)   PATH '$.type',
              subquery        CLOB           FORMAT JSON PATH '$.subquery'
            )
          ))
        ) LOOP
          -- Process each value in this row
          IF row_values.subquery IS NOT NULL THEN
            -- Handle subquery as a value
            v_value_item := process_subquery(row_values.subquery);
          ELSE
            -- Handle regular value
            v_value_item := format_value(row_values.value_data, row_values.value_type);
          END IF;

          

          IF v_values_array IS NULL OR v_values_array = '' THEN
            v_values_array := v_value_item;
          ELSE
            v_values_array := v_values_array || ', ' || v_value_item;
          END IF;
        END LOOP;
        
        -- Build the INSERT ... VALUES statement
        v_sql := 'INSERT INTO ' || v_table_name || '(' || v_columns || ') VALUES (' || v_values_array || ')';
      END;
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Generated INSERT SQL: ' || v_sql);
    
    -- Execute the INSERT statement
    EXECUTE IMMEDIATE v_sql;
    v_rows_affected := SQL%ROWCOUNT;
    
    RETURN v_rows_affected;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20004, 'Error executing INSERT: ' || SQLERRM || '. SQL: ' || v_sql);
  END json_insert_handler;
  
  -- Function to handle UPDATE operations
  FUNCTION json_update_handler(p_json CLOB) RETURN NUMBER IS
    v_sql           VARCHAR2(4000);
    v_table_name    VARCHAR2(100);
    v_set_clause    VARCHAR2(4000) := '';
    v_where_clause  VARCHAR2(4000);
    v_rows_affected NUMBER := 0;
  BEGIN
    -- Extract target table name
    SELECT jt.table_name
    INTO v_table_name
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(100) PATH '$.table')) jt;
    
    -- Build SET clause
    FOR upd_item IN (
      SELECT *
      FROM JSON_TABLE(p_json, '$.set_values[*]'
        COLUMNS (
          column_name     VARCHAR2(100) PATH '$.column',
          value           VARCHAR2(1000) PATH '$.value',
          value_type      VARCHAR2(30)   PATH '$.value_type',
          subquery        CLOB           FORMAT JSON PATH '$.subquery'
        )
      )
    ) LOOP
      -- Add comma if needed
      IF v_set_clause IS NOT NULL THEN
        v_set_clause := v_set_clause || ', ';
      END IF;
      
      -- Check if value is from subquery
      IF upd_item.subquery IS NOT NULL THEN
        -- Handle subquery as a value
        v_set_clause := v_set_clause || upd_item.column_name || ' = ' || 
                        process_subquery(upd_item.subquery);
      ELSE
        -- Handle regular value
        v_set_clause := v_set_clause || upd_item.column_name || ' = ' || 
                        format_value(upd_item.value, upd_item.value_type);
      END IF;
    END LOOP;
    
    -- Generate WHERE clause
    v_where_clause := generate_where_clause(p_json);
    
    -- Build the UPDATE statement
    v_sql := 'UPDATE ' || v_table_name || 
             ' SET ' || v_set_clause || 
             v_where_clause;
    
    DBMS_OUTPUT.PUT_LINE('Generated UPDATE SQL: ' || v_sql);
    
    -- Execute the UPDATE statement
    EXECUTE IMMEDIATE v_sql;
    v_rows_affected := SQL%ROWCOUNT;
    
    RETURN v_rows_affected;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20005, 'Error executing UPDATE: ' || SQLERRM || '. SQL: ' || v_sql);
  END json_update_handler;
  
  -- Function to handle DELETE operations
  FUNCTION json_delete_handler(p_json CLOB) RETURN NUMBER IS
    v_sql           VARCHAR2(4000);
    v_table_name    VARCHAR2(100);
    v_where_clause  VARCHAR2(4000);
    v_rows_affected NUMBER := 0;
  BEGIN
    -- Extract target table name
    SELECT jt.table_name
    INTO v_table_name
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(100) PATH '$.table')) jt;
    
    -- Generate WHERE clause
    v_where_clause := generate_where_clause(p_json);
    
    -- Build the DELETE statement
    v_sql := 'DELETE FROM ' || v_table_name || v_where_clause;
    
    DBMS_OUTPUT.PUT_LINE('Generated DELETE SQL: ' || v_sql);
    
    -- Execute the DELETE statement
    EXECUTE IMMEDIATE v_sql;
    v_rows_affected := SQL%ROWCOUNT;
    
    RETURN v_rows_affected;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20006, 'Error executing DELETE: ' || SQLERRM || '. SQL: ' || v_sql);
  END json_delete_handler;
  
  -- Main entry point for SELECT queries
  FUNCTION execute_sql(p_json CLOB) RETURN SYS_REFCURSOR IS
    v_query_type VARCHAR2(20);
    v_cursor     SYS_REFCURSOR;
  BEGIN
    -- Determine the query type
    BEGIN
      SELECT NVL(UPPER(jt.query_type), 'SELECT')
      INTO v_query_type
      FROM JSON_TABLE(p_json, '$' COLUMNS (query_type VARCHAR2(20) PATH '$.query_type')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_query_type := 'SELECT'; -- Default to SELECT if not specified
    END;
    
    -- Execute the appropriate handler
    IF v_query_type = 'SELECT' THEN
      RETURN json_select_handler(p_json);
    ELSE
      RAISE_APPLICATION_ERROR(-20007, 'Only SELECT queries can return a cursor. For DML operations use execute_dml procedure');
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20008, 'Error in execute_sql: ' || SQLERRM);
  END execute_sql;
  
  -- Main entry point for DML operations
  PROCEDURE execute_dml(p_json CLOB, p_rows_affected OUT NUMBER) IS
    v_query_type VARCHAR2(20);
  BEGIN
    -- Determine the query type
    SELECT UPPER(jt.query_type)
    INTO v_query_type
    FROM JSON_TABLE(p_json, '$' COLUMNS (query_type VARCHAR2(20) PATH '$.query_type')) jt;
    
    -- Execute the appropriate handler
    CASE v_query_type
      WHEN 'INSERT' THEN
        p_rows_affected := json_insert_handler(p_json);
      WHEN 'UPDATE' THEN
        p_rows_affected := json_update_handler(p_json);
      WHEN 'DELETE' THEN
        p_rows_affected := json_delete_handler(p_json);
      ELSE
        RAISE_APPLICATION_ERROR(-20009, 'Unsupported query type: ' || v_query_type);
    END CASE;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Error in execute_dml: ' || SQLERRM);
  END execute_dml;
END SQL_GENERATOR_PKG;
/