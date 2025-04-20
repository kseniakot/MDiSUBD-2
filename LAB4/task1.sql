CREATE OR REPLACE PACKAGE SQL_GENERATOR_PKG AUTHID CURRENT_USER AS
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
  
  -- Functions for DDL operations
  FUNCTION json_create_table_handler(p_json CLOB) RETURN NUMBER;
  FUNCTION json_drop_table_handler(p_json CLOB) RETURN NUMBER;
  
  -- Main entry point for any SQL operation (DQL or DML)
  FUNCTION execute_sql(p_json CLOB) RETURN SYS_REFCURSOR;
  PROCEDURE execute_dml(p_json CLOB, p_rows_affected OUT NUMBER);
  PROCEDURE execute_ddl(p_json CLOB, p_result OUT NUMBER);
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
  
  -- Function to handle CREATE TABLE DDL operation
  FUNCTION json_create_table_handler(p_json CLOB) RETURN NUMBER IS
    v_sql           VARCHAR2(4000);
    v_table_name    VARCHAR2(100);
    v_column_defs   VARCHAR2(4000) := '';
    v_constraints   VARCHAR2(4000) := '';
    v_success       NUMBER := 1; -- 1 = success, 0 = failure
    v_pk_column     VARCHAR2(100) := NULL; -- Store primary key column name
    v_auto_pk       NUMBER(1) := 0; -- Flag to indicate if we should generate a sequence/trigger
    v_pk_data_type  VARCHAR2(100) := NULL; -- Store PK column data type
    v_column_count  NUMBER := 0; -- Count of columns processed
    v_fk_constraints CLOB := NULL; -- Store foreign key constraints to execute separately
  BEGIN
    -- Extract target table name
    SELECT jt.table_name
    INTO v_table_name
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(100) PATH '$.table')) jt;
    
    -- Check if auto PK is enabled
    BEGIN
      SELECT CASE WHEN UPPER(jt.auto_primary_key) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END
      INTO v_auto_pk
      FROM JSON_TABLE(p_json, '$' COLUMNS (auto_primary_key VARCHAR2(10) PATH '$.auto_primary_key')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_auto_pk := 0;
    END;
    
    -- Process column definitions
    FOR col IN (
      SELECT *
      FROM JSON_TABLE(p_json, '$.columns[*]'
        COLUMNS (
          col_name      VARCHAR2(100) PATH '$.name',
          col_type      VARCHAR2(100) PATH '$.type',
          col_default   VARCHAR2(200) PATH '$.default',
          constraints   VARCHAR2(1000) FORMAT JSON PATH '$.constraints'
        )
      )
    ) LOOP
      v_column_count := v_column_count + 1;
      
      -- Add comma and space if needed
      IF v_column_count > 1 THEN
        v_column_defs := v_column_defs || ', ';
      END IF;
  
      -- Start new column definition with proper spacing
      v_column_defs := v_column_defs || col.col_name || ' ' || col.col_type;
              
      -- Add column constraints if any
      DECLARE
        v_col_constraints VARCHAR2(1000) := '';
        v_is_pk NUMBER(1) := 0;
      BEGIN
        FOR const IN (
          SELECT *
          FROM JSON_TABLE(col.constraints, '$[*]' COLUMNS (
            constraint_text VARCHAR2(100) PATH '$'
          ))
        ) LOOP
          v_col_constraints := v_col_constraints || ' ' || const.constraint_text;
          
          -- Check if this column is a primary key
          IF UPPER(const.constraint_text) = 'PRIMARY KEY' THEN
            v_pk_column := col.col_name;
            v_pk_data_type := col.col_type;
            v_is_pk := 1;
          END IF;
        END LOOP;
        
        -- Add column constraints to the definition
        IF v_col_constraints IS NOT NULL THEN
          v_column_defs := v_column_defs || v_col_constraints;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          NULL; -- No constraints for this column
      END;
      
      -- Add default value if specified
      IF col.col_default IS NOT NULL THEN
        v_column_defs := v_column_defs || ' DEFAULT ' || col.col_default;
      END IF;
    END LOOP;
    
    -- Process table-level constraints if any
    BEGIN
      FOR tconst IN (
        SELECT *
        FROM JSON_TABLE(p_json, '$.constraints[*]'
          COLUMNS (
            const_type    VARCHAR2(30) PATH '$.type',
            const_name    VARCHAR2(100) PATH '$.name',
            const_columns VARCHAR2(1000) FORMAT JSON PATH '$.columns',
            check_condition VARCHAR2(1000) PATH '$.check_condition',
            ref_table     VARCHAR2(100) PATH '$.references.table',
            ref_columns   VARCHAR2(1000) FORMAT JSON PATH '$.references.columns'
          )
        )
      ) LOOP
        -- Special handling for foreign keys - we'll add them after table creation 
        -- to avoid issues with referenced tables not existing yet
        IF UPPER(tconst.const_type) = 'FOREIGN KEY' THEN
          DECLARE
            v_column_list VARCHAR2(1000) := '';
            v_ref_column_list VARCHAR2(1000) := '';
            v_fk_sql VARCHAR2(4000);
          BEGIN
            -- Build the column list for the foreign key
            FOR col IN (
              SELECT column_name
              FROM JSON_TABLE(tconst.const_columns, '$[*]' COLUMNS (
                column_name VARCHAR2(100) PATH '$'
              ))
            ) LOOP
              IF v_column_list IS NOT NULL AND v_column_list != '' THEN
                v_column_list := v_column_list || ', ';
              END IF;
              v_column_list := v_column_list || col.column_name;
            END LOOP;
            
            -- Build the referenced column list
            FOR col IN (
              SELECT column_name
              FROM JSON_TABLE(tconst.ref_columns, '$[*]' COLUMNS (
                column_name VARCHAR2(100) PATH '$'
              ))
            ) LOOP
              IF v_ref_column_list IS NOT NULL AND v_ref_column_list != '' THEN
                v_ref_column_list := v_ref_column_list || ', ';
              END IF;
              v_ref_column_list := v_ref_column_list || col.column_name;
            END LOOP;
            
            -- Build the ALTER TABLE ADD CONSTRAINT command for the foreign key
            v_fk_sql := 'ALTER TABLE ' || v_table_name || 
                        ' ADD CONSTRAINT ' || tconst.const_name || 
                        ' FOREIGN KEY (' || v_column_list || ')' || 
                        ' REFERENCES ' || tconst.ref_table || '(' || v_ref_column_list || ')';
                        
            -- Add this foreign key command to our list to execute after table creation
            v_fk_constraints := v_fk_constraints || v_fk_sql || ';';
          END;
        ELSE
          DECLARE
            v_column_list VARCHAR2(1000) := '';
          BEGIN
            -- Add comma if needed
            IF v_constraints IS NOT NULL AND v_constraints != '' THEN
              v_constraints := v_constraints || ', ';
            END IF;
            
            -- Build the constraint definition
            IF tconst.const_name IS NOT NULL THEN
              v_constraints := v_constraints || 'CONSTRAINT ' || tconst.const_name || ' ';
            END IF;
            
            -- Handle different constraint types
            IF UPPER(tconst.const_type) = 'CHECK' THEN
              -- Handle CHECK constraints
              v_constraints := v_constraints || 'CHECK (' || tconst.check_condition || ')';
            ELSE
              -- Handle PRIMARY KEY and UNIQUE constraints
              -- Build the comma-separated list of columns
              FOR col IN (
                SELECT column_name
                FROM JSON_TABLE(tconst.const_columns, '$[*]' COLUMNS (
                  column_name VARCHAR2(100) PATH '$'
                ))
              ) LOOP
                IF v_column_list IS NOT NULL AND v_column_list != '' THEN
                  v_column_list := v_column_list || ', ';
                END IF;
                v_column_list := v_column_list || col.column_name;
              END LOOP;
              
              v_constraints := v_constraints || tconst.const_type || ' (' || v_column_list || ')';
              
              -- If this is a PRIMARY KEY constraint and we have only one column, store it for later
              IF UPPER(tconst.const_type) = 'PRIMARY KEY' AND 
                INSTR(v_column_list, ',') = 0 AND v_pk_column IS NULL THEN
                v_pk_column := TRIM(v_column_list);
                
                -- Try to determine the data type of the PK column
                FOR col IN (
                  SELECT col_name, col_type
                  FROM JSON_TABLE(p_json, '$.columns[*]'
                    COLUMNS (
                      col_name VARCHAR2(100) PATH '$.name',
                      col_type VARCHAR2(100) PATH '$.type'
                    )
                  )
                  WHERE col_name = TRIM(v_column_list)
                ) LOOP
                  v_pk_data_type := col.col_type;
                END LOOP;
              END IF;
            END IF;
          END;
        END IF;
      END LOOP;
      
      -- Add constraints to column definitions if any
      IF v_constraints IS NOT NULL AND v_constraints != '' THEN
        IF v_column_defs IS NOT NULL AND v_column_defs != '' THEN
          v_column_defs := v_column_defs || ', ' || v_constraints;
        ELSE
          v_column_defs := v_constraints;
        END IF;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error processing table constraints: ' || SQLERRM);
    END;
    
    -- Build the CREATE TABLE statement - use TRIM to ensure clean output
    v_sql := 'CREATE TABLE ' || v_table_name || ' (' || TRIM(v_column_defs) || ')';
    
    -- Add storage parameters if specified
    DECLARE
      v_tablespace VARCHAR2(100);
    BEGIN
      SELECT jt.tablespace
      INTO v_tablespace
      FROM JSON_TABLE(p_json, '$' COLUMNS (tablespace VARCHAR2(100) PATH '$.tablespace')) jt;
      
      IF v_tablespace IS NOT NULL THEN
        v_sql := v_sql || ' TABLESPACE ' || v_tablespace;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        NULL; -- No tablespace specified
    END;
    
    DBMS_OUTPUT.PUT_LINE('Generated CREATE TABLE SQL: ' || v_sql);
    
    -- Execute the CREATE TABLE statement
    EXECUTE IMMEDIATE v_sql;
    
    -- Now execute any foreign key constraints
    IF v_fk_constraints IS NOT NULL THEN
      DECLARE
        v_start_pos  PLS_INTEGER := 1;
        v_end_pos    PLS_INTEGER;
        v_current_stmt VARCHAR2(4000);
      BEGIN
        -- Manually parse the semicolon-separated statements to avoid CONNECT BY issues
        LOOP
          -- Find the next semicolon
          v_end_pos := INSTR(v_fk_constraints, ';', v_start_pos);
          
          -- Exit if no more semicolons found
          EXIT WHEN v_end_pos = 0;
          
          -- Extract the current statement
          v_current_stmt := TRIM(SUBSTR(v_fk_constraints, v_start_pos, v_end_pos - v_start_pos));
          
          -- Process the current statement
          IF v_current_stmt IS NOT NULL AND LENGTH(v_current_stmt) > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Executing foreign key constraint: ' || v_current_stmt);
            
            BEGIN
              EXECUTE IMMEDIATE v_current_stmt;
              DBMS_OUTPUT.PUT_LINE('Foreign key constraint added successfully');
            EXCEPTION
              WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error adding foreign key constraint: ' || SQLERRM);
                -- Don't fail the whole operation if a single FK fails
            END;
          END IF;
          
          -- Move to the position after the semicolon
          v_start_pos := v_end_pos + 1;
        END LOOP;
      END;
    END IF;
    
    -- If auto PK is enabled and we have a PK column, create a sequence and trigger
    IF v_auto_pk = 1 AND v_pk_column IS NOT NULL THEN
      -- Create sequence for the primary key
      DECLARE
        v_seq_name VARCHAR2(128) := SUBSTR(v_table_name || '_' || v_pk_column || '_SEQ', 1, 128);
        v_seq_sql VARCHAR2(1000);
      BEGIN
        -- Create sequence for auto incrementing
        v_seq_sql := 'CREATE SEQUENCE ' || v_seq_name || 
                     ' START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE';
        DBMS_OUTPUT.PUT_LINE('Generated SEQUENCE SQL: ' || v_seq_sql);
        
        BEGIN
          EXECUTE IMMEDIATE v_seq_sql;
        EXCEPTION
          WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error creating sequence: ' || SQLERRM);
            -- Continue execution - don't fail if sequence creation fails
        END;
        
        -- Create trigger for auto PK
        DECLARE
          v_trigger_name VARCHAR2(128) := SUBSTR(v_table_name || '_' || v_pk_column || '_TRG', 1, 128);
          v_trigger_sql VARCHAR2(2000);
        BEGIN
          v_trigger_sql := 'CREATE OR REPLACE TRIGGER ' || v_trigger_name || ' 
                           BEFORE INSERT ON ' || v_table_name || ' 
                           FOR EACH ROW 
                           BEGIN 
                               IF :NEW.' || v_pk_column || ' IS NULL THEN 
                                   SELECT ' || v_seq_name || '.NEXTVAL 
                                   INTO :NEW.' || v_pk_column || ' 
                                   FROM DUAL; 
                               END IF; 
                           END;';
          
          DBMS_OUTPUT.PUT_LINE('Generated TRIGGER SQL: ' || v_trigger_sql);
          
          BEGIN
            EXECUTE IMMEDIATE v_trigger_sql;
          EXCEPTION
            WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error creating trigger: ' || SQLERRM);
              -- Continue execution - don't fail if trigger creation fails
          END;
        END;
      END;
    END IF;
    
    RETURN v_success;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error executing CREATE TABLE: ' || SQLERRM);
      RETURN 0; -- Failure
  END json_create_table_handler;

  -- Function to handle DROP TABLE DDL operation
  FUNCTION json_drop_table_handler(p_json CLOB) RETURN NUMBER IS
    v_sql              VARCHAR2(4000);
    v_table_name       VARCHAR2(100);
    v_cascade_constraints NUMBER(1) := 0; -- 0 = false, 1 = true
    v_purge           NUMBER(1) := 0; -- 0 = false, 1 = true
    v_success         NUMBER := 1; -- 1 = success, 0 = failure
  BEGIN
    -- Extract target table name
    SELECT jt.table_name
    INTO v_table_name
    FROM JSON_TABLE(p_json, '$' COLUMNS (table_name VARCHAR2(100) PATH '$.table')) jt;
    
    -- Check if CASCADE CONSTRAINTS is specified
    BEGIN
      SELECT CASE WHEN UPPER(jt.cascade_constraints) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END
      INTO v_cascade_constraints
      FROM JSON_TABLE(p_json, '$' COLUMNS (cascade_constraints VARCHAR2(10) PATH '$.cascade_constraints')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_cascade_constraints := 0;
    END;
    
    -- Check if PURGE is specified
    BEGIN
      SELECT CASE WHEN UPPER(jt.purge) IN ('TRUE', 'YES', 'Y', '1') THEN 1 ELSE 0 END
      INTO v_purge
      FROM JSON_TABLE(p_json, '$' COLUMNS (purge VARCHAR2(10) PATH '$.purge')) jt;
    EXCEPTION
      WHEN OTHERS THEN
        v_purge := 0;
    END;
    
    -- Build the DROP TABLE statement
    v_sql := 'DROP TABLE ' || v_table_name;
    
    -- Add CASCADE CONSTRAINTS if specified
    IF v_cascade_constraints = 1 THEN
      v_sql := v_sql || ' CASCADE CONSTRAINTS';
    END IF;
    
    -- Add PURGE if specified
    IF v_purge = 1 THEN
      v_sql := v_sql || ' PURGE';
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('Generated DROP TABLE SQL: ' || v_sql);
    
    -- Execute the DROP TABLE statement
    EXECUTE IMMEDIATE v_sql;
    
    RETURN v_success;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error executing DROP TABLE: ' || SQLERRM);
      RETURN 0; -- Failure
  END json_drop_table_handler;
  
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
  
  -- Main entry point for DDL operations
  PROCEDURE execute_ddl(p_json CLOB, p_result OUT NUMBER) IS
    v_query_type VARCHAR2(20);
  BEGIN
    -- Determine the query type
    SELECT UPPER(jt.query_type)
    INTO v_query_type
    FROM JSON_TABLE(p_json, '$' COLUMNS (query_type VARCHAR2(20) PATH '$.query_type')) jt;
    
    -- Execute the appropriate handler
    CASE v_query_type
      WHEN 'CREATE_TABLE' THEN
        p_result := json_create_table_handler(p_json);
      WHEN 'DROP_TABLE' THEN
        p_result := json_drop_table_handler(p_json);
      ELSE
        RAISE_APPLICATION_ERROR(-20011, 'Unsupported DDL query type: ' || v_query_type);
    END CASE;
  EXCEPTION
    WHEN OTHERS THEN
      p_result := 0; -- Failure
      RAISE_APPLICATION_ERROR(-20012, 'Error in execute_ddl: ' || SQLERRM);
  END execute_ddl;
  
  -- Update the existing execute_dml procedure to support DDL operations
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
      WHEN 'CREATE_TABLE' THEN
        p_rows_affected := json_create_table_handler(p_json);
      WHEN 'DROP_TABLE' THEN
        p_rows_affected := json_drop_table_handler(p_json);
      ELSE
        RAISE_APPLICATION_ERROR(-20009, 'Unsupported query type: ' || v_query_type);
    END CASE;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20010, 'Error in execute_dml: ' || SQLERRM);
  END execute_dml;
END SQL_GENERATOR_PKG;
/