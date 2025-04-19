CREATE OR REPLACE PACKAGE SQL_GENERATOR_PKG AS
  -- Main function to handle JSON input and return cursor with query results
  FUNCTION json_select_handler(p_json CLOB) RETURN SYS_REFCURSOR;
  
  -- Helper function to process subqueries
  FUNCTION process_subquery(p_subquery CLOB) RETURN VARCHAR2;
  
  -- Helper function to handle data type conversions
  FUNCTION format_value(p_value VARCHAR2, p_data_type VARCHAR2 DEFAULT NULL) RETURN VARCHAR2;
  
  -- Helper function to process a single query part (for main queries and UNION parts)
  FUNCTION process_query_part(p_query_part CLOB) RETURN VARCHAR2;
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

  -- Main function to parse JSON and execute query
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
END SQL_GENERATOR_PKG;
/