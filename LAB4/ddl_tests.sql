-- Setup test environment
-- Clean up any existing test objects
BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE ddl_test_employees CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE ddl_test_departments CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

----------------------------
-- TEST 1: Simple CREATE TABLE
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_employees",
    "columns": [
      {
        "name": "employee_id",
        "type": "NUMBER(6)",
        "constraints": ["PRIMARY KEY"]
      },
      {
        "name": "first_name",
        "type": "VARCHAR2(50)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "last_name",
        "type": "VARCHAR2(50)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "email",
        "type": "VARCHAR2(100)",
        "constraints": ["UNIQUE"]
      },
      {
        "name": "hire_date",
        "type": "DATE",
        "default": "SYSDATE"
      },
      {
        "name": "salary",
        "type": "NUMBER(10,2)"
      }
    ]
  }';
  v_result NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 1: Simple CREATE TABLE');
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
  -- Verify the table was created
  FOR c IN (SELECT column_name, data_type, data_length, nullable 
            FROM user_tab_columns 
            WHERE table_name = 'DDL_TEST_EMPLOYEES'
            ORDER BY column_id) LOOP
    DBMS_OUTPUT.PUT_LINE('Column: ' || c.column_name || 
                         ', Type: ' || c.data_type || 
                         CASE WHEN c.data_length IS NOT NULL THEN '(' || c.data_length || ')' ELSE '' END ||
                         ', Nullable: ' || c.nullable);
  END LOOP;
  
  -- Verify constraints
  FOR c IN (SELECT constraint_name, constraint_type, search_condition
            FROM user_constraints
            WHERE table_name = 'DDL_TEST_EMPLOYEES'
            ORDER BY constraint_name) LOOP
    DBMS_OUTPUT.PUT_LINE('Constraint: ' || c.constraint_name || 
                         ', Type: ' || 
                         CASE c.constraint_type 
                           WHEN 'P' THEN 'PRIMARY KEY'
                           WHEN 'U' THEN 'UNIQUE'
                           WHEN 'C' THEN 'CHECK/NOT NULL'
                           ELSE c.constraint_type
                         END);
  END LOOP;
END;
/

INSERT INTO ddl_test_employees (first_name, last_name, email, hire_date, salary) VALUES ('John', 'Doe', 'john.doe@example.com', SYSDATE, 50000);
----------------------------
-- TEST 2: CREATE TABLE with Table-Level Constraints
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_departments",
    "columns": [
      {
        "name": "department_id",
        "type": "NUMBER(4)"
      },
      {
        "name": "department_name",
        "type": "VARCHAR2(100)"
      },
      {
        "name": "location_id",
        "type": "NUMBER(4)"
      },
      {
        "name": "manager_id",
        "type": "NUMBER(6)"
      }
    ],
    "constraints": [
      {
        "name": "dept_pk",
        "type": "PRIMARY KEY",
        "columns": ["department_id"]
      },
      {
        "name": "dept_name_uk",
        "type": "UNIQUE",
        "columns": ["department_name"]
      }
    ]
  }';
  v_result NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 2: CREATE TABLE with Table-Level Constraints');
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
  -- Verify the table was created
  FOR c IN (SELECT column_name, data_type, data_length, nullable 
            FROM user_tab_columns 
            WHERE table_name = 'DDL_TEST_DEPARTMENTS'
            ORDER BY column_id) LOOP
    DBMS_OUTPUT.PUT_LINE('Column: ' || c.column_name || 
                         ', Type: ' || c.data_type || 
                         CASE WHEN c.data_length IS NOT NULL THEN '(' || c.data_length || ')' ELSE '' END ||
                         ', Nullable: ' || c.nullable);
  END LOOP;
  
  -- Verify constraints
  FOR c IN (SELECT constraint_name, constraint_type
            FROM user_constraints
            WHERE table_name = 'DDL_TEST_DEPARTMENTS'
            ORDER BY constraint_name) LOOP
    DBMS_OUTPUT.PUT_LINE('Constraint: ' || c.constraint_name || 
                         ', Type: ' || 
                         CASE c.constraint_type 
                           WHEN 'P' THEN 'PRIMARY KEY'
                           WHEN 'U' THEN 'UNIQUE'
                           WHEN 'C' THEN 'CHECK/NOT NULL'
                           ELSE c.constraint_type
                         END);
  END LOOP;
END;
/

----------------------------
-- TEST 3: Adding a Foreign Key Constraint
----------------------------
DECLARE
  v_dept_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_departments",
    "columns": [
      {
        "name": "department_id",
        "type": "NUMBER(4)",
        "constraints": ["PRIMARY KEY"]
      },
      {
        "name": "department_name",
        "type": "VARCHAR2(100)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "location_id",
        "type": "NUMBER(4)"
      },
      {
        "name": "manager_id",
        "type": "NUMBER(6)"
      }
    ]
  }';
  
  v_emp_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_employees",
    "columns": [
      {
        "name": "employee_id",
        "type": "NUMBER(6)",
        "constraints": ["PRIMARY KEY"]
      },
      {
        "name": "first_name",
        "type": "VARCHAR2(50)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "last_name",
        "type": "VARCHAR2(50)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "department_id",
        "type": "NUMBER(4)"
      }
    ],
    "constraints": [
      {
        "name": "emp_dept_fk",
        "type": "FOREIGN KEY",
        "columns": ["department_id"],
        "references": {
          "table": "ddl_test_departments",
          "columns": ["department_id"]
        }
      }
    ]
  }';
  v_result NUMBER;
BEGIN
  -- First, clean up any existing tables
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ddl_test_employees CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ddl_test_departments CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  
  DBMS_OUTPUT.PUT_LINE('TEST 3: Adding a Foreign Key Constraint');
  
  -- First, create the departments table (parent table for the FK)
  DBMS_OUTPUT.PUT_LINE('Creating parent table ddl_test_departments first...');
  SQL_GENERATOR_PKG.execute_ddl(v_dept_json, v_result);
  
  IF v_result = 1 THEN
    DBMS_OUTPUT.PUT_LINE('Parent table created successfully');
    
    -- Now create the employees table with the foreign key
    DBMS_OUTPUT.PUT_LINE('Creating child table ddl_test_employees with foreign key...');
    SQL_GENERATOR_PKG.execute_ddl(v_emp_json, v_result);
    DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
    
    -- Verify foreign key constraints
    FOR c IN (SELECT a.constraint_name, a.constraint_type, a.table_name,
                    b.column_name,
                    c.table_name as r_table_name, 
                    d.column_name as r_column_name
             FROM user_constraints a
             JOIN user_cons_columns b ON a.constraint_name = b.constraint_name
             JOIN user_constraints c ON a.r_constraint_name = c.constraint_name
             JOIN user_cons_columns d ON c.constraint_name = d.constraint_name
             WHERE a.constraint_type = 'R'
             AND a.table_name = 'DDL_TEST_EMPLOYEES') LOOP
      DBMS_OUTPUT.PUT_LINE('Foreign Key: ' || c.constraint_name || 
                          ', Column: ' || c.column_name ||
                          ', References: ' || c.r_table_name || '.' || c.r_column_name);
    END LOOP;
  ELSE
    DBMS_OUTPUT.PUT_LINE('Failed to create parent table - cannot proceed with foreign key test');
  END IF;
END;
/

----------------------------
-- TEST 4: DROP TABLE Simple
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "DROP_TABLE",
    "table": "ddl_test_employees"
  }';
  v_result NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 4: DROP TABLE Simple');
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
  -- Verify the table was dropped
  DECLARE
    v_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'DDL_TEST_EMPLOYEES';
    DBMS_OUTPUT.PUT_LINE('Table exists: ' || CASE WHEN v_count = 0 THEN 'NO' ELSE 'YES' END);
  END;
END;
/

----------------------------
-- TEST 5: DROP TABLE with CASCADE CONSTRAINTS
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "DROP_TABLE",
    "table": "ddl_test_departments",
    "cascade_constraints": true
  }';
  v_result NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 5: DROP TABLE with CASCADE CONSTRAINTS');
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
  -- Verify the table was dropped
  DECLARE
    v_count NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_count FROM user_tables WHERE table_name = 'DDL_TEST_DEPARTMENTS';
    DBMS_OUTPUT.PUT_LINE('Table exists: ' || CASE WHEN v_count = 0 THEN 'NO' ELSE 'YES' END);
  END;
END;
/

----------------------------
-- TEST 6: Create Table with More Complex Constraints
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_orders",
    "columns": [
      {
        "name": "order_id",
        "type": "NUMBER(12)",
        "constraints": ["PRIMARY KEY"]
      },
      {
        "name": "customer_id",
        "type": "NUMBER(6)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "order_date",
        "type": "DATE",
        "default": "SYSDATE"
      },
      {
        "name": "order_status",
        "type": "VARCHAR2(20)",
        "default": "''PENDING''"
      },
      {
        "name": "order_total",
        "type": "NUMBER(12,2)",
        "constraints": ["CHECK (order_total >= 0)"]
      }
    ],
    "constraints": [
      {
        "name": "ord_status_chk",
        "type": "CHECK",
        "check_condition": "order_status IN (''PENDING'', ''PROCESSING'', ''SHIPPED'', ''DELIVERED'', ''CANCELLED'')"
      }
    ]
  }';
  v_result NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 6: Create Table with More Complex Constraints');
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ddl_test_orders CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
  -- Verify check constraints
  FOR c IN (SELECT constraint_name, constraint_type, search_condition
            FROM user_constraints
            WHERE table_name = 'DDL_TEST_ORDERS'
            AND constraint_type = 'C'
            ORDER BY constraint_name) LOOP
    DBMS_OUTPUT.PUT_LINE('CHECK Constraint: ' || c.constraint_name || 
                         ', Condition: ' || c.search_condition);
  END LOOP;
  
  -- Cleanup
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ddl_test_orders CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
END;
/

----------------------------
-- TEST 7: CREATE TABLE with Auto-Generated Primary Key
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "CREATE_TABLE",
    "table": "ddl_test_products",
    "auto_primary_key": true,
    "columns": [
      {
        "name": "product_id",
        "type": "NUMBER(6)",
        "constraints": ["PRIMARY KEY"]
      },
      {
        "name": "product_name",
        "type": "VARCHAR2(100)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "price",
        "type": "NUMBER(10,2)",
        "constraints": ["NOT NULL"]
      },
      {
        "name": "description",
        "type": "VARCHAR2(500)"
      },
      {
        "name": "created_date",
        "type": "DATE",
        "default": "SYSDATE"
      }
    ]
  }';
  v_result NUMBER;
  v_table_exists NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 7: CREATE TABLE with Auto-Generated Primary Key');
  
  -- Drop the table if it exists
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE ddl_test_products CASCADE CONSTRAINTS';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  
  -- Drop the sequence if it exists
  BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE ddl_test_products_product_id_SEQ';
  EXCEPTION
    WHEN OTHERS THEN NULL;
  END;
  
  SQL_GENERATOR_PKG.execute_ddl(v_json, v_result);
  DBMS_OUTPUT.PUT_LINE('Result: ' || CASE WHEN v_result = 1 THEN 'SUCCESS' ELSE 'FAILURE' END);
  
END;
/

select * from ddl_test_products;
drop table ddl_test_products;

DECLARE
  v_json CLOB := '{
    "query_type": "INSERT",
    "table": "ddl_test_products",
    "columns": ["product_name", "price", "description", "created_date"],
    "values": [
      [
        {"value": "Product 33", "type": "VARCHAR2"},
        {"value": "10", "type": "NUMBER"},
        {"value": "Description 1", "type": "VARCHAR2"},
        {"value": "2023-01-01", "type": "DATE"}
      ]
    ]
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 1: Simple INSERT');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the insertion
  FOR r IN (SELECT * FROM dml_test_orders WHERE order_id = 1001) LOOP
    DBMS_OUTPUT.PUT_LINE('Inserted: Order ID=' || r.order_id || ', Customer=' || r.customer_id || 
                        ', Date=' || TO_CHAR(r.order_date, 'YYYY-MM-DD') || 
                        ', Amount=' || r.total_amount || ', Status=' || r.status);
  END LOOP;
END;
/

select * from ddl_test_products;
