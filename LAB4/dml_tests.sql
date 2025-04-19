-- Setup test environment
-- Clean up any existing test objects
BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_test_orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_test_customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_test_products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE dml_test_backup CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

-- Create test tables
CREATE TABLE dml_test_customers (
  customer_id NUMBER PRIMARY KEY,
  name VARCHAR2(100),
  email VARCHAR2(100),
  credit_limit NUMBER(10,2),
  active NUMBER(1)
);

CREATE TABLE dml_test_products (
  product_id NUMBER PRIMARY KEY,
  name VARCHAR2(100),
  price NUMBER(10,2),
  category VARCHAR2(50),
  in_stock NUMBER(1)
);

CREATE TABLE dml_test_orders (
  order_id NUMBER PRIMARY KEY,
  customer_id NUMBER REFERENCES dml_test_customers(customer_id),
  order_date DATE,
  total_amount NUMBER(10,2),
  status VARCHAR2(20)
);

-- Create backup table for testing INSERT...SELECT
CREATE TABLE dml_test_backup (
  customer_id NUMBER,
  name VARCHAR2(100),
  email VARCHAR2(100)
);

-- Insert initial data
INSERT INTO dml_test_customers VALUES (1, 'John Smith', 'john@example.com', 5000.00, 1);
INSERT INTO dml_test_customers VALUES (2, 'Jane Doe', 'jane@example.com', 3000.00, 1);
INSERT INTO dml_test_customers VALUES (3, 'Bob Johnson', 'bob@example.com', 7500.00, 0);

INSERT INTO dml_test_products VALUES (101, 'Laptop', 1200.00, 'Electronics', 1);
INSERT INTO dml_test_products VALUES (102, 'Smartphone', 800.00, 'Electronics', 1);
INSERT INTO dml_test_products VALUES (103, 'Desk Chair', 250.00, 'Furniture', 1);

COMMIT;

----------------------------
-- TEST 1: Simple INSERT
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "INSERT",
    "table": "dml_test_orders",
    "columns": ["order_id", "customer_id", "order_date", "total_amount", "status"],
    "values": [
      [
        {"value": "1001", "type": "NUMBER"},
        {"value": "1", "type": "NUMBER"},
        {"value": "2023-05-15", "type": "DATE"},
        {"value": "1200.00", "type": "NUMBER"},
        {"value": "Completed"}
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

----------------------------
-- TEST 2: INSERT with SELECT
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "INSERT",
    "table": "dml_test_backup",
    "columns": ["customer_id", "name", "email"],
    "values_type": "SELECT",
    "select_query": {
      "columns": ["customer_id", "name", "email"],
      "tables": ["dml_test_customers"],
      "where": {
        "conditions": [
          {
            "column": "active",
            "operator": "=",
            "value": "1",
            "value_type": "NUMBER"
          }
        ]
      }
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 2: INSERT with SELECT');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the insertion
  DBMS_OUTPUT.PUT_LINE('Backup table contents:');
  FOR r IN (SELECT * FROM dml_test_backup ORDER BY customer_id) LOOP
    DBMS_OUTPUT.PUT_LINE('Customer ID=' || r.customer_id || ', Name=' || r.name || ', Email=' || r.email);
  END LOOP;
END;
/

----------------------------
-- TEST 3: INSERT with Subquery Value
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "INSERT",
    "table": "dml_test_orders",
    "columns": ["order_id", "customer_id", "order_date", "total_amount", "status"],
    "values": [
      [
        {"value": "1003", "type": "NUMBER"},
        {
          "subquery": {
            "columns": ["customer_id"],
            "tables": ["dml_test_customers"],
            "conditions": [
              {
                "column": "name",
                "operator": "=",
                "value": "Jane Doe"
              }
            ]
          }
        },
        {"value": "2023-06-10", "type": "DATE"},
        {"value": "800.00", "type": "NUMBER"},
        {"value": "Processing"}
      ]
    ]
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 3: INSERT with Subquery Value');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the insertion
  FOR r IN (SELECT o.*, c.name as customer_name 
            FROM dml_test_orders o 
            JOIN dml_test_customers c ON o.customer_id = c.customer_id
            WHERE o.order_id = 1003) LOOP
    DBMS_OUTPUT.PUT_LINE('Inserted: Order ID=' || r.order_id || ', Customer=' || r.customer_name || 
                        ', Date=' || TO_CHAR(r.order_date, 'YYYY-MM-DD') || 
                        ', Amount=' || r.total_amount || ', Status=' || r.status);
  END LOOP;
END;
/

----------------------------
-- TEST 4: UPDATE with Direct Values
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "UPDATE",
    "table": "dml_test_customers",
    "set_values": [
      {
        "column": "credit_limit",
        "value": "6000.00",
        "value_type": "NUMBER"
      },
      {
        "column": "email",
        "value": "john.smith@example.com"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "customer_id",
          "operator": "=",
          "value": "1",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 4: UPDATE with Direct Values');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the update
  FOR r IN (SELECT * FROM dml_test_customers WHERE customer_id = 1) LOOP
    DBMS_OUTPUT.PUT_LINE('Updated: Customer ID=' || r.customer_id || ', Name=' || r.name || 
                        ', Email=' || r.email || ', Credit Limit=' || r.credit_limit);
  END LOOP;
END;
/

----------------------------
-- TEST 5: UPDATE with Subquery Value
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "UPDATE",
    "table": "dml_test_orders",
    "set_values": [
      {
        "column": "total_amount",
        "subquery": {
          "columns": ["price"],
          "tables": ["dml_test_products"],
          "conditions": [
            {
              "column": "product_id",
              "operator": "=",
              "value": "102",
              "value_type": "NUMBER"
            }
          ]
        }
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "order_id",
          "operator": "=",
          "value": "1002",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 5: UPDATE with Subquery Value');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the update
  FOR r IN (SELECT o.*, p.price 
            FROM dml_test_orders o, dml_test_products p 
            WHERE o.order_id = 1002 AND p.product_id = 102) LOOP
    DBMS_OUTPUT.PUT_LINE('Updated: Order ID=' || r.order_id || 
                        ', New Total Amount=' || r.total_amount || 
                        ' (Matches Product Price=' || r.price || ')');
  END LOOP;
END;
/

----------------------------
-- TEST 6: DELETE with Simple Condition
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "DELETE",
    "table": "dml_test_orders",
    "where": {
      "conditions": [
        {
          "column": "order_id",
          "operator": "=",
          "value": "1001",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 6: DELETE with Simple Condition');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the deletion
  FOR r IN (SELECT COUNT(*) as count FROM dml_test_orders WHERE order_id = 1001) LOOP
    DBMS_OUTPUT.PUT_LINE('Remaining records with order_id=1001: ' || r.count);
  END LOOP;
END;
/

----------------------------
-- TEST 7: DELETE with Subquery Condition
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "DELETE",
    "table": "dml_test_orders",
    "where": {
      "conditions": [
        {
          "column": "customer_id",
          "operator": "IN",
          "subquery": {
            "columns": ["customer_id"],
            "tables": ["dml_test_customers"],
            "conditions": [
              {
                "column": "active",
                "operator": "=",
                "value": "0",
                "value_type": "NUMBER"
              }
            ]
          }
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  -- First, insert an order for inactive customer
  EXECUTE IMMEDIATE 'INSERT INTO dml_test_orders VALUES (1003, 3, SYSDATE, 500, ''Pending'')';
  
  DBMS_OUTPUT.PUT_LINE('TEST 7: DELETE with Subquery Condition');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify no orders remain for inactive customers
  FOR r IN (
    SELECT COUNT(*) as count 
    FROM dml_test_orders o 
    JOIN dml_test_customers c ON o.customer_id = c.customer_id
    WHERE c.active = 0
  ) LOOP
    DBMS_OUTPUT.PUT_LINE('Remaining orders for inactive customers: ' || r.count);
  END LOOP;
END;
/



----------------------------
-- TEST 9: UPDATE with BETWEEN condition
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "UPDATE",
    "table": "dml_test_products",
    "set_values": [
      {
        "column": "price",
        "value": "price * 0.9",
        "value_type": "IDENTIFIER"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "price",
          "operator": "BETWEEN",
          "value": "50",
          "value2": "100",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 9: UPDATE with BETWEEN condition');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the update
  DBMS_OUTPUT.PUT_LINE('Products with price between 50 and 100 (now discounted):');
  FOR r IN (SELECT * FROM dml_test_products 
            WHERE price BETWEEN 45 AND 90
            ORDER BY product_id) LOOP
    DBMS_OUTPUT.PUT_LINE('Product ID=' || r.product_id || ', Name=' || r.name || 
                        ', Discounted Price=' || r.price);
  END LOOP;
END;
/

----------------------------
-- TEST 10: DELETE with EXISTS Subquery
----------------------------
DECLARE
  v_json CLOB := '{
    "query_type": "DELETE",
    "table": "dml_test_customers",
    "where": {
      "conditions": [
        {
          "column": "EXISTS",
          "operator": "",
          "subquery": {
            "columns": ["1"],
            "tables": ["dml_test_backup b"],
            "conditions": [
              {
                "column": "b.customer_id",
                "operator": "=",
                "value": "dml_test_customers.customer_id",
                "value_type": "IDENTIFIER"
              }
            ]
          }
        }
      ]
    }
  }';
  v_rows_affected NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 10: DELETE with EXISTS Subquery');
  SQL_GENERATOR_PKG.execute_dml(v_json, v_rows_affected);
  DBMS_OUTPUT.PUT_LINE('Rows affected: ' || v_rows_affected);
  
  -- Verify the deletion
  DBMS_OUTPUT.PUT_LINE('Remaining customers:');
  FOR r IN (SELECT * FROM dml_test_customers ORDER BY customer_id) LOOP
    DBMS_OUTPUT.PUT_LINE('Customer ID=' || r.customer_id || ', Name=' || r.name || 
                        ', Email=' || r.email);
  END LOOP;
END;
/




