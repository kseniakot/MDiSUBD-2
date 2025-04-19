-- Setup test environment

BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

-- Create test tables
CREATE TABLE customers (
  customer_id NUMBER PRIMARY KEY,
  name VARCHAR2(100),
  email VARCHAR2(100),
  birthdate DATE,
  registration_date TIMESTAMP,
  credit_limit NUMBER(10,2),
  is_active NUMBER(1)
);

CREATE TABLE products (
  product_id NUMBER PRIMARY KEY,
  name VARCHAR2(100),
  price NUMBER(10,2),
  category VARCHAR2(50),
  in_stock NUMBER(1)
);

CREATE TABLE orders (
  order_id NUMBER PRIMARY KEY,
  customer_id NUMBER REFERENCES customers(customer_id),
  order_date DATE,
  total_amount NUMBER(10,2),
  status VARCHAR2(20)
);

-- Insert test data
INSERT INTO customers VALUES (1, 'John Smith', 'john@example.com', TO_DATE('1980-05-15', 'YYYY-MM-DD'), 
                             SYSTIMESTAMP - 365, 5000.00, 1);
INSERT INTO customers VALUES (2, 'Jane Doe', 'jane@example.com', TO_DATE('1992-09-20', 'YYYY-MM-DD'), 
                             SYSTIMESTAMP - 180, 3000.00, 1);
INSERT INTO customers VALUES (3, 'Bob Johnson', 'bob@example.com', TO_DATE('1975-11-30', 'YYYY-MM-DD'), 
                             SYSTIMESTAMP - 730, 7500.00, 0);
INSERT INTO customers VALUES (4, 'Alice Brown', 'alice@example.com', TO_DATE('1988-03-12', 'YYYY-MM-DD'), 
                             SYSTIMESTAMP - 90, 4000.00, 1);

INSERT INTO products VALUES (101, 'Laptop', 1200.00, 'Electronics', 1);
INSERT INTO products VALUES (102, 'Smartphone', 800.00, 'Electronics', 1);
INSERT INTO products VALUES (103, 'Desk Chair', 250.00, 'Furniture', 1);
INSERT INTO products VALUES (104, 'Coffee Table', 350.00, 'Furniture', 0);
INSERT INTO products VALUES (105, 'Headphones', 150.00, 'Electronics', 1);

INSERT INTO orders VALUES (1001, 1, TO_DATE('2023-01-15', 'YYYY-MM-DD'), 1200.00, 'Completed');
INSERT INTO orders VALUES (1002, 2, TO_DATE('2023-02-20', 'YYYY-MM-DD'), 800.00, 'Completed');
INSERT INTO orders VALUES (1003, 1, TO_DATE('2023-03-10', 'YYYY-MM-DD'), 400.00, 'Processing');
INSERT INTO orders VALUES (1004, 3, TO_DATE('2023-03-15', 'YYYY-MM-DD'), 1550.00, 'Completed');
INSERT INTO orders VALUES (1005, 4, TO_DATE('2023-04-05', 'YYYY-MM-DD'), 950.00, 'Processing');
INSERT INTO orders VALUES (1006, 2, TO_DATE('2023-04-10', 'YYYY-MM-DD'), 150.00, 'Pending');
COMMIT;

----------------------------
-- TEST 1: Basic SELECT query
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["customer_id", "name", "email"],
    "tables": ["customers"],
    "where": {
      "conditions": [
        {
          "column": "is_active",
          "operator": "=",
          "value": "1",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_email VARCHAR2(100);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 1: Basic SELECT query');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name, v_email;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || ', Email: ' || v_email);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 2: JOIN operation
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["c.name", "o.order_id", "o.order_date", "o.total_amount"],
    "tables": ["customers c"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "orders o",
        "on": "c.customer_id = o.customer_id"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "o.total_amount",
          "operator": ">",
          "value": "500",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_name VARCHAR2(100);
  v_order_id NUMBER;
  v_order_date DATE;
  v_total_amount NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 2: JOIN operation');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_name, v_order_id, v_order_date, v_total_amount;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('Customer: ' || v_name || ', Order ID: ' || v_order_id || 
                        ', Date: ' || TO_CHAR(v_order_date, 'YYYY-MM-DD') || 
                        ', Amount: ' || v_total_amount);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 3: BETWEEN operator
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["product_id", "name", "price"],
    "tables": ["products"],
    "where": {
      "conditions": [
        {
          "column": "price",
          "operator": "BETWEEN",
          "value": "200",
          "value2": "900",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_price NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 3: BETWEEN operator');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name, v_price;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Product: ' || v_name || ', Price: ' || v_price);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 4: DATE handling
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["customer_id", "name", "birthdate"],
    "tables": ["customers"],
    "where": {
      "conditions": [
        {
          "column": "birthdate",
          "operator": ">=",
          "value": "1980-01-01",
          "value_type": "DATE"
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_birthdate DATE;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 4: DATE handling');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name, v_birthdate;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || 
                        ', Birthdate: ' || TO_CHAR(v_birthdate, 'YYYY-MM-DD'));
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 5: Multiple Conditions with logical operator
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["product_id", "name", "price", "category"],
    "tables": ["products"],
    "where": {
      "operator": "OR",
      "conditions": [
        {
          "column": "category",
          "operator": "=",
          "value": "Electronics"
        },
        {
          "column": "price",
          "operator": ">",
          "value": "300",
          "value_type": "NUMBER"
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_price NUMBER;
  v_category VARCHAR2(50);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 5: Multiple Conditions with OR operator');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name, v_price, v_category;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Product: ' || v_name || 
                        ', Price: ' || v_price || ', Category: ' || v_category);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 6: GROUP BY and HAVING
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["c.name", "COUNT(o.order_id) as order_count", "SUM(o.total_amount) as total_spent"],
    "tables": ["customers c"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "orders o",
        "on": "c.customer_id = o.customer_id"
      }
    ],
    "group_by": ["c.name"],
    "having": [
      {
        "column": "COUNT(o.order_id)",
        "operator": ">",
        "value": "1",
        "value_type": "NUMBER"
      }
    ]
  }';
  v_cursor SYS_REFCURSOR;
  v_name VARCHAR2(100);
  v_order_count NUMBER;
  v_total_spent NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 6: GROUP BY and HAVING');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_name, v_order_count, v_total_spent;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('Customer: ' || v_name || ', Order Count: ' || v_order_count || 
                        ', Total Spent: ' || v_total_spent);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 7: IN Subquery
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["customer_id", "name", "email"],
    "tables": ["customers"],
    "where": {
      "conditions": [
        {
          "column": "customer_id",
          "operator": "IN",
          "subquery": {
            "columns": ["customer_id"],
            "tables": ["orders"],
            "conditions": [
              {
                "column": "total_amount",
                "operator": ">",
                "value": "1000",
                "value_type": "NUMBER"
              }
            ]
          }
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_email VARCHAR2(100);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 7: IN Subquery');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name, v_email;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || ', Email: ' || v_email);
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 8: EXISTS Subquery
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["customer_id", "name"],
    "tables": ["customers c"],
    "where": {
      "conditions": [
        {
          "column": "EXISTS",
          "operator": "",
          "subquery": {
            "columns": ["1"],
            "tables": ["orders o"],
            "conditions": [
              {
                "column": "o.customer_id",
                "operator": "=",
                "value": "c.customer_id",
                "value_type": "IDENTIFIER"
              },
              {
                "column": "o.status",
                "operator": "=",
                "value": "Processing"
              }
            ]
          }
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 8: EXISTS Subquery');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || 
                        ' (has processing orders)');
  END LOOP;
  
  CLOSE v_cursor;
END;
/

----------------------------
-- TEST 9: NOT EXISTS Subquery
----------------------------
DECLARE
  v_json CLOB := '{
    "columns": ["customer_id", "name"],
    "tables": ["customers c"],
    "where": {
      "conditions": [
        {
          "column": "NOT EXISTS",
          "operator": "",
          "subquery": {
            "columns": ["1"],
            "tables": ["orders o"],
            "conditions": [
              {
                "column": "o.customer_id",
                "operator": "=",
                "value": "c.customer_id",
                "value_type": "IDENTIFIER"
              },
              {
                "column": "o.status",
                "operator": "=",
                "value": "Pending"
              }
            ]
          }
        }
      ]
    }
  }';
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 9: NOT EXISTS Subquery');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);
  
  LOOP
    FETCH v_cursor INTO v_id, v_name;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || ' (has no pending orders)');
  END LOOP;
  
  CLOSE v_cursor;
END;
/
----------------------------
-- TEST 10: UNION ALL
----------------------------

DECLARE
  v_json CLOB := '{
    "columns": ["c.customer_id", "c.name", "o.status"],
    "tables": ["customers c"],
    "joins": [
      {
        "type": "INNER JOIN",
        "table": "orders o",
        "on": "c.customer_id = o.customer_id"
      }
    ],
    "where": {
      "conditions": [
        {
          "column": "o.status",
          "operator": "=",
          "value": "Processing",
          "value_type": "VARCHAR2"
        }
      ]
    },
    "union_type": "UNION ALL",
    "union_parts": [
      {
        "columns": ["c.customer_id", "c.name", "o.status"],
        "tables": ["customers c"],
        "joins": [
          {
            "type": "INNER JOIN",
            "table": "orders o",
            "on": "c.customer_id = o.customer_id"
          }
        ],
        "where": {
          "conditions": [
            {
              "column": "o.status",
              "operator": "=",
              "value": "Pending",
              "value_type": "VARCHAR2"
            }
          ]
        }
      }
    ]
  }';
  
  v_cursor SYS_REFCURSOR;
  v_id NUMBER;
  v_name VARCHAR2(100);
  v_status VARCHAR2(20);
BEGIN
  DBMS_OUTPUT.PUT_LINE('TEST 10: UNION ALL');
  v_cursor := SQL_GENERATOR_PKG.json_select_handler(v_json);

  LOOP
    FETCH v_cursor INTO v_id, v_name, v_status;
    EXIT WHEN v_cursor%NOTFOUND;
    DBMS_OUTPUT.PUT_LINE('ID: ' || v_id || ', Name: ' || v_name || ', Status: ' || v_status);
  END LOOP;

  CLOSE v_cursor;
END;
/
