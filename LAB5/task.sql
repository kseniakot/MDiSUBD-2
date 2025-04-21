BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders_history CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE history_id_seq'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

-- Sequence for history tables
CREATE SEQUENCE history_id_seq
  START WITH 1
  INCREMENT BY 1
  NOCACHE
  NOCYCLE;


CREATE TABLE customers (
  customer_id     NUMBER(6)      PRIMARY KEY,
  customer_name   VARCHAR2(100)  NOT NULL,
  registered_at   DATE           DEFAULT SYSDATE
);


CREATE TABLE products (
  product_id      NUMBER(6)      PRIMARY KEY,
  product_name    VARCHAR2(100)  NOT NULL,
  price           NUMBER(10, 2)  NOT NULL
);


CREATE TABLE orders (
  order_id        NUMBER(10)     PRIMARY KEY,
  customer_id     NUMBER(6),
  product_id      NUMBER(6),
  order_date      DATE           DEFAULT SYSDATE,
  quantity        NUMBER(5),

  CONSTRAINT fk_order_customer FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id),
  
  CONSTRAINT fk_order_product FOREIGN KEY (product_id)
    REFERENCES products(product_id)
);

-- History tables to track changes
CREATE TABLE customers_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,  -- 'INSERT', 'UPDATE', 'DELETE'
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  customer_id     NUMBER(6)      NOT NULL,
  customer_name   VARCHAR2(100),
  old_name        VARCHAR2(100),  
  registered_at   DATE,
  old_reg_date    DATE,
  rolled_back     NUMBER(1)      DEFAULT 0  -- Flag to indicate if operation was rolled back
);

CREATE TABLE products_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  product_id      NUMBER(6)      NOT NULL,
  product_name    VARCHAR2(100),
  old_name        VARCHAR2(100),  
  price           NUMBER(10, 2),
  old_price       NUMBER(10, 2),
  rolled_back     NUMBER(1)      DEFAULT 0  -- Flag to indicate if operation was rolled back
);

CREATE TABLE orders_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  order_id        NUMBER(10)     NOT NULL,
  customer_id     NUMBER(6),
  old_customer_id NUMBER(6),      
  product_id      NUMBER(6),
  old_product_id  NUMBER(6),      
  order_date      DATE,
  old_order_date  DATE,           
  quantity        NUMBER(5),
  old_quantity    NUMBER(5),
  rolled_back     NUMBER(1)      DEFAULT 0  -- Flag to indicate if operation was rolled back
);

-- Triggers to track all DML operations


CREATE OR REPLACE TRIGGER customers_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON customers
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO customers_history (
    history_id, operation_type, customer_id, 
    customer_name, old_name, 
    registered_at, old_reg_date
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.customer_id
      ELSE :NEW.customer_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.customer_name
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.customer_name
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.registered_at
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.registered_at
    END
  );
END;
/


CREATE OR REPLACE TRIGGER products_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON products
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO products_history (
    history_id, operation_type, product_id, 
    product_name, old_name, 
    price, old_price
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.product_id
      ELSE :NEW.product_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.product_name
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.product_name
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.price
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.price
    END
  );
END;
/


CREATE OR REPLACE TRIGGER orders_audit_trg
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW
DECLARE
  v_operation VARCHAR2(10);
BEGIN
  IF INSERTING THEN
    v_operation := 'INSERT';
  ELSIF UPDATING THEN
    v_operation := 'UPDATE';
  ELSIF DELETING THEN
    v_operation := 'DELETE';
  END IF;
  
  INSERT INTO orders_history (
    history_id, operation_type, order_id, 
    customer_id, old_customer_id,
    product_id, old_product_id,
    order_date, old_order_date,
    quantity, old_quantity
  ) VALUES (
    history_id_seq.NEXTVAL, v_operation,
    CASE 
      WHEN v_operation = 'DELETE' THEN :OLD.order_id
      ELSE :NEW.order_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.customer_id
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.customer_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.product_id
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.product_id
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.order_date
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.order_date
    END,
    CASE 
      WHEN v_operation = 'DELETE' THEN NULL  
      ELSE :NEW.quantity
    END,
    CASE 
      WHEN v_operation = 'INSERT' THEN NULL 
      ELSE :OLD.quantity
    END
  );
END;
/


CREATE OR REPLACE PACKAGE history_mgmt AS
  
  PROCEDURE rollback_to(p_timestamp IN TIMESTAMP);
  PROCEDURE rollback_to(p_milliseconds IN NUMBER);
  PROCEDURE show_changes_after(p_timestamp IN TIMESTAMP);
  
END history_mgmt;
/

CREATE OR REPLACE PACKAGE BODY history_mgmt AS

  PROCEDURE do_rollback(p_timestamp IN TIMESTAMP) IS
    v_exists NUMBER;
  BEGIN
    
    SAVEPOINT rollback_start; -- Start transaction

    -- Disable triggers to avoid recursive history recording
    EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg DISABLE';
    EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg DISABLE';
    
    BEGIN
      -- Process changes in reverse chronological order (newest to oldest)
      -- This ensures we undo operations in the correct sequence

      FOR r_cust IN (
        SELECT history_id, customer_id, customer_name, registered_at, old_name, old_reg_date, operation_type, change_time
        FROM customers_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        -- Undo the operation that was done
        CASE r_cust.operation_type
          WHEN 'INSERT' THEN
            -- If it was an insert after our timestamp, we need to delete it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting customer ' || r_cust.customer_id);
                DELETE FROM customers WHERE customer_id = r_cust.customer_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            -- If it was updated after our timestamp, restore the previous values
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring customer ' || r_cust.customer_id || ' to previous state');
                UPDATE customers
                SET customer_name = r_cust.old_name,
                    registered_at = r_cust.old_reg_date
                WHERE customer_id = r_cust.customer_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            -- If it was deleted after our timestamp, restore it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring customer ' || r_cust.customer_id);
                INSERT INTO customers (customer_id, customer_name, registered_at)
                VALUES (r_cust.customer_id, r_cust.old_name, r_cust.old_reg_date);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing customer delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE customers_history SET rolled_back = 1 WHERE history_id = r_cust.history_id;
      END LOOP;

      -- Next handle products
      FOR r_prod IN (
        SELECT history_id, product_id, product_name, price, old_name, old_price, operation_type, change_time
        FROM products_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        -- Undo the operation that was done
        CASE r_prod.operation_type
          WHEN 'INSERT' THEN
            -- If it was an insert after our timestamp, we need to delete it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting product ' || r_prod.product_id);
                DELETE FROM products WHERE product_id = r_prod.product_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            -- If it was updated after our timestamp, restore the previous values
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring product ' || r_prod.product_id || ' to previous state');
                UPDATE products
                SET product_name = r_prod.old_name,
                    price = r_prod.old_price
                WHERE product_id = r_prod.product_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            -- If it was deleted after our timestamp, restore it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring product ' || r_prod.product_id);
                INSERT INTO products (product_id, product_name, price)
                VALUES (r_prod.product_id, r_prod.old_name, r_prod.old_price);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing product delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE products_history SET rolled_back = 1 WHERE history_id = r_prod.history_id;
      END LOOP;

      -- orders (they depend on customers and products)
      FOR r_ord IN (
        SELECT history_id, order_id, customer_id, product_id, order_date, quantity, operation_type, change_time, old_quantity, old_order_date, old_product_id, old_customer_id
        FROM orders_history
        WHERE change_time > p_timestamp
        ORDER BY change_time DESC
      ) LOOP
        -- Undo the operation that was done
        CASE r_ord.operation_type
          WHEN 'INSERT' THEN
            -- If it was an insert after our timestamp, we need to delete it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing INSERT: Deleting order ' || r_ord.order_id);
                DELETE FROM orders WHERE order_id = r_ord.order_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order insert: ' || SQLERRM);
            END;
            
          WHEN 'UPDATE' THEN
            -- If it was updated after our timestamp, restore the previous values
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists > 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing UPDATE: Restoring order ' || r_ord.order_id || ' to previous state');
                UPDATE orders
                SET customer_id = r_ord.old_customer_id,
                    product_id = r_ord.old_product_id,
                    order_date = r_ord.old_order_date,
                    quantity = r_ord.old_quantity
                WHERE order_id = r_ord.order_id;
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order update: ' || SQLERRM);
            END;
            
          WHEN 'DELETE' THEN
            -- If it was deleted after our timestamp, restore it
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
              IF v_exists = 0 THEN
                DBMS_OUTPUT.PUT_LINE('Undoing DELETE: Restoring order ' || r_ord.order_id);
                INSERT INTO orders (order_id, customer_id, product_id, order_date, quantity)
                VALUES (r_ord.order_id, r_ord.old_customer_id, r_ord.old_product_id, r_ord.old_order_date, r_ord.old_quantity);
              END IF;
            EXCEPTION WHEN OTHERS THEN
              DBMS_OUTPUT.PUT_LINE('Error undoing order delete: ' || SQLERRM);
            END;
        END CASE;
        UPDATE orders_history SET rolled_back = 1 WHERE history_id = r_ord.history_id;
      END LOOP;


      -- Re-enable triggers
      EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg ENABLE';
      EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg ENABLE';
      EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg ENABLE';
      
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Rollback to ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3') || ' completed successfully.');
      
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK TO rollback_start;
        -- Re-enable triggers even if failure
        EXECUTE IMMEDIATE 'ALTER TRIGGER customers_audit_trg ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER products_audit_trg ENABLE';
        EXECUTE IMMEDIATE 'ALTER TRIGGER orders_audit_trg ENABLE';
        DBMS_OUTPUT.PUT_LINE('Error during rollback: ' || SQLERRM);
        RAISE;
    END;
  END do_rollback;

  -- Implementation of rollback to timestamp
  PROCEDURE rollback_to(p_timestamp IN TIMESTAMP) IS
  BEGIN
    do_rollback(p_timestamp);
  END rollback_to;
  
  -- Implementation of rollback by milliseconds
  PROCEDURE rollback_to(p_milliseconds IN NUMBER) IS
    v_timestamp TIMESTAMP;
  BEGIN
    -- Calculate the timestamp p_milliseconds ago
    SELECT SYSTIMESTAMP - NUMTODSINTERVAL(p_milliseconds/1000, 'SECOND') 
    INTO v_timestamp 
    FROM DUAL;
    
    DBMS_OUTPUT.PUT_LINE('Rolling back to ' || TO_CHAR(v_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
                         ' (' || p_milliseconds || ' milliseconds ago)');
    
    do_rollback(v_timestamp);
  END rollback_to;
  
  -- Procedure to show changes made after a specific timestamp
  PROCEDURE show_changes_after(p_timestamp IN TIMESTAMP) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('Changes to Customers after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR c IN (
      SELECT operation_type, change_time, customer_id, customer_name, old_name, registered_at, old_reg_date
      FROM customers_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        c.operation_type || ' at ' || TO_CHAR(c.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | ID: ' || c.customer_id || 
        ' | Name: ' || COALESCE(c.customer_name, 'NULL') || 
        ' | Old Name: ' || COALESCE(c.old_name, 'NULL') ||
        ' | Reg Date: ' || COALESCE(TO_CHAR(c.registered_at, 'YYYY-MM-DD'), 'NULL') ||
        ' | Old Reg Date: ' || COALESCE(TO_CHAR(c.old_reg_date, 'YYYY-MM-DD'), 'NULL')
      );
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Changes to Products after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR p IN (
      SELECT operation_type, change_time, product_id, product_name, old_name, price, old_price
      FROM products_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        p.operation_type || ' at ' || TO_CHAR(p.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | ID: ' || p.product_id || 
        ' | Name: ' || COALESCE(p.product_name, 'NULL') || 
        ' | Old Name: ' || COALESCE(p.old_name, 'NULL') ||
        ' | Price: ' || COALESCE(TO_CHAR(p.price), 'NULL') ||
        ' | Old Price: ' || COALESCE(TO_CHAR(p.old_price), 'NULL')
      );
    END LOOP;
    
    DBMS_OUTPUT.PUT_LINE(CHR(10) || 'Changes to Orders after ' || TO_CHAR(p_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3'));
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    
    FOR o IN (
      SELECT operation_type, change_time, order_id, 
             customer_id, old_customer_id, 
             product_id, old_product_id, 
             order_date, old_order_date,
             quantity, old_quantity
      FROM orders_history
      WHERE change_time > p_timestamp
      ORDER BY change_time
    ) LOOP
      DBMS_OUTPUT.PUT_LINE(
        o.operation_type || ' at ' || TO_CHAR(o.change_time, 'YYYY-MM-DD HH24:MI:SS.FF3') || 
        ' | Order ID: ' || o.order_id || 
        ' | Customer: ' || COALESCE(TO_CHAR(o.customer_id), 'NULL') || 
        ' | Old Customer: ' || COALESCE(TO_CHAR(o.old_customer_id), 'NULL') ||
        ' | Product: ' || COALESCE(TO_CHAR(o.product_id), 'NULL') ||
        ' | Old Product: ' || COALESCE(TO_CHAR(o.old_product_id), 'NULL') ||
        ' | Qty: ' || COALESCE(TO_CHAR(o.quantity), 'NULL') ||
        ' | Old Qty: ' || COALESCE(TO_CHAR(o.old_quantity), 'NULL')
      );
    END LOOP;
  END show_changes_after;
  
END history_mgmt;
/

---------------------------
--Report generation
--------------------------

-- Create a table to track report generation times
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE report_tracking';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE report_tracking (
  last_report_time TIMESTAMP,
  report_count NUMBER DEFAULT 0
);

-- Insert initial record
INSERT INTO report_tracking (last_report_time, report_count) 
VALUES (SYSTIMESTAMP, 0);

COMMIT;
/

-- Create or replace the directory object with proper path format
BEGIN
  EXECUTE IMMEDIATE 'DROP DIRECTORY MY_DIR';
EXCEPTION
  WHEN OTHERS THEN NULL;
END;
/

CREATE OR REPLACE DIRECTORY MY_DIR AS '/opt/oracle/reports';

CREATE OR REPLACE PROCEDURE generate_changes_report(
  p_since_timestamp IN TIMESTAMP DEFAULT NULL,
  p_file_path IN VARCHAR2,
  p_include_details IN BOOLEAN DEFAULT FALSE
) AS
  v_file UTL_FILE.FILE_TYPE;
  v_start_time TIMESTAMP;
  v_end_time TIMESTAMP := SYSTIMESTAMP;
  v_report_num NUMBER;
  v_filename VARCHAR2(255);
  
  -- Operation counts for each table
  v_cust_inserts NUMBER := 0;
  v_cust_updates NUMBER := 0;
  v_cust_deletes NUMBER := 0;
  v_prod_inserts NUMBER := 0;
  v_prod_updates NUMBER := 0;
  v_prod_deletes NUMBER := 0;
  v_order_inserts NUMBER := 0;
  v_order_updates NUMBER := 0;
  v_order_deletes NUMBER := 0;
  
  -- Helper function to write HTML content safely
  PROCEDURE write_html(p_content IN VARCHAR2) IS
  BEGIN
    UTL_FILE.PUT_LINE(v_file, p_content);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('Error writing to file: ' || SQLERRM);
      RAISE;
  END write_html;
  
BEGIN
  -- Determine the starting timestamp
  IF p_since_timestamp IS NULL THEN
    SELECT last_report_time, report_count+1
    INTO v_start_time, v_report_num
    FROM report_tracking;
  ELSE
    v_start_time := p_since_timestamp;
    
    SELECT report_count+1
    INTO v_report_num
    FROM report_tracking;
  END IF;
  
  -- Count operations for customers table
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_cust_inserts, v_cust_updates, v_cust_deletes
  FROM customers_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  -- Count operations for products table
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_prod_inserts, v_prod_updates, v_prod_deletes
  FROM products_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  -- Count operations for orders table
  SELECT 
    COUNT(CASE WHEN operation_type = 'INSERT' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'UPDATE' AND rolled_back = 0 THEN 1 END),
    COUNT(CASE WHEN operation_type = 'DELETE' AND rolled_back = 0 THEN 1 END)
  INTO v_order_inserts, v_order_updates, v_order_deletes
  FROM orders_history
  WHERE change_time BETWEEN v_start_time AND v_end_time;
  
  v_filename := p_file_path;
  
  -- Generate HTML report
  BEGIN
  
    BEGIN
      DBMS_OUTPUT.PUT_LINE('Attempting to open file: ' || v_filename || ' in directory MY_DIR');
      v_file := UTL_FILE.FOPEN('MY_DIR', v_filename, 'W', 32767);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error opening file: ' || SQLERRM || ' - ' || SQLCODE);
        RAISE;
    END;
    
    -- HTML Header
    write_html('<!DOCTYPE html>');
    write_html('<html lang="en">');
    write_html('<head>');
    write_html('  <meta charset="UTF-8">');
    write_html('  <title>Database Changes Report #' || v_report_num || '</title>');
    write_html('  <style>');
    write_html('    body { font-family: Arial, sans-serif; margin: 20px; }');
    write_html('    table { border-collapse: collapse; width: 100%; }');
    write_html('    th, td { border: 1px solid #ddd; padding: 8px; }');
    write_html('    th { background-color: #f2f2f2; }');
    write_html('    .insert { color: green; }');
    write_html('    .update { color: blue; }');
    write_html('    .delete { color: red; }');
    write_html('  </style>');
    write_html('</head>');
    write_html('<body>');
    
    -- Report header
    write_html('  <h1>Database Changes Report #' || v_report_num || '</h1>');
    write_html('  <p>');
    write_html('    Report generated: ' || TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS') || '<br>');
    write_html('    Period covered: ' || TO_CHAR(v_start_time, 'YYYY-MM-DD HH24:MI:SS') || 
               ' to ' || TO_CHAR(v_end_time, 'YYYY-MM-DD HH24:MI:SS'));
    write_html('  </p>');
    
    -- Summary section
    write_html('  <h2>Summary of Changes</h2>');
    write_html('  <table>');
    write_html('    <tr>');
    write_html('      <th>Table</th>');
    write_html('      <th>Inserts</th>');
    write_html('      <th>Updates</th>');
    write_html('      <th>Deletes</th>');
    write_html('      <th>Total</th>');
    write_html('    </tr>');
    
    -- Customers summary
    write_html('    <tr>');
    write_html('      <td>Customers</td>');
    write_html('      <td class="insert">' || v_cust_inserts || '</td>');
    write_html('      <td class="update">' || v_cust_updates || '</td>');
    write_html('      <td class="delete">' || v_cust_deletes || '</td>');
    write_html('      <td>' || (v_cust_inserts + v_cust_updates + v_cust_deletes) || '</td>');
    write_html('    </tr>');
    
    -- Products summary
    write_html('    <tr>');
    write_html('      <td>Products</td>');
    write_html('      <td class="insert">' || v_prod_inserts || '</td>');
    write_html('      <td class="update">' || v_prod_updates || '</td>');
    write_html('      <td class="delete">' || v_prod_deletes || '</td>');
    write_html('      <td>' || (v_prod_inserts + v_prod_updates + v_prod_deletes) || '</td>');
    write_html('    </tr>');
    
    -- Orders summary
    write_html('    <tr>');
    write_html('      <td>Orders</td>');
    write_html('      <td class="insert">' || v_order_inserts || '</td>');
    write_html('      <td class="update">' || v_order_updates || '</td>');
    write_html('      <td class="delete">' || v_order_deletes || '</td>');
    write_html('      <td>' || (v_order_inserts + v_order_updates + v_order_deletes) || '</td>');
    write_html('    </tr>');
    
    -- Total row
    write_html('    <tr>');
    write_html('      <td><strong>Total</strong></td>');
    write_html('      <td class="insert"><strong>' || (v_cust_inserts + v_prod_inserts + v_order_inserts) || '</strong></td>');
    write_html('      <td class="update"><strong>' || (v_cust_updates + v_prod_updates + v_order_updates) || '</strong></td>');
    write_html('      <td class="delete"><strong>' || (v_cust_deletes + v_prod_deletes + v_order_deletes) || '</strong></td>');
    write_html('      <td><strong>' || 
              (v_cust_inserts + v_cust_updates + v_cust_deletes +
               v_prod_inserts + v_prod_updates + v_prod_deletes +
               v_order_inserts + v_order_updates + v_order_deletes) || '</strong></td>');
    write_html('    </tr>');
    write_html('  </table>');
    
    -- Detailed changes if requested
    IF p_include_details THEN
      -- Customer details
      IF (v_cust_inserts + v_cust_updates + v_cust_deletes) > 0 THEN
        write_html('  <h2>Customer Changes</h2>');
        write_html('  <table>');
        write_html('    <tr>');
        write_html('      <th>Time</th>');
        write_html('      <th>Operation</th>');
        write_html('      <th>ID</th>');
        write_html('      <th>Name</th>');
        write_html('      <th>Old Name</th>');
        write_html('    </tr>');
        
        FOR c IN (
          SELECT operation_type, change_time, customer_id, customer_name, old_name
          FROM customers_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
          write_html('    <tr>');
          write_html('      <td>' || TO_CHAR(c.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('      <td class="' || LOWER(c.operation_type) || '">' || c.operation_type || '</td>');
          write_html('      <td>' || c.customer_id || '</td>');
          write_html('      <td>' || COALESCE(c.customer_name, 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(c.old_name, 'NULL') || '</td>');
          write_html('    </tr>');
        END LOOP;
        
        write_html('  </table>');
      END IF;
      
      -- Product details
      IF (v_prod_inserts + v_prod_updates + v_prod_deletes) > 0 THEN
        write_html('  <h2>Product Changes</h2>');
        write_html('  <table>');
        write_html('    <tr>');
        write_html('      <th>Time</th>');
        write_html('      <th>Operation</th>');
        write_html('      <th>ID</th>');
        write_html('      <th>Name</th>');
        write_html('      <th>Old Name</th>');
        write_html('      <th>Price</th>');
        write_html('      <th>Old Price</th>');
        write_html('    </tr>');
        
        FOR p IN (
          SELECT operation_type, change_time, product_id, product_name, old_name, price, old_price
          FROM products_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
          write_html('    <tr>');
          write_html('      <td>' || TO_CHAR(p.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('      <td class="' || LOWER(p.operation_type) || '">' || p.operation_type || '</td>');
          write_html('      <td>' || p.product_id || '</td>');
          write_html('      <td>' || COALESCE(p.product_name, 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(p.old_name, 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(p.price), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(p.old_price), 'NULL') || '</td>');
          write_html('    </tr>');
        END LOOP;
        
        write_html('  </table>');
      END IF;
      
      -- Order details
      IF (v_order_inserts + v_order_updates + v_order_deletes) > 0 THEN
        write_html('  <h2>Order Changes</h2>');
        write_html('  <table>');
        write_html('    <tr>');
        write_html('      <th>Time</th>');
        write_html('      <th>Operation</th>');
        write_html('      <th>Order ID</th>');
        write_html('      <th>Customer</th>');
        write_html('      <th>Old Customer</th>');
        write_html('      <th>Product</th>');
        write_html('      <th>Old Product</th>');
        write_html('      <th>Qty</th>');
        write_html('      <th>Old Qty</th>');
        write_html('    </tr>');
        
        FOR o IN (
          SELECT operation_type, change_time, order_id, 
                 customer_id, old_customer_id, 
                 product_id, old_product_id,
                 quantity, old_quantity
          FROM orders_history
          WHERE change_time BETWEEN v_start_time AND v_end_time AND rolled_back = 0
          ORDER BY change_time
        ) LOOP
          write_html('    <tr>');
          write_html('      <td>' || TO_CHAR(o.change_time, 'YYYY-MM-DD HH24:MI:SS') || '</td>');
          write_html('      <td class="' || LOWER(o.operation_type) || '">' || o.operation_type || '</td>');
          write_html('      <td>' || o.order_id || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.customer_id), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.old_customer_id), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.product_id), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.old_product_id), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.quantity), 'NULL') || '</td>');
          write_html('      <td>' || COALESCE(TO_CHAR(o.old_quantity), 'NULL') || '</td>');
          write_html('    </tr>');
        END LOOP;
        
        write_html('  </table>');
      END IF;
    END IF;
    
    -- HTML Footer
    write_html('</body>');
    write_html('</html>');
    
    -- Close the file
    UTL_FILE.FCLOSE(v_file);
    
    -- Update the last report time
    UPDATE report_tracking
    SET last_report_time = v_end_time,
        report_count = v_report_num;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('Report #' || v_report_num || ' generated successfully at: ' || p_file_path);
    
  EXCEPTION
    WHEN OTHERS THEN
      IF UTL_FILE.IS_OPEN(v_file) THEN
        UTL_FILE.FCLOSE(v_file);
      END IF;
      DBMS_OUTPUT.PUT_LINE('Error generating report: ' || SQLERRM);
      RAISE;
  END;
END generate_changes_report;
/

-- Reset the report tracking to capture all changes since the beginning
UPDATE report_tracking 
SET last_report_time = TO_TIMESTAMP('2000-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS');
COMMIT;

-- Insert some test data to verify tracking
INSERT INTO customers (customer_id, customer_name) VALUES (101, 'Test Customer');
INSERT INTO products (product_id, product_name, price) VALUES (101, 'Test Product', 99.99);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (101, 101, 101, 5);

COMMIT;

-- To generate a simple report since the last report:
EXEC generate_changes_report(p_file_path => 'changes_report.html', p_include_details => TRUE);

-- Clean up test data
DELETE FROM orders WHERE order_id = 101;
DELETE FROM products WHERE product_id = 101;
DELETE FROM customers WHERE customer_id = 101;
COMMIT;

-- Generate another report to see the delete operations
EXEC generate_changes_report(p_since_timestamp => TO_TIMESTAMP('2025-04-21 15:29:21.440', 'YYYY-MM-DD HH24:MI:SS.FF3'), p_file_path => 'changes_report2.html', p_include_details => TRUE);


INSERT INTO customers (customer_id, customer_name) VALUES (101, 'Test Customer');
INSERT INTO products (product_id, product_name, price) VALUES (101, 'Test Product', 99.99);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (101, 101, 101, 5);
COMMIT;

UPDATE customers SET customer_name = 'SSS' WHERE customer_id = 101;
UPDATE products SET product_name = 'SSS' WHERE product_id = 101;
UPDATE orders SET quantity = 100 WHERE order_id = 101;
COMMIT;

DELETE FROM orders WHERE order_id = 101;
DELETE FROM products WHERE product_id = 101;
DELETE FROM customers WHERE customer_id = 101;
COMMIT;











