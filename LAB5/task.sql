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
  old_reg_date    DATE           
);

CREATE TABLE products_history (
  history_id      NUMBER         PRIMARY KEY,
  operation_type  VARCHAR2(10)   NOT NULL,
  change_time     TIMESTAMP      DEFAULT SYSTIMESTAMP,
  product_id      NUMBER(6)      NOT NULL,
  product_name    VARCHAR2(100),
  old_name        VARCHAR2(100),  
  price           NUMBER(10, 2),
  old_price       NUMBER(10, 2)   
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
  old_quantity    NUMBER(5)      
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
      -- First, identify all customers and products referenced in orders that need to be restored
      -- need to ensure they exist before restoring the orders
      DECLARE
        -- Tables to store IDs of entities that need to be restored
        TYPE t_id_array IS TABLE OF NUMBER INDEX BY PLS_INTEGER; -- similar to dct in python
        v_required_customers t_id_array;
        v_required_products t_id_array;
        v_customer_count PLS_INTEGER := 0;
        v_product_count PLS_INTEGER := 0;
        
        -- Cursor to find orders that will be restored and their references
        CURSOR c_order_references IS
          SELECT DISTINCT customer_id, product_id
          FROM orders_history
          WHERE change_time <= p_timestamp
          AND operation_type IN ('INSERT', 'UPDATE')
          AND customer_id IS NOT NULL
          AND product_id IS NOT NULL;
          
      BEGIN
        -- Collect all required customers and products from orders
        FOR r_ref IN c_order_references LOOP
          -- Check if customers exist
          SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_ref.customer_id;
          IF v_exists = 0 THEN 
            v_customer_count := v_customer_count + 1;
            v_required_customers(v_customer_count) := r_ref.customer_id;
          END IF;
          
          -- Check if products exist
          SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_ref.product_id;
          IF v_exists = 0 THEN
            v_product_count := v_product_count + 1;
            v_required_products(v_product_count) := r_ref.product_id;
          END IF;
        END LOOP;
        
        DBMS_OUTPUT.PUT_LINE('Found ' || v_customer_count || ' missing customers and ' || 
                           v_product_count || ' missing products referenced by orders');
      
        -- First, restore missing customers
        IF v_customer_count > 0 THEN
          FOR i IN 1..v_customer_count LOOP
            DECLARE
              v_customer_id NUMBER := v_required_customers(i);
              v_name VARCHAR2(100);
              v_reg_date DATE;
              
              -- Find the most recent state of this customer before the timestamp
              CURSOR c_find_customer IS
                SELECT customer_id, customer_name, registered_at, operation_type, 
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_time DESC) AS rn
                FROM customers_history
                WHERE change_time <= p_timestamp
                AND customer_id = v_customer_id
                AND operation_type IN ('INSERT', 'UPDATE');
            BEGIN
              -- Attempt to restore the customer
              FOR r_cust IN c_find_customer LOOP
                IF r_cust.rn = 1 THEN 
                  DBMS_OUTPUT.PUT_LINE('Restoring referenced customer ID: ' || v_customer_id);
                  
                  INSERT INTO customers (customer_id, customer_name, registered_at)
                  VALUES (r_cust.customer_id, r_cust.customer_name, r_cust.registered_at);
                  
                  EXIT; -- We found and restored the customer
                END IF;
              END LOOP;
            END;
          END LOOP;
        END IF;
        
        -- Next, restore missing products
        IF v_product_count > 0 THEN
          FOR i IN 1..v_product_count LOOP
            DECLARE
              v_product_id NUMBER := v_required_products(i);
              
              -- Find the most recent state of this product before the timestamp
              CURSOR c_find_product IS
                SELECT product_id, product_name, price, operation_type, 
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY change_time DESC) AS rn
                FROM products_history
                WHERE change_time <= p_timestamp
                AND product_id = v_product_id
                AND operation_type IN ('INSERT', 'UPDATE');
            BEGIN
              -- Attempt to restore the product
              FOR r_prod IN c_find_product LOOP
                IF r_prod.rn = 1 THEN 
                  DBMS_OUTPUT.PUT_LINE('Restoring referenced product ID: ' || v_product_id);
                  
                  INSERT INTO products (product_id, product_name, price)
                  VALUES (r_prod.product_id, r_prod.product_name, r_prod.price);
                  
                  EXIT; -- We found and restored the product
                END IF;
              END LOOP;
            END;
          END LOOP;
        END IF;
      END;

      -- Now proceed with regular rollback of customers
      DECLARE
        CURSOR c_customers IS
          SELECT customer_id, customer_name, registered_at, operation_type, 
                 ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY change_time DESC) AS rn
          FROM customers_history
          WHERE change_time <= p_timestamp;
      BEGIN
        FOR r_cust IN c_customers LOOP
          IF r_cust.rn = 1 THEN  -- Get the latest state before the timestamp
            -- Check if the customer exists
            SELECT COUNT(*) INTO v_exists FROM customers WHERE customer_id = r_cust.customer_id;
            
            IF r_cust.operation_type = 'DELETE' THEN
              IF v_exists > 0 THEN
                DELETE FROM customers WHERE customer_id = r_cust.customer_id;
              END IF;
            ELSIF r_cust.operation_type IN ('INSERT', 'UPDATE') THEN
              IF v_exists > 0 THEN
                UPDATE customers
                SET customer_name = r_cust.customer_name,
                    registered_at = r_cust.registered_at
                WHERE customer_id = r_cust.customer_id;
              ELSE
                INSERT INTO customers (customer_id, customer_name, registered_at)
                VALUES (r_cust.customer_id, r_cust.customer_name, r_cust.registered_at);
              END IF;
            END IF;
          END IF;
        END LOOP;
      END;

      -- Rollback products
      DECLARE
        CURSOR c_products IS
          SELECT product_id, product_name, price, operation_type, 
                 ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY change_time DESC) AS rn
          FROM products_history
          WHERE change_time <= p_timestamp;
      BEGIN
        FOR r_prod IN c_products LOOP
          IF r_prod.rn = 1 THEN
            SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = r_prod.product_id;
            
            IF r_prod.operation_type = 'DELETE' THEN
              IF v_exists > 0 THEN
                DELETE FROM products WHERE product_id = r_prod.product_id;
              END IF;
            ELSIF r_prod.operation_type IN ('INSERT', 'UPDATE') THEN
              IF v_exists > 0 THEN
                UPDATE products
                SET product_name = r_prod.product_name,
                    price = r_prod.price
                WHERE product_id = r_prod.product_id;
              ELSE
                INSERT INTO products (product_id, product_name, price)
                VALUES (r_prod.product_id, r_prod.product_name, r_prod.price);
              END IF;
            END IF;
          END IF;
        END LOOP;
      END;

      -- Finally rollback orders
      DECLARE
        CURSOR c_orders IS
          SELECT order_id, customer_id, product_id, order_date, quantity, operation_type, 
                 ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY change_time DESC) AS rn
          FROM orders_history
          WHERE change_time <= p_timestamp;
      BEGIN
        FOR r_ord IN c_orders LOOP
          IF r_ord.rn = 1 THEN
            SELECT COUNT(*) INTO v_exists FROM orders WHERE order_id = r_ord.order_id;
            
            IF r_ord.operation_type = 'DELETE' THEN
              IF v_exists > 0 THEN
                DELETE FROM orders WHERE order_id = r_ord.order_id;
              END IF;
            ELSIF r_ord.operation_type IN ('INSERT', 'UPDATE') THEN
              -- Check that referenced entities exist
              DECLARE
                v_customer_exists NUMBER := 0;
                v_product_exists NUMBER := 0;
              BEGIN
                IF r_ord.customer_id IS NOT NULL THEN
                  SELECT COUNT(*) INTO v_customer_exists FROM customers WHERE customer_id = r_ord.customer_id;
                ELSE
                  v_customer_exists := 1; -- NULL customer_id is allowed
                END IF;
                
                IF r_ord.product_id IS NOT NULL THEN
                  SELECT COUNT(*) INTO v_product_exists FROM products WHERE product_id = r_ord.product_id;
                ELSE
                  v_product_exists := 1; -- NULL product_id is allowed
                END IF;
                
                -- Only restore order if referenced entities exist
                IF v_customer_exists > 0 AND v_product_exists > 0 THEN
                  IF v_exists > 0 THEN
                    UPDATE orders
                    SET customer_id = r_ord.customer_id,
                        product_id = r_ord.product_id,
                        order_date = r_ord.order_date,
                        quantity = r_ord.quantity
                    WHERE order_id = r_ord.order_id;
                  ELSE
                    INSERT INTO orders (order_id, customer_id, product_id, order_date, quantity)
                    VALUES (r_ord.order_id, r_ord.customer_id, r_ord.product_id, r_ord.order_date, r_ord.quantity);
                  END IF;
                ELSE
                  DBMS_OUTPUT.PUT_LINE('Warning: Cannot restore order ID ' || r_ord.order_id || 
                                       ' - Missing referenced customer or product');
                END IF;
              END;
            END IF;
          END IF;
        END LOOP;
      END;

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


