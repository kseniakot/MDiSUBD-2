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


