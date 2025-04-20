BEGIN
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
END;
/

-- Таблица клиентов
CREATE TABLE customers (
  customer_id     NUMBER(6)      PRIMARY KEY,
  customer_name   VARCHAR2(100)  NOT NULL,
  registered_at   DATE           DEFAULT SYSDATE
);

-- Таблица продуктов
CREATE TABLE products (
  product_id      NUMBER(6)      PRIMARY KEY,
  product_name    VARCHAR2(100)  NOT NULL,
  price           NUMBER(10, 2)  NOT NULL
);

-- Таблица заказов
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
