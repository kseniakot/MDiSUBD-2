insert into customers (customer_id, customer_name) values (1, 'John Doe');
insert into customers (customer_id, customer_name) values (2, 'Jane Smith');

insert into products (product_id, product_name, price) values (1, 'Laptop', 1200.00);
insert into products (product_id, product_name, price) values (2, 'Smartphone', 800.00);

insert into orders (order_id, customer_id, product_id, quantity) values (1, 1, 1, 1);
insert into orders (order_id, customer_id, product_id, quantity) values (2, 2, 2, 2);

delete from orders where order_id = 2;
select * from orders_history;



-- Example data and usage
INSERT INTO customers (customer_id, customer_name) VALUES (3, 'John Doe');
INSERT INTO customers (customer_id, customer_name) VALUES (4, 'Jane Smith');

INSERT INTO products (product_id, product_name, price) VALUES (3, 'Laptop', 1200.00);
INSERT INTO products (product_id, product_name, price) VALUES (4, 'Smartphone', 800.00);

INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (3, 1, 1, 3);
INSERT INTO orders (order_id, customer_id, product_id, quantity) VALUES (4, 2, 2, 2);

-- Save current timestamp for future reference
DECLARE
  current_time TIMESTAMP;
BEGIN
  SELECT SYSTIMESTAMP INTO current_time FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('Current timestamp: ' || TO_CHAR(current_time, 'YYYY-MM-DD HH24:MI:SS.FF3'));
  DBMS_OUTPUT.PUT_LINE('Remember this timestamp to rollback to this point later');
END;
/

-- Make some changes
UPDATE customers SET customer_name = 'Daniel' WHERE customer_id = 1;
DELETE FROM orders WHERE order_id = 2;
INSERT INTO products (product_id, product_name, price) VALUES (3, 'Tablet', 500.00);


select * from customers;
select * from products;
select * from orders;
select * from customers_history;
select * from products_history;
select * from orders_history;

delete from customers where customer_id = 1;
ALTER TABLE orders DROP CONSTRAINT fk_order_customer;
ALTER TABLE orders ADD CONSTRAINT fk_order_customer
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
ON DELETE CASCADE;


-- tests

EXEC history_mgmt.rollback_to(TO_TIMESTAMP(' 2025-04-21 13:40:23.575', 'YYYY-MM-DD HH24:MI:SS.FF3'));

-- Rollback by milliseconds (e.g., 60000 ms = 1 minute ago):
EXEC history_mgmt.rollback_to(1204560);

-- Show changes after a specific timestamp:
EXEC history_mgmt.show_changes_after(TO_TIMESTAMP('2025-04-20 17:25:54.319', 'YYYY-MM-DD HH24:MI:SS.FF3'));


DECLARE
  v_start TIMESTAMP := TO_TIMESTAMP('2025-04-20 17:25:54.319', 'YYYY-MM-DD HH24:MI:SS.FF3');
  v_diff INTERVAL DAY TO SECOND := SYSTIMESTAMP - v_start;
  v_ms NUMBER;
BEGIN
  DBMS_OUTPUT.PUT_LINE('v_diff: ' || v_diff);
  v_ms := EXTRACT(DAY FROM v_diff) * 24*60*60*1000 +
          EXTRACT(HOUR FROM v_diff) * 60*60*1000 +
          EXTRACT(MINUTE FROM v_diff) * 60*1000 +
          EXTRACT(SECOND FROM v_diff) * 1000;
  
  DBMS_OUTPUT.PUT_LINE('Разница в миллисекундах: ' || ROUND(v_ms, 3));
END;