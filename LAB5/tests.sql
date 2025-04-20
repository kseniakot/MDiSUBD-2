insert into customers (customer_id, customer_name) values (1, 'John Doe');
insert into customers (customer_id, customer_name) values (2, 'Jane Smith');

insert into products (product_id, product_name, price) values (1, 'Laptop', 1200.00);
insert into products (product_id, product_name, price) values (2, 'Smartphone', 800.00);

insert into orders (order_id, customer_id, product_id, quantity) values (1, 1, 1, 1);
insert into orders (order_id, customer_id, product_id, quantity) values (2, 2, 2, 2);

delete from orders where order_id = 2;
select * from orders_history;