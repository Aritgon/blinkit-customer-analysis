-- injection of products table and order_items table in blinkit database for product analysis. 

drop table if exists blinkit_products;
drop table if exists blinkit_order_items;

create table if not exists blinkit_products (
	product_id bigint not null,
	product_name text not null,
	category text,
	brand text,
	price float,
	mrp float,
	margin_percentage float,
	shelf_life_days int,
	min_stock_level int,
	max_stock_level int,
	profit float
);


create table if not exists blinkit_order_items (
	order_id bigint not null,
	product_id bigint not null,
	quantity int,
	unit_price float
);

-- establishing relationships by addition of foreign keys.
-- PKs.
alter table blinkit_products
add constraint pk_product_id
primary key (product_id);

alter table blinkit_order_items
add constraint pk_order_id
primary key (order_id);

-- FKs.
