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
alter table blinkit_order_items
add constraint fk_product_id
foreign key (product_id)
references blinkit_products(product_id);

-- joining the order items table with blinkit_orders table as a foreign key,
-- because order_id is the only matching key in this both tables.

alter table blinkit_order_items
add constraint fk_order_items_order_id
foreign key (order_id)
references blinkit_orders(order_id);

------- table creation and alteration is completed. -------

-- select * from blinkit_order_items, blinkit_products;  -- data is injected.

select count(*) from blinkit_order_items; -- 5000 rows as expected.
select count(*) from blinkit_products; -- 268 rows as expected.

-- data quality checking with SQL.

select *
from (select
	order_id,
	product_id,
	dense_rank() over (partition by order_id, product_id) as order_rnk
from blinkit_order_items) as subq
where order_rnk > 1; -- no double entries per each order_id and product_id.

select *
from (select
	product_id,
	product_name,
	category,
	dense_rank() over (partition by product_id, product_name, category) as product_rnk
from blinkit_products) as sbq
where product_rnk > 1; -- no double entries.

-- ****** data quality and sanity check is completed ****** --

-- doing product analysis with customer behavior -- 

-- customer's buying pattern analysis.
--we are seeing which products is the immediate next product ordered by the same customer,
-- to analyze hooked products and possibly most revenue generating product.


with cte as 
(select
	a.customer_id,
	a.order_id,

	-- getting product category & name.
	c.category,
	c.product_name as curr_prod_txn,

	-- getting products total order value.
	a.order_total,

	-- getting user's next immediate product.
	lead(c.product_name) over (partition by a.customer_id order by a.order_date) as nxt_prod_txn
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_products as c on c.product_id = b.product_id
join blinkit_customer_details as d on d.customer_id = a.customer_id)

select
	curr_prod_txn,
	nxt_prod_txn,
	count(*) as txn_cnt,
	round(avg(order_total)::decimal, 2) as aov_per_pair
from cte
where nxt_prod_txn is not null
group by 1,2
order by 4 desc;


-- doing the same analysis per category to which category was bought after immediate first order of any customer.

with cte as
(select
	a.customer_id,
	b.product_id,

	c.category as curr_category,
	a.order_total,

	lead(c.category) over (partition by a.customer_id order by a.order_date) as next_category,
	lead(a.order_total) over (partition by a.customer_id order by a.order_date) as next_order_total
	
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_products as c on c.product_id = b.product_id
join blinkit_customer_details as d on d.customer_id = a.customer_id)

select
	curr_category,
	next_category,
	count(*) as txn_cnt,
	round(sum(order_total + next_order_total)::decimal, 2) as total_order_value
from cte
where next_category is not null and next_order_total is not null
group by 1, 2
order by 1,2,4 desc;

select * from cte;