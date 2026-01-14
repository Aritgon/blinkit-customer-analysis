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
	a.order_total as curr_order_total,

	-- getting user's next immediate product.
	lead(c.product_name) over (partition by a.customer_id order by a.order_date) as nxt_prod_txn,
	lead(a.order_total) over (partition by a.customer_id order by a.order_date) as nxt_order_total
	
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_products as c on c.product_id = b.product_id
join blinkit_customer_details as d on d.customer_id = a.customer_id)

select
	curr_prod_txn,
	nxt_prod_txn,
	count(*) as txn_cnt,
	round(sum(curr_order_total + nxt_order_total)::decimal, 2) as total_order_value
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
order by 4 desc;

-- analysing customer's choice of product inside blinkit. In this analysis, we will analyze if any customer has bought the same 
-- product for constant 3 times to see if any customer has a solid product preference or not.
-- this is a strict analysis, where we are using windows functions to get the immediate product they have ordered.

with cte as
(select
	a.order_id,
	c.customer_id,

	d.product_id,
	d.product_name as fst_purchase,
	
	lead(d.product_name, 1) over (partition by c.customer_id order by a.order_date) as snd_purchase,
	
	lead(d.product_name, 2) over (partition by c.customer_id order by a.order_date) as trd_purchase
	
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_customer_details as c on c.customer_id = a.customer_id
join blinkit_products as d on d.product_id = b.product_id
)

select
	customer_id,
	fst_purchase,
	snd_purchase,
	trd_purchase
from cte
where fst_purchase = snd_purchase and trd_purchase = snd_purchase; -- there's no result.

-- now we are analysing customer's who bought a product more than one time.
select
	b.customer_id,
	b.customer_name,

	c.product_id,
	d.product_name,
	count(*) as size_count
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_order_items as c on c.order_id = a.order_id
join blinkit_products as d on d.product_id = c.product_id
group by 1,2,3,4
having count(*) > 1; -- filtering out one time buyers. 

-- as there's very low quantity of order sizes per product and customer, I think it is best suited to not to elongate that analysis.

-- analysing time gap between two purchased products (the products should be same).
-- because from our previous analysis we've found a small percentage of customers who have ordered one product more than once.
-- trying to use self join on same order_id, customer_id and product_id to get order date.

with cte as
(select
	a.customer_id,
	d.product_id,
	a.order_date,
	d.product_name
	
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_order_items as c on c.order_id = a.order_id
join blinkit_products as d on d.product_id = c.product_id
)

select
	a.customer_id,
	a.product_id,
	a.product_name,
	a.order_date as first_buying_date,
	b.order_date as next_buying_date,
	b.order_date::date - a.order_date::date as order_day_gap
from cte as a
join cte as b on b.customer_id = a.customer_id and b.product_id = a.product_id
	where a.order_date < b.order_date;

-- CLV analysis (customer life value analysis)
select
	b.customer_id,
	b.customer_name,

	-- statistical numbers.
	count(a.order_id) as order_cnt,
	round(sum(a.order_total)::decimal, 2) as total_value_spent,
	round(avg(a.order_total)::decimal, 2) as aov,

	-- delivery timings.
	round(avg(a.delivery_gap_in_minutes)::decimal, 2) as avg_delivery_delay,
	round(avg(a.gap_between_promised_actual_time)::decimal, 2) as avg_promised_actual_time,

	-- extra additional analysis.
	-- counting pct of their good feedback among all orders.
	round(count(case when c.sentiment != 'Negative' then 1 end) * 100.0 / count(a.order_id), 2) as good_review_pct,
	round(count(case when c.sentiment = 'Negative' and c.feedback_category = 'Delivery' then 1 end) * 100.0 / count(a.order_id), 2) as delivery_wise_bad_category_pct

	
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
join blinkit_customer_feedback as c on c.customer_id = b.customer_id
group by 1, 2;

-- analysing product sentiment wise review. 
-- this analysis will help to identify products which generates more average order value but get's bad feedback regarding 
-- 'product quality', this will help the business to improve product wise quality improvement and better product management.
-- for this analysis, I am using DENSE_RANK() function to gives ranks to products which got the most fdbk regarding sentiment.
-- this will farther help us to identify that to categorize product by sentiment count.

with cte1 as
(select
	b.product_name,

	c.sentiment,

	round(avg(d.order_total)::decimal, 2) as avg_order_value,
	count(*) as fdbk_size,
	round(avg(c.rating), 2) as avg_rating,

	dense_rank() over (partition by b.product_name order by count(*) desc) as most_fdbk_received_rnk
from blinkit_order_items as a
join blinkit_products as b on b.product_id = a.product_id
join blinkit_customer_feedback as c on c.order_id = a.order_id
join blinkit_orders as d on d.order_id = a.order_id
where c.feedback_category = 'Product Quality' -- filtering which product got how much feedback by sentiment.
group by 1,2
),

cte2 as 
(select
	*,
	max(avg_order_value) over (partition by product_name) as max_revenue_per_product
from cte1
order by product_name
)

-- filtering aov with the max aov to actually get to know the product aov by sentiment wise, avg rnk and also sentiment rank.
select
	product_name,
	sentiment,
	avg_order_value,
	fdbk_size as fdbk_received,
	avg_rating,
	most_fdbk_received_rnk
from cte2
where avg_order_value = max_revenue_per_product; -- main filter to filter out products which is making below and above avg order value.

-- product wise MoM growth of order counts.

-- seeing which product got ordered by any customer to see how many of repeating products where bought

with cte as
(select
	b.product_id,
	b.product_name,
	c.order_date,
	lead(c.order_date) over (partition by b.product_id order by c.order_date) as next_order_date,
	extract(day from (lead(c.order_date) over (partition by b.product_id order by c.order_date)::timestamp - c.order_date::timestamp)) as gap_in_days
from blinkit_order_items as a
join blinkit_products as b on b.product_id = a.product_id
join blinkit_orders as c on c.order_id = a.order_id
)

select
	product_id,
	product_name,
	floor(avg(gap_in_days)) as avg_day_gap
from cte
group by product_id, product_name
order by 3;

-- products which ranked higher in each area in India? 
-- How much each product contributed to any state's total? 
-- advanced analysis : which product ranked 1st more than one time in India among many area?

select
	d.area,
	c.product_id,
	c.product_name,

	-- numerical stats.
	count(*) as order_size,
	round(avg(a.order_total)::decimal, 2) as aov,

	-- using dense_rank() to rank aov inside each area for each product.
	dense_rank() over (partition by d.area order by avg(a.order_total) desc) as aov_rnk
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_products as c on c.product_id = b.product_id 
join blinkit_customer_details as d on d.customer_id = a.customer_id
group by 1,2,3
having count(*) > 1;

-- seems a lot of product was only ordered one time in our 5000 row data. Unfortunately, this data isn't answering our motive.
-- we are using having clause to apply a filter to set the ranking among products that were ordered more than one time.
-- we are dropping this analysis as this set of data isn't valid to our analysis.

-- analysing which product contributed the most in our total revenue?

select
	c.product_name,
	round(sum(a.order_total)::decimal, 2) as total_order_value,

	-- using scaler sub_query to get total order value.
	-- (select round(sum(order_total)::decimal, 2) from blinkit_orders) as tov_static,

	round(sum(a.order_total)::decimal * 100.0 / (select sum(order_total) from blinkit_orders)::decimal, 2) as contribution_pct
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_products as c on c.product_id = b.product_id
group by 1
order by 2 desc;

-- "pet treats contributed the most with ~5% of order value, followed by toilet cleaners with ~4% and
-- lotion with ~4% of their order total".

-- doing same analysis on monthly basis to find if any product falls into any special or dedicated purchasing habit among customers

with cte1 as
(select
	extract(month from a.order_date) as order_month,
	-- getting product details.
	c.product_name,

	-- numerical stats.
	count(a.order_id) as order_size,
	round(sum(a.order_total)::decimal, 2) as total_order_value,

	-- ranking product total_order_value per month.
	dense_rank() over (partition by extract(month from a.order_date) order by sum(a.order_total) desc) as order_value_rnk
from blinkit_orders as a
join blinkit_order_items as b on a.order_id = b.order_id
join blinkit_products as c on c.product_id = b.product_id
group by 1, 2
),
-- doing monthly contribution percentage.
cte2 as (select
	order_month,
	product_name,
	order_size,
	total_order_value,
	order_value_rnk,

	-- each month's order_size and total order value. 
	-- sum(order_size) over (partition by order_month) as total_order_per_month,
	-- sum(total_order_value) over (partition by order_month) as total_order_value_per_month,

	round((order_size * 100.0 /sum(order_size) over (partition by order_month)), 2) as order_size_contribution_to_monthly_total_order_size,
	round((total_order_value * 100.0 /sum(total_order_value) over (partition by order_month)), 2) as order_value_contribution_to_monthly_total_order_value
from cte1)

-- filter to get top five products each month by it's total order value and it's contribution to that month's total order value.
select
	*
from cte2
where order_value_rnk <= 5;

-- counting products that came in 1st rank every month.
-- select
-- 	product_name,
-- 	count(*) as repetition_size
-- from cte2
-- where order_value_rnk <= 1
-- group by 1
-- order by 2 desc;

-- from this analysis, we acknoledged that "pet treats" has appeared 5 time in all month, followed by "lotion" (2 times) 
-- contributing mostly to monthly revenue contribution.

-- doing category total revenue contribution analysis per month
with cte1 as
(select
	extract(month from a.order_date) as order_month,
	-- getting product details.
	c.category,

	-- numerical stats.
	count(a.order_id) as order_size,
	round(sum(a.order_total)::decimal, 2) as total_order_value,

	-- ranking product total_order_value per month.
	dense_rank() over (partition by extract(month from a.order_date) order by sum(a.order_total) desc) as order_value_rnk
from blinkit_orders as a
join blinkit_order_items as b on a.order_id = b.order_id
join blinkit_products as c on c.product_id = b.product_id
group by 1, 2
),

cte2 as (select
	order_month,
	category,
	order_size,
	total_order_value,
	order_value_rnk,

	-- each month's order_size and total order value. 
	-- sum(order_size) over (partition by order_month) as total_order_per_month,
	-- sum(total_order_value) over (partition by order_month) as total_order_value_per_month,

	round((order_size * 100.0 /sum(order_size) over (partition by order_month)), 2) as order_size_contribution_to_monthly_total_order_size,
	round((total_order_value * 100.0 /sum(total_order_value) over (partition by order_month)), 2) as order_value_contribution_to_monthly_total_order_value
from cte1)

-- filtering top 3 categories each month.
-- select
-- 	*
-- from cte2
-- where order_value_rnk <= 3;

select
	category,
	count(*) as repetition_size
from cte2
where order_value_rnk <= 3
group by category
order by 2 desc;

-- in this categorical monthly revenue contribution analysis,
-- "dairy & breakfast" came in first position appearning 9 times
-- (in short, it has ranked 1st in 9 months regarding total order value contribution), followed by "pet care"
-- appearing 6 times through our analysis.

/* doing product analysis, which has a lower avg day gap during it's orders among customers.
	1. this will help us know which product was in hot needs for every customer.
	2. products which has bigger day gap. This might indicates that this products have lesser need, or needs better algorithm tweaks or may risk stock outdate
*/

