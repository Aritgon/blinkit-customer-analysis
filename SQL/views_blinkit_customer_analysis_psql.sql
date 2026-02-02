-- Views psql file.

/*
	This file will hold the official documentation of all the views for our blinkit customer analysis project.
*/

-- 1. RFM analysis view.
/*
	Why?
		- This analysis indicates an interesting customer behaviour for blinkit. 
		Customer retention (order count for our case) generally drops to an avg of approx. 9% 
		after their first month of order (which is generally at 100%).

		- After that drop off, blinkit generally maintains an avg retention rate of 9% every month after customer's
		first order.

		- This signifies a big and decisive step has to be taken to attract more purchase from existing customers.
		

		
*/


create view vw_monthly_retention_rate as
with birth_month_cte as 
(select
	customer_id,
	min(date_trunc('month', order_date)) as birth_month
from blinkit_orders
group by customer_id),

-- joining with the main table to get next purchase date.
next_month_cte as (
select
	b.birth_month,
	b.customer_id,
	(extract(year from a.order_date) - extract(year from b.birth_month)) * 12 +
	(extract(month from a.order_date) - extract(month from b.birth_month)) as month_number
from blinkit_orders as a
join birth_month_cte as b on b.customer_id = a.customer_id),

-- now counting each month's user first purchase and it's subsequent month's user comeback.
cohort_month_cte as (
select
	birth_month,
	month_number,
	count(distinct customer_id) as customer_cnt
from next_month_cte
group by birth_month, month_number)

/* using first_value() windows functions to get each birth_month's total user count.
	that first_value() will fetch the each month's total user count who ordered for first time in the app. 
	the subsequent month will show users who ordered in first month and cameback next month too.
*/

select
	birth_month,
	month_number,
	customer_cnt,
	first_value(customer_cnt) over (partition by birth_month) as first_month_customer_cnt_locked,
	round(customer_cnt * 100.0 / first_value(customer_cnt) over (partition by birth_month), 2) as retention_rate
from cohort_month_cte;


-- avg retention rate of every month.
select
	round(avg(retention_rate)::decimal, 2) as avg_retention_rate
from vw_monthly_retention_rate
where month_number != 0;
