-- In this file, we are commencing our main part blinkit analysis which will be put in github to create a portfolio.

-- 1. Customer cohort analysis --
/* In this analysis, we will analyse each customer's first month, 
it will be termed as 'birth month' and how many customer's came at later month. 

This will help us identify customer's buying pattern and monthly retention rate of blinkit.
*/

-- getting each customer's first month of purchase as 'birth month of each customer'
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
group by birth_month, month_number),

/* using first_value() windows functions to get each birth_month's total user count.
	that first_value() will fetch the each month's total user count who ordered for first time in the app. 
	the subsequent month will show users who ordered in first month and cameback next month too.
*/

cohort_analysis_main as (
select
	birth_month,
	month_number,
	customer_cnt,
	first_value(customer_cnt) over (partition by birth_month) as first_month_customer_cnt_locked,
	round(customer_cnt * 100.0 / first_value(customer_cnt) over (partition by birth_month), 2) as retention_rate
from cohort_month_cte)

-- analysing what is the average retention rate for each month.

select
	month_number,
	round(avg(retention_rate)::decimal, 3) as avg_retention_rate
from cohort_analysis_main
group by month_number
order by month_number;

/*
	The latest query shows that right after birth month, user retention rate  dropped to ~9% of total users.
	This shows that blinkit's most customers are one time buyers according to the dataset and 
	only a small unit of people ordered second time.

	Blinkit need to improve it's user retention. 
	May be by giving super deals, coupons to selected customers and better product value.
*/

