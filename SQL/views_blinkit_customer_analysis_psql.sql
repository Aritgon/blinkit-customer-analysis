-- Views psql file.

/*
	This file will hold the official documentation of all the views for our blinkit customer analysis project.
*/

-- 1. Customer cohort view.
/*
	Why?
		- This analysis indicates an interesting customer behaviour for blinkit. 
		Customer retention (order count for our case) generally drops to an avg of approx. 9% 
		after their first month of order (which is generally at 100%).

		- After that drop off, blinkit generally maintains an avg retention rate of 9% every month after customer's
		first order.

		- This signifies a big and decisive step has to be taken to attract more purchase from existing customers, 
		after that blinkit can focus more on new customers.
		
		- Some recommendations : blinkit can use customer loyalty programs, customer profiling (such as membership plans
		, bonus points etc. Blinkit also need to put more effort on better ad management and publish. 

		- Like big e-commerce stores like FlipKart, Amazon they can also launch timely events such as republic day events,
		october sales during peak festival time etc.
		
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

-- =======================================================================================
-- 2. RFM customer segment view.

create or replace view vw_rfm_customer_segment as
with cust_details_cte as 
(select
	a.customer_id,
	b.customer_name,

	(select max(order_date)::date from blinkit_orders) - max(order_date)::date as recency,
	count(a.order_id) as frequency,
	round(sum(a.order_total)::decimal, 2) as monetary
from blinkit_orders as a
join blinkit_customer_details as b on b.customer_id = a.customer_id
group by a.customer_id, b.customer_name),

score_cte as 
(select
	customer_id,
	customer_name,

	recency,
	frequency,
	monetary,
	
	case
		when recency between 0 and 20 then 1
		when recency between 21 and 35 then 2
		when recency between 36 and 50 then 3
		when recency between 51 and 80 then 4
		else 5
	end as r_score,
	case
		when frequency >= 7 and frequency <= 9 then 1
		when frequency >= 4 and frequency <= 6 then 2
		when frequency = 3 then 3
		when frequency = 2 then 4
		else 5
	end as f_score,
	case
		when monetary > 5000 then 1
		when monetary between 3001 and 5000 then 2
		when monetary between 1501 and 3000 then 3
		when monetary between 801 and 1500 then 4
		else 5
	end as m_score
	
from cust_details_cte
)

select
	-- merging all the scores first.
	r_score,
	f_score,
	m_score,
	
	(r_score * 100 + f_score * 10 + m_score) as rfm_bins,

	round(avg(recency)::decimal, 2) as avg_recency,
	round(avg(monetary)::decimal, 2) as avg_monetary,
	
	-- counting customers.
	count(distinct customer_id) as cust_cnt,
	
	-- using whole customer_base to get pct of customers from each segments.
	round(count(distinct customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_orders), 2) as cust_size_pct
from score_cte
group by r_score, f_score, m_score, rfm_bins
order by cust_size_pct desc;

-- ==============================================================================
-- 3. View of next category propensity or category sequencing.
-- In this query, we analysed which product was bought after a certain product and how much that combo made in revenue,
-- ranking with dense_rank.

create or replace view vw_category_sequencing as
with category_cte as
(select
	a.customer_id,

	d.category as curr_category,
	a.order_total as curr_order_total,
	
	a.order_date as curr_order_date,
	lead(d.category) over (partition by a.customer_id order by a.order_date) as next_category,
	lead(a.order_total) over (partition by a.customer_id order by a.order_date) as next_order_total,
	lead(a.order_date) over (partition by a.customer_id order by a.order_date) as next_order_date
	
from blinkit_orders as a
join blinkit_order_items as b on b.order_id = a.order_id
join blinkit_customer_details as c on c.customer_id = a.customer_id
join blinkit_products as d on d.product_id = b.product_id)

select
	curr_category,
	next_category,

	count(*) as pair_ordering_size,
	round(avg(curr_order_total + next_order_total)::decimal, 2) as avg_revenue_per_pair,
	
	-- we are analysis products that was bought after a certain product and how many much revenue they made.
	dense_rank() over (partition by curr_category order by avg(curr_order_total + next_order_total) desc) as revenue_rank
from category_cte
where next_category is not null
group by 1, 2
having count(*) > 1; -- filtering category sequence that was only ordered one time.

select * from vw_category_sequencing;

-- ==================================================================================
-- 4. View : how delivery timing affected customer feedback by every customer and every order.
create or replace view vw_delivery_timing_effect_on_cust_fdbk as
with cust_cte as
(select

	b.order_id,
	a.customer_id,
	b.gap_between_promised_actual_time,
	a.feedback_category,
	a.sentiment,
	a.rating
	
from blinkit_customer_feedback as a
left join blinkit_orders as b on b.order_id = a.order_id)

select
	case
		when gap_between_promised_actual_time between -5 and -1 then 'slightly late delivery'
		when gap_between_promised_actual_time between -10 and -6 then 'moderate late delivery'
		when gap_between_promised_actual_time between -15 and -11 then 'late delivery'
		when gap_between_promised_actual_time between -20 and -16 then 'very late delivery'
		when gap_between_promised_actual_time between -25 and -21 then 'concerningly late delivery'
		when gap_between_promised_actual_time between -30 and -26 then 'customer might churn if delivery becomes this late'

		when gap_between_promised_actual_time between 0 and 5 then 'slightly fast delivery'
		when gap_between_promised_actual_time between 6 and 10 then 'moderate late delivery'
		when gap_between_promised_actual_time between 11 and 15 then 'late delivery'
		when gap_between_promised_actual_time between 16 and 20 then 'very late delivery'
		when gap_between_promised_actual_time between 21 and 25 then 'concerningly late delivery'
	else 'other timing'
	end as delivery_timing_range,

	-- stats.
	round(avg(gap_between_promised_actual_time)::decimal, 2) as avg_gap_between_promised_and_actual_time,
	count(order_id) as order_cnt,
	
	-- getting negative sentiments feedback count and pct from all feedbacks recorded for each delivery type.
	count(case when sentiment = 'negative' then 1 end) as negative_fdbk_cnt,

	count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) as neg_fdbk_for_delivery_cnt,
	-- round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	-- count(order_id), 2) as negative_fdbk_pct

	round(count(case when sentiment = 'negative' and feedback_category = 'delivery' then 1 end) * 100.0 / 
	count(case when sentiment = 'negative' then 1 end), 2) as neg_fdbk_for_delivery_among_all_neg_reviews_pct,

	-- getting order contribution pct of each segments from total orders.
	round(count(order_id) * 100.0 / sum(count(order_id)) over (), 2) as order_contribution_pct
	
from cust_cte
group by delivery_timing_range;

select * from vw_delivery_timing_effect_on_cust_fdbk;
-- drop view if exists vw_delivery_timing_effect_on_cust_fdbk;

-- ========================================================================================w
-- 5. View : Monthly product revenue and order count analysis.
create or replace view vw_monthly_product_analysis as
with mthly_categorical_order_cte as
(select
	date_trunc('month', o.order_date) as order_month,
	
	-- product category.
	p.category,
	count(*) as order_cnt,

	round(avg(o.order_total)::decimal, 2) as aov,
	round(sum(o.order_total)::decimal, 2) as order_total_value

from blinkit_orders as o
join blinkit_order_items as oi on oi.order_id = o.order_id
join blinkit_products as p on p.product_id = oi.product_id
group by 1,2),

/*
	Next cte will be about per month's total order count, avg order value and total order value.
*/

mthly_order_cte as (
select
	date_trunc('month', order_date) as order_month,
	count(*) as order_count_mth, -- each month's total order count.
	round(avg(order_total)::decimal, 2) as monthly_aov,
	round(sum(order_total)::decimal, 2) as mth_total_order_value
from blinkit_orders
group by 1),

main_cte as
(select
	a.order_month,
	a.category,
	a.order_cnt,
	b.order_count_mth,
	
	a.aov,
	b.monthly_aov,

	a.order_total_value,
	b.mth_total_order_value,
	
	-- analysing order contribution ratio for that month's total order count.
	round((a.order_cnt * 100.0 / b.order_count_mth)::decimal, 2) as mthly_total_order_count_contribution_pct,
	round((a.order_total_value * 100.0 / b.mth_total_order_value)::decimal, 2) as monthly_order_value_contribution_pct
	
from mthly_categorical_order_cte as a
left join mthly_order_cte as b on b.order_month = a.order_month)

/*
	Using dense_rank() on order_count_pct and order_value_pct to rank categories that made the highest revenue
	for a month and also order count ranking.
*/

select
	*,

	-- dense_rank() on revenue first.
	dense_rank() over (partition by order_month order by monthly_order_value_contribution_pct desc) as monthly_order_value_cont_rank,

	-- dense_rank() on order count.
	dense_rank() over (partition by order_month order by mthly_total_order_count_contribution_pct desc) as monthly_order_count_cont_rank
from main_cte;

-- select * from vw_monthly_product_analysis;

-- ============================================================
-- 5. View : marginal difference analysis.

create or replace view vw_marginal_diff as
select
	-- firstly, we are getting the margin difference percentage.
	p.margin_percentage,
	
	-- getting the product categories.
	p.category,
	p.product_name,

	count(*) as order_cnt,
	
	-- avg mrp per category.
	round(avg(p.price)::decimal, 2) as avg_price,
	round(avg(p.mrp)::decimal, 2) as avg_mrp,
	
	-- avg order value.
	round(avg(o.order_total)::decimal, 2) as aov
	
from blinkit_orders as o
left join blinkit_order_items as oi on oi.order_id = o.order_id
left join blinkit_products as p on p.product_id = oi.product_id
group by p.margin_percentage, p.category, p.product_name;

select * from vw_marginal_diff order by aov desc;
