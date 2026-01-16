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
	The latest query shows that right after birth month, user retention rate dropped to ~9% at 
	the subsequent month.
	This shows that blinkit's most customers are one time buyers according to the dataset and 
	only a small unit of people ordered second time.

	Blinkit need to improve it's user retention. 
	May be by giving super deals, coupons to selected customers and better product value.
*/


-- 1. RFM (recency, frequency and monetary) analysis --

/*
	This analysis will help us know blinkit's customer base in depth.
	what is RFM analysis ?
	
	R -> Recency - meaning how current the customer has ordered from blinkit from today 
	(in this case, last transactional day of the dataset).
	F -> Frequency - how many times each customers have ordered from blinkit.
	M -> Monetary - How much total value each customer has spent in the platform.

	After getting this three stats of each customer, we will use NTILE() to bucketize customers or create bins
	that will help us know which category of customer is contributing to blinkit.
*/

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

/*
	Now, we will categorize each customer to give them a certain category.
	we will use case function to categorize recency score based on a numerical rank.
	
	min recency -> 0 & max recency -> 599
	since the gap is huge and probably contains a lot of churn customers. we are gonna rank recency score from backwards.
	we will follow the following formula, 
		if recency between 0 and 20, then rank 1,
		if recency between 21 and 35, then rank 2,
		if recency between 36 and 50, then rank 3,
		if recency between 51 and 80, then rank 4
		and rank 5 to customer's who have a recency score more than 80.
	
	min frequency -> 1 & max frequency -> 9.
	for setting up freq_score we will follow the following formula,
		if frequency between 9 and 6 then rank 1,
		if frequency between 6 and 4 then rank 2,
		if frequency betweek 3 and 2 then rank 3,
		if frequency = 2 then rank 4,
		else 1.

	min monetary -> 22.14 & max monetary -> 21686.80
	for setting up monetary_score, we will follow the following formula,
		monetary > 8000 then rank 1
		monetary > 5000 and < 8000, then rank 2
		monetary > 2000 and < 5000, then rank 3
		monetary < 1000 and < 2000, then rank 4
		else 5
*/

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
		when frequency between 9 and 7 then 1
		when frequency between 6 and 4 then 2
		when frequency between 3 and 2 then 3
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

/*
	In the next CTE we are going to create bins or categories 
	with the merged scores to get more compact result from the score cte.		
*/

select
	-- merging all the scores first.

	r_score,
	f_score,
	m_score,
	
	(r_score * 100 + f_score * 10 + m_score) as rfm_bins,
	-- counting customers.

	-- stats per group.
	round(avg(recency)::decimal, 2) as avg_recency,
	round(avg(frequency)::decimal, 2) as avg_frequency,
	round(avg(monetary)::decimal, 2) as avg_monetary,
	count(distinct customer_id) as cust_cnt,
	(select count(distinct customer_id) from blinkit_orders) as total_cust_cnt,
	
	-- using whole customer_base to get pct of customers from each segments.
	round(count(distinct customer_id) * 100.0 / (select count(distinct customer_id) from blinkit_orders), 2) as cust_size_pct
from score_cte
group by r_score, f_score, m_score, rfm_bins
order by avg(monetary) desc;

/*
	Through this RFM analysis, we acknowledged that 

*/