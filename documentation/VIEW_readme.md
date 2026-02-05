# BLINKIT CUSTOMER ANALYSIS VIEWS DOCS

=====================================================
### WHY VIEWS? BUILDING A SEMANTIC LAYER FOR POWER BI
=====================================================
```
    **BUSINESS CONTEXT**:
    After doing a thorough-out sql *ad-hoc* analysis, we have found some valuable insights to show it through dashboards.
    But this insights are derived from multi-joined, complex queries that requires SQL knowledge in any dashboard creation tools too.
    

    **THE CHALLENGE:**
    Our insights or queries requires complex joining and window functions:
    - Multi-table joins  (orders * products * customers)
    - Window functions
    - more than one CTEs (multi-step calculations)

    **To counter this challenges we have chosen SQL VIEWS.**

    **What is a VIEW?**
        - Views are virtual tables based on the result set of a SQL statement or query.
        - Views contains rows and columns, just like a real table. The fields in the views are rows and columns from one or more than one different real tables in a database.
        - Views are not stored physically, avoiding storage dilemmas and restriction.
        - We can query the views without any restrictions.
        - Easier to understand for non-tech peoples or stakeholders.
```

========================================
## Views created for our analysis
========================================

### 1. monthly cohort and customer retention view

> Description about columns:
- **birth_month** -> all the months that users made their first purchase or order as 'birth month'.
- **month_number** -> really important for our analysis, this columns indicates subsequent months after the birth month of a group of customers.
- **customer_cnt** -> count of customers (orders by customers) in their first month (birth month) and subsequenct months.
- **first_month_customer_cnt_locked** -> numbers of customer that made their first login or order in a certain month. ***Window function (First_value()) is used to get the first month's user count or the birth month of a number of customers.***
- **retention_rate** -> percentage of each month's customer and first month's total customer count. This column signifies, the retention of customer who came back after their first purchase in a later month.


### 2. RFM customer segment view

> Description about columns:
- **r_score** -> shows customer recent purchase by substracting it from the latest order date of the dataset.
- **f_score** -> shows customer's total order count till last date.
- **m_score** -> shows customer's total order value till last date.
- **rfm_bins** -> top three columns joined together and craeted as a text format (text, varchar(3))
- **avg_recency** -> average recency days of each group.
- **avg_monetary** -> average money spend per group RFM group of customers.
- **cust_cnt** -> count of distinct customers.
- **cust_size_pct** -> percentage of customer shares to total distinct customer count. 


### 3. category sequencing(which category was ordered after a order) view

> Description about columns:
- **curr_category** -> shows the first category which was order before another order.
- **next_category** -> next category after the first category. **Window function (lead())** was used to fetch next product category of every customer.
- **pair_ordering_size** -> times that this combo was ordered by any customer.
- **avg_revenue_per_pair** -> avg revenue each pair (curr category + next category) made.
- **revenue_rank** -> revenue rank by locking curr_category to see which combo paired with the curr category made the most revenue.


### 4. delivery timing effect on customer's feedback view.

> Description about columns:
- **delivery_timing_range** -> separated timing range using case() function.
- **avg_gap_between_promised_and_actual_time** -> avg gap of delivery timing between promised delivery time and actual delivery time.
- **order_cnt** -> order counts that fall into the different delivery timing ranges.
- **negative_fdbk_cnt** -> negative feedback count for each time range.
- **neg_fdbk_for_delivery_cnt** -> count negative reviews regarding delivery among all negative reviews.
- **neg_fdbk_for_delivery_among_all_neg_reviews_pct** -> percentage of neg reviews of delivery related reviews among all negative reviews.
- **order_contribution_pct** -> percent of orders among total order count.



### 5. monthly product performance analysis view

> Description about columns:
- **order_month** -> order months.
- **category** -> category of products.
- **order_cnt** -> order count of categories per month.
- **order_count_mth** -> monthly total order count.
- **aov** -> average order value of each categories per month.
- **monthly_aov** -> monthly average order value.
- **order_total_value** -> each category total order value per month.
- **mth_total_order_value** -> each month's total order value.
- **mthly_total_order_count_contribution_pct** -> percentage of each category's monthly total order count by monthly total order count.
- **monthly_order_value_contribution_pct** -> percentage of each category's monthly total order value by monthly total order value.
- **monthly_order_value_cont_rank** -> contribution rank of each month's total order value per category.
- **monthly_order_count_cont_rank** -> - **monthly_order_value_cont_rank** -> contribution rank of each month's total order count per category.


### 6. product marginal difference analysis view

> Description about columns:
- **margin_percentage** -> marginal difference of product's mrp and cost price.
- **category** -> category of products.
- **product_name** -> name of products.
- **order_cnt** -> count of orders per product.
- **avg_price** -> average cost price of each products.
- **avg_mrp** -> average mrp of each products.
- **aov** -> average order value per product.

> all views are added to the database and is ready for powerBI data connection build-up.
