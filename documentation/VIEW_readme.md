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

# Views created for our analysis
========================================

## 1. monthly cohort and customer retention view

> Description about columns:
- **birth_month** -> all the months that users made their first purchase or order as 'birth month'.
- **month_number** -> really important for our analysis, this columns indicates subsequent months after the birth month of a group of customers.
- **customer_cnt** -> count of customers (orders by customers) in their first month (birth month) and subsequenct months.
- **first_month_customer_cnt_locked** -> numbers of customer that made their first login or order in a certain month. ***Window function (First_value()) is used to get the first month's user count or the birth month of a number of customers.***
- **retention_rate** -> percentage of each month's customer and first month's total customer count. This column signifies, the retention of customer who came back after their first purchase in a later month.



### =============== 🔍 insights ===============


``` 
This analysis indicates an **interesting** customer behaviour for blinkit.
    
    - Customer retention (order count for our case) generally drops to an avg of approx. 9% 
		after their first month of order (which is generally at 100%).

	- After that drop off, blinkit generally maintains an avg retention rate of 9% every month after customer's first order.

	- This signifies a big and decisive step has to be taken to attract more purchase from existing customers, after that blinkit can focus more on new customers.
		
	- Some recommendations ✨: blinkit can use customer loyalty programs, customer profiling (such as membership plans, bonus points etc) and targetted ad to customers who are in need of certain products (customer that are searching for a specific product but they can't find a good deal on it).

	- Like big e-commerce stores like FlipKart, Amazon they can also launch timely events such as republic day events, october sales during peak festival time etc.
```

## 2. RFM customer segment view

> Description about columns:
- **r_score** -> shows customer recent purchase by substracting it from the latest order date of the dataset.
- **f_score** -> shows customer's total order count till last date.
- **m_score** -> shows customer's total order value till last date.
- **rfm_bins** -> top three columns joined together and craeted as a text format (text, varchar(3))
- **avg_recency** -> average recency days of each group.
- **avg_monetary** -> average money spend per group RFM group of customers.
- **cust_cnt** -> count of distinct customers.
- **cust_size_pct** -> percentage of customer shares to total distinct customer count. 



### =============== 🔍 insights ===============


``` 
    From this RFM segment analysis, we got to know about blinkit's customer and their behavior.

    Points to consider:
        - Most numbers of customers have a longer recency than usual customers (around 67% of orders had a recency which had an average of ~200 days.) This means, most of the orders are old than recent and latest date of the dataset.

        - Even if (67% customers) ordered much time ago, the most revenue are also coming from the same category of people.

        - On the other hand, This same category of people haven't scored well in terms of frequency (order frequency). They stayed mostly in the (3-4 points) interms of recency score.

        - Recency and monetary is the important factor for most of the businesses. But q-commerce platforms like Blinkit, recency also matters to a pivotal point.

```


## 3. category sequencing(which category was ordered after a order) view

> Description about columns:
- **curr_category** -> shows the first category which was order before another order.
- **next_category** -> next category after the first category. **Window function (lead())** was used to fetch next product category of every customer.
- **pair_ordering_size** -> times that this combo was ordered by any customer.
- **avg_revenue_per_pair** -> avg revenue each pair (curr category + next category) made.
- **revenue_rank** -> revenue rank by locking curr_category to see which combo paired with the curr category made the most revenue.



### =============== 🔍 insights ===============


```
    From this analysis we analysed which category coupling or sequence made the most revenue.

    While this analysis, brings so much in-depth insights per category coupling. 
    I am only adding a few on here : 
    
    **special work : Before the query were answering category sequences ranked by first categories in terms of total revenue they made.
    For example, how much revenue 'groceries' + 'baby care' made than another category coupled with 'groceries'.
    Now for our analysis, I used filter using where to see which category combos ranked 1st interms of revenue for different categories. After that, I used order by clause get category combos of which made the most revenue out of all categories combos.

    -- Insights we found :
        - 'baby care & snacks-munchies' made the most revenue (avg of ~5260 rupees per order!), followed by 'instant-frozen food & personal care' with a avg of ~5191 rupees per order. After that at the third position we got 'snacks-munchies & household care' with a avg of ~5072 rupees per order.
```



## 4. delivery timing effect on customer's feedback view.

> Description about columns:
- **delivery_timing_range** -> separated timing range using case() function.
- **avg_gap_between_promised_and_actual_time** -> avg gap of delivery timing between promised delivery time and actual delivery time.
- **order_cnt** -> order counts that fall into the different delivery timing ranges.
- **negative_fdbk_cnt** -> negative feedback count for each time range.
- **neg_fdbk_for_delivery_cnt** -> count negative reviews regarding delivery among all negative reviews.
- **neg_fdbk_for_delivery_among_all_neg_reviews_pct** -> percentage of neg reviews of delivery related reviews among all negative reviews.
- **order_contribution_pct** -> percent of orders among total order count.



### =============== 🔍 insights ===============


```
    This query answers delivery timing ranges and how was the customer's feedback response for each timing range.

    - Timing range 'slightly late delivery' & 'slightly fast delivery' which has an average timing of '-3.02' (late) and '2.47' in minutes respectively, has received the most negative feedback in their own segment and among all orders. 

    - Both of this timing range has the most orders, contributing to ~70% of total orders.

    - Both of this timing range also received around ~6% of negative reviews regarding delivery timing in all feedbacks.

    - Quite surprisingly, This are the only two timing range that was close to the blinkit's promised delivery time and actual delivery time. But this segments has received the most delivery related negative reviews. 

    - This shows an interesting customer behaviour, because other delivery timing ranges which has a larger gap in minutes between promised delivery time and actual delivery time has received lesser negative feedback than this two!

    Extra insights:
        - 'very late delivery' category which has an avg delay from promised delivery time of ~18 minutes late. This category has also received the lowest negative feedback regarding late delivery (0.18% of negative feedback for delivery delay among all negative reviews). 

        - After both 'slightly late delivery' and 'slightly fast delivery', there are two other segments which are 'moderate late delivery' and 'late delivery' which are leading in third and fourth position for order contribution (~21% order contribution to total order). This segments have ~8 minutes and ~13 minutes of delivery delay from promised delivery timing (combined ~11 minutes of delay).  

```



## 5. monthly product performance analysis view

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
- **monthly_order_count_cont_rank** ->  contribution rank of each month's total order count per category.



### =============== 🔍 insights ===============


```
    This query answers monthly categorial performance trend.

    This query is pretty self explanatory but I am applying a filter where I will only get products that made to the top position per month. I am applying on both monthly_order_value_cont_rank and monthly_order_count_cont_rank to get number 1 spot.

    **Insights**:
        - Category 'dairy & breakfast' came in top 4 times throughout the dataset. It came in top 2 times in both 2023 and 2024.
        - Category 'pet care' came in top 3 times, 1 time in 2023 and 2 times in 2024.
        - Neither of this categories dominated the market for consecutive months.
```



## 6. product marginal difference analysis view

> Description about columns:
- **margin_percentage** -> marginal difference of product's mrp and cost price.
- **category** -> category of products.
- **product_name** -> name of products.
- **order_cnt** -> count of orders per product.
- **avg_price** -> average cost price of each products.
- **avg_mrp** -> average mrp of each products.
- **aov** -> average order value per product.



### =============== 🔍 insights ===============


```
    This query answers performance of categories that fall into different margin percentages.

    **Insights**:
        - 'sugar' that falls into 'groceries and staples' category is making the most average revenue (~2500 rupees per order) while its marginal difference is just 15%. Followed by 'mangoes' that falls into 'fruits & vegetables' category came into the second position while making average revenue of ~2396 rupees.

    **More interesting insights**:
        - If we query this view with only categorical data while excluding product, we will get 
        'pet care' category making the most aov (~2253 rupees per order) falls into marginal difference range of 35 (cost to order total).

        - Followed by 'household care' which falls into the marginal difference category of 25, is making an aov of ~2250 rupees per order.

        - After that we can see 'groceries & staples' category at the third position making an aov of ~2227 rupees per order. This category falls into the **15%** of marginal category. 

    --conclusion--:
    'sugar' from 'groceries & staples' which falls into marginal difference category of '15%' generally is making the most aov but if we exclude it, we can see 'pet care' category is making the most aov while having a marginal difference of 35%!!
```

### Views doc ends here. Moving onto powerBI dashboard creation.