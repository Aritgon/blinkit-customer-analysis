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
        - We can query the views many times.
        - Easier to understandd for non-tech peoples or stakeholders.
```

