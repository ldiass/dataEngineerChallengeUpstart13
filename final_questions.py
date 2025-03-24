import duckdb
from duckdb.typing import *
from utils import business_days_calc


#Which color generated the highest revenue each year?
high_color_sql="""
    SELECT year
        , color
        , colorRevenue
        , RANK() OVER (PARTITION BY year ORDER BY colorRevenue DESC) as rank
    FROM (
        SELECT datepart('year', orderDate) as year
            , color
            , SUM(totalLineExtendedPrice) as colorRevenue
        FROM read_csv_auto('publish_orders.csv') orders
        JOIN read_csv_auto('publish_product.csv') product
            ON orders.productID=product.productID
        GROUP BY 1,2
    ) t
    QUALIFY rank=1
    ORDER BY year desc
    ;
    """

#What is the average LeadTimeInBusinessDays by ProductCategoryName?
avg_leadTime_sql="""
    SELECT ProductCategoryName
        , AVG(LeadTimeInBusinessDays)
    FROM read_csv_auto('publish_orders.csv') orders
    JOIN read_csv_auto('publish_product.csv') product
        ON orders.productID=product.productID
    GROUP BY 1
"""


# Connect to in-memory DuckDB database
conn = duckdb.connect(":memory:")

# Load run SQL commands for loading
conn.execute(high_color_sql)
print(conn.fetchall())

conn.execute(avg_leadTime_sql)
print(conn.fetchall())

# Close connection
conn.close()