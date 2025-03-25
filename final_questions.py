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
#[(2024, 'Yellow', 6368158.47890033, 1), (2023, 'Black', 15047694.369200917, 1), (2022, 'Black', 14005242.975200394, 1), (2021, 'Red', 6019614.015699884, 1)]

conn.execute(avg_leadTime_sql)
print(conn.fetchall())
#[('Clothing', 5.005443886097153), ('Accessories', 5.007219107595422), ('Bikes', 5.005137673597174), (None, 5.011021698138262), ('Components', 5.003177286771993)]

# Close connection
conn.close()