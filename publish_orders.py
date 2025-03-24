import duckdb
from duckdb.typing import *
from utils import business_days_calc

publish_orders_sql="""
    CREATE TABLE publish_orders (
        salesOrderID INTEGER,
        salesOrderDetailID INTEGER PRIMARY KEY,
        orderQty INTEGER,
        productID INTEGER,
        unitPrice DECIMAL(10,4),
        unitPriceDiscount DECIMAL(4,4),
        orderDate DATE,
        shipDate DATE,
        onlineOrderFlag VARCHAR(5),
        accountNumber TEXT,
        customerID INTEGER,
        salesPersonID INTEGER,
        totalOrderFreight INTEGER,
        totalLineExtendedPrice DECIMAL(10,4),
        LeadTimeInBusinessDays INTEGER
    );

    INSERT INTO publish_orders
    SELECT order_detail.salesOrderID
        , order_detail.salesOrderDetailID
        , order_detail.orderQty
        , order_detail.productID
        , order_detail.unitPrice
        , order_detail.unitPriceDiscount
        , order_header.orderDate
        , order_header.shipDate
        , order_header.onlineOrderFlag
        , order_header.accountNumber
        , order_header.customerID
        , order_header.salesPersonID
        , order_header.freight as totalOrderFreight
        , order_detail.orderQty * (order_detail.unitPrice - order_detail.unitPriceDiscount) as totalLineExtendedPrice
        , business_days_calc(datediff('day', order_header.orderDate, order_header.shipDate), datepart('dow', order_header.orderDate)) as LeadTimeInBusinessDays
    FROM read_parquet('storage_sales_order_detail.parquet') order_detail
    JOIN read_parquet('storage_sales_order_header.parquet') order_header 
        ON order_detail.salesOrderID=order_header.salesOrderID
    ;

    --Write storage table to csv to make it readable
    COPY
    (SELECT * FROM publish_orders)
    TO 'publish_orders.csv'
    (FORMAT CSV);
    """

# Connect to in-memory DuckDB database
conn = duckdb.connect(":memory:")

#Load the UDF
conn.create_function('business_days_calc', business_days_calc, [BIGINT, BIGINT], BIGINT)

# Load run SQL commands for loading
conn.execute(publish_orders_sql)

# Close connection
conn.close()