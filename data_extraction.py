import duckdb

raw_products_sql="""
    CREATE TABLE storage_products (
        productId INTEGER PRIMARY KEY,
        productDesc TEXT,
        productNumber TEXT,
        makeFlag varchar(5),
        color varchar(16),
        safetyStockLevel INTEGER,
        reorderPoint INTEGER,
        standardCost FLOAT,
        listPrice DECIMAL(10,2),
        size TEXT,
        sizeUnitMeasureCode varchar(5),
        weight DECIMAL(10,2),
        weightUnitMeasureCode varchar(5),
        productCategoryName VARCHAR,
        productSubCategoryName VARCHAR
    );

    INSERT INTO storage_products
    SELECT productId
        , productDesc
        , productNumber
        , makeFlag
        , color
        , safetyStockLevel
        , reorderPoint
        , standardCost
        , listPrice
        , size
        , sizeUnitMeasureCode
        , weight
        , weightUnitMeasureCode
        , STRING_AGG(productCategoryName, ', ') AS productCategoryName
        , STRING_AGG(productSubCategoryName, ', ') AS productSubCategoryName
    FROM read_csv_auto('raw_products.csv', HEADER=True)
    GROUP BY productId
        , productDesc
        , productNumber
        , makeFlag
        , color
        , safetyStockLevel
        , reorderPoint
        , standardCost
        , listPrice
        , size
        , sizeUnitMeasureCode
        , weight
        , weightUnitMeasureCode
        ;

    --Write storage table to parquet to keep data types and optmize ETL
    COPY
    (SELECT * FROM storage_products)
    TO 'storage_products.parquet'
    (FORMAT parquet);
    """

raw_sales_order_header_sql="""CREATE TABLE storage_sales_order_header (
    salesOrderID INTEGER PRIMARY KEY,
    orderDate DATE,
    shipDate DATE,
    onlineOrderFlag VARCHAR(5),
    accountNumber TEXT,
    customerID INTEGER,
    salesPersonID INTEGER,
    freight INTEGER
);

INSERT INTO storage_sales_order_header 
SELECT salesOrderID
    , CASE WHEN length(orderDate)=10 THEN orderDate::DATE
        ELSE (orderDate||'-01')::DATE
    END AS orderDate
    , shipDate
    , onlineOrderFlag
    , accountNumber
    , customerID
    , salesPersonID
    , freight
FROM read_csv_auto('raw_sales_order_header.csv', HEADER=True);

--Write storage table to parquet to keep data types and optmize ETL
COPY
    (SELECT * FROM storage_sales_order_header)
    TO 'storage_sales_order_header.parquet'
    (FORMAT parquet);
"""


raw_sales_order_detail_sql="""CREATE TABLE storage_sales_order_detail (
    salesOrderID INTEGER,
    salesOrderDetailID INTEGER PRIMARY KEY,
    orderQty INTEGER,
    productID INTEGER REFERENCES storage_products(productId),
    unitPrice DECIMAL(10,4),
    UnitPriceDiscount DECIMAL(4,4)
);

INSERT INTO storage_sales_order_detail 
SELECT salesOrderID
    , salesOrderDetailID
    , orderQty
    , productID
    , unitPrice
    , unitPriceDiscount
FROM read_csv_auto('raw_sales_order_detail.csv', HEADER=True);

--Write storage table to parquet to keep data types and optmize ETL
COPY
    (SELECT * FROM storage_sales_order_detail)
    TO 'storage_sales_order_detail.parquet'
    (FORMAT parquet);
"""


# Connect to in-memory DuckDB database
conn = duckdb.connect(":memory:")

# Load run SQL commands for loading
conn.execute(raw_products_sql)
conn.execute(raw_sales_order_header_sql)
conn.execute(raw_sales_order_detail_sql)

# Close connection
conn.close()