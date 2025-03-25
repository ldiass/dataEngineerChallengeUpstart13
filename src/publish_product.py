import duckdb

publish_product_sql="""
    CREATE TABLE publish_product (
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

    INSERT INTO publish_product
    SELECT productId
        , productDesc
        , productNumber
        , makeFlag
        , COALESCE(color, 'N/A') as color
        , safetyStockLevel
        , reorderPoint
        , standardCost
        , listPrice
        , size
        , sizeUnitMeasureCode
        , weight
        , weightUnitMeasureCode
        , CASE WHEN productCategoryName IS NULL THEN
            CASE
                WHEN ProductSubCategoryName IN ('Gloves', 'Shorts', 'Socks', 'Tights', 'Vests') THEN 'Clothing'
                WHEN ProductSubCategoryName IN ('Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps') THEN 'Accessories'
                WHEN ProductSubCategoryName ILIKE '%frames%' OR ProductSubCategoryName in ('Wheels', 'Saddles') THEN 'Components'
            END
            ELSE productCategoryName
        END as productCategoryName
        , productSubCategoryName
    FROM read_parquet('storage_products.parquet')
    ORDER BY productId
    ;

    --Write storage table to csv to make it readable
    COPY
    (SELECT * FROM publish_product)
    TO 'publish_product.csv'
    (FORMAT CSV);
    """

if __name__=='__main__':
    # Connect to in-memory DuckDB database
    conn = duckdb.connect(":memory:")

    # Load run SQL commands for loading
    conn.execute(publish_product_sql)

    # Close connection
    conn.close()