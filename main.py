import duckdb

from src.data_extraction import *
from src.publish_orders import *
from src.publish_product import *
from src.final_questions import *
from src.utils import export_file
from src.utils import export_file


##Run whole pipeline
def main():
    # Connect to in-memory DuckDB database
    conn = duckdb.connect(":memory:")

    # Load run SQL commands for loading
    conn.execute(raw_products_sql)
    export_file(conn, "storage_products")

    conn.execute(raw_sales_order_header_sql)
    export_file(conn, "storage_sales_order_header", "parquet")
    
    conn.execute(raw_sales_order_detail_sql)
    export_file(conn, "storage_sales_order_detail", "parquet")

    # Load run SQL commands for publish_product
    conn.execute(publish_product_sql)
    export_file(conn, "storage_sales_order_detail", "parquet")
    export_file(conn, "publish_product")

    # Load run SQL commands for publish_orders
    conn.create_function('business_days_calc', business_days_calc, [BIGINT, BIGINT], BIGINT)
    conn.execute(publish_orders_sql)
    export_file(conn, "publish_orders")

    #Final questions
    conn.execute(high_color_sql)
    conn.execute(avg_leadTime_sql)
    
    conn.close()

if __name__=='__main__':
    main()