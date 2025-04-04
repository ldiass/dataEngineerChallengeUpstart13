# Initial considerations
    For solving the challenge, a SQL implementation was preferred, once it could solve the questions with a simple interface and can be hosted on multiple services with few compatibility issues. The SQLs are handled in Python scripts, in which an in-memory DuckDB instance is created.
    Additionally, in order to execute all the transformation stages in a single step, a main file was implemented, in which all the tables are loaded into the same DuckDB instance.
    For the challenge, it was assumed the provided files are the raw ones, therefore, the first transformation layer creates the storage files.

# 1. Data Loading:
    Data loading was executed by copying the data from the source .csv file provided, with the raw prefix, with DuckDB — an OLAP DBMS which is able to run in memory. For this challenge, it was assumed the files provided are the raw ones and no data transformation was implemented in this first step, only the names of the files were changed.
    
# 2. Data review and storage 
    Once the data is available in DuckDB through a SELECT directly from the source raw files, a few transformations are applied. In this step, the data types and key relationships are enforced. This new dataset is materialized in tables with the storage prefix. These DuckDB tables, in this architecture, are transitory, and therefore, the result is dumped into a .parquet file and the connection is closed, finalizing the database instance. The implementation of the first and second steps is in the data_extraction.py file.

    The Parquet file was chosen due to its optimized compression, for keeping the data schema and to have better performance when being read to build the next layer. As a downside, data in a Parquet file is not human-readable, but this criterion can be ignored at this layer, because it is an intermediary one, not designed to be directly accessed. However, one can read the Parquet content using the appropriate tools.

# 3. Product Master Transformations:
    To build the product master table result, the .parquet file wrote in the previous step is load into duckDB in the FROM command, just as step two.
    To replace the null collors with N/A string a coalesce function was used. To replace the NULL product categories, a nested CASE/WHEN command was used, once this implementation reduces the amount of comparisons need. These implementations are defined in the publish_product.py.
    
# 4. Sales Order Transformations:
    Just as in the previous steps, data from the .parquet file was loaded into DuckDB for the transformations. To join head and detail orders tables, only the salesOrderId key was used. Also, the key matching is indirectly enforced using the INNER JOIN, in such a way that an order has to have both head and detail values to be materialized in the publish_orders table.
    To calculate the LeadTimeInBusinessDays in the most performant way, a Python function which counts the amount of days between two dates and removes the number of Saturdays and Sundays was used. This approach was chosen, once it doesn't require making an expansion of all the days between the two dates to then remove the Saturdays and Sundays. This function was implemented in the utils.__init__ file, and is loaded when the DuckDB connection is made in the publish_orders.py file. Later, the SQL transformations which solve the other requirements are implemented too.
    In order to validate the computation of the business days, a test.py file was implemented, validating both the function and its insertion in a SQL transformation.

# 5. Final questions
    The solutions to the two final questions are in the final_questions.py. The implementations are coded in SQL. In this file, there is the unformatted result of the SQL queries.

## Which color generated the highest revenue each year?
| Year | Color  | Value       |
|------|--------|-------------|
| 2024 | Yellow | 6368158.48  |
| 2023 | Black  | 15047694.37 |
| 2022 | Black  | 14005242.98 |
| 2021 | Red    | 6019614.02  |

## What is the average LeadTimeInBusinessDays by ProductCategoryName?
| Category     | Avg Lead Time (Days) |
|--------------|----------------------|
| Clothing     | 5.01                 |
| Accessories  | 5.01                 |
| Bikes        | 5.01                 |
| None         | 5.01                 |
| Components   | 5.00                 |