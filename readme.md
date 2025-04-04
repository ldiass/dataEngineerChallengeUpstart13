# Initial considerations
    For solve the challenge, a SQL implementation where prefered, once it could solve the questions with a simple interface and can be hosted into multiple services with few compatibility issues. The SQLs are handled in python scripts, in which a in-memory duckDB instance is created.
    Additionally, in order to execute all the transforamtion stages in a single step, a main file was implemented, in which all the tables are loaded into the same duckDB instance.

# 1. Data Loading:
    Data loading was executed copying the data from the source .csv file provided, with the raw prefix, with duckDB a OLAP DBMS which is able to run in memory. For this challenge, it was assumed files provided are the raw ones and no data transformation was implemented in this first step, only name of the files were changed.
    
# 2. Data review and storage 
    Once the data is available in duckDB through a SELECT directly from the source raw files, a few transformations are applied. In this step the data types and keys relationships are enforced. This new dataset is materialized in tables with the storage prefix. These duckDB tables, in this architecture are trasitory, and therefore, the result is dumped into a .parquet file and the connection is close, finalizing the database instance. The implementation of the first and second steps are in the data_extraction.py file.

    The parquet file was choosen due to its optimized compactation, for keeping the data schema and to have a better performance when being read to build the next layer. As a downside, data in parquet file is not human readable, but this criteria can be ignored at this layer, because is an intermediary one, not designed to be directly accessed. However, one can read the parquet content using the appropriate tools.

# 3. Product Master Transformations:
    To build the product master table result, the .parquet file wrote in the previous step is load into duckDB in the FROM command, just as step two.
    To replace the null collors with N/A string a coalesce function was used. To replace the NULL product categories, a nested CASE/WHEN command was used, once this implementation reduces the amount of comparisons need. These implementations are defined in the publish_product.py.
    
# 4. Sales Order Transformations:
    Just as in the previous steps, data from the .parquet file was loaded into duckDB for the transformations. For join head and detail orders tables, only the salesOrderId key was used. Also, the keys matching is undirectly enforced using the INNER JOIN, in such a way a order has to have both head and detail values to be materialized in the publish_orders table.
    To calculate the LeadTimeInBusinessDays in the most performatic way, a python function which counts the amount of days between two dates and removes the number of Saturdays and Sundays was used. This approach was chosen, once it doesn't required to make a expension of all the day between the two dates to then remove the Saturdays and Sundays. This function was implemented in the utils.__init__ file, and is loaded when the duckDB connection is made in the publish_orders.py file. In later, the SQL transformations which solve the other requirements are implemented too.
    In order to validate the computation of the business days, a test.py file was implemented, validating both the function and it's insertion in a SQL transformation.

# 5. Final questions
    The solutions to the two final questions are in the final_questions.py. The implementations are coded in SQL. In this file there is the unformated result of the SQL queries.