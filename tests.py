from src.utils import business_days_calc
import duckdb
from duckdb.typing import *

if __name__=='__main__':
    try:
        #Start Sunday, ends 10 days 
        assert business_days_calc(10, 0)==8, "Test 1 failed"

        #Start Sunday, ends 13 days 
        assert business_days_calc(13, 0)==10, "Test 2 failed"

        #Start Thuesday, ends 6 days 
        assert business_days_calc(6, 2)==4, "Test 3 failed"

        #Start Friday, ends 10 days 
        assert business_days_calc(10, 5)==6, "Test 4 failed"

        #Start Saturday, ends 9 days 
        assert business_days_calc(9, 6)==6, "Test 5 failed"

        #Start Thursday, ends 5 days 
        assert business_days_calc(5, 4)==3, "Test 6 failed"

        #Start Friday, ends 8 days 
        assert business_days_calc(8, 5)==5, "Test 7 failed"

        #Test on duckDB

        #2024-02-26	2024-03-05
        conn = duckdb.connect(":memory:")
        conn.create_function('business_days_calc', business_days_calc, [BIGINT, BIGINT], BIGINT)
        sql="SELECT business_days_calc(datediff('day', '2024-02-26'::date, '2024-03-05'::date), datepart('dow','2024-02-26'::date))"
        conn.execute(sql)
        r=conn.fetchall()
        conn.close()
        assert r[0][0]==6, "Test duckDB 1 failed"

        print("All tests passed")

    except AssertionError as e:
        print(f"{e}")

