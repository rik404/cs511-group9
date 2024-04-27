import psycopg2
import time

# Define your PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'cs511',
    'user': 'postgres',
    'password': 'cs511'
}

# Define the queries to execute
queries = [
    """
    SELECT
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    FROM
        lineitem
    WHERE
        l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days'  
    GROUP BY
        l_returnflag,
        l_linestatus
    ORDER BY
        l_returnflag,
        l_linestatus
    """,
    """
    SELECT
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    FROM
        customer,
        orders,
        lineitem
    WHERE
        c_mktsegment = 'HOUSEHOLD'
        AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate < DATE '2000-08-29'
        AND l_shipdate > DATE '1990-08-29'
    GROUP BY
        l_orderkey,
        o_orderdate,
        o_shippriority
    ORDER BY
        revenue DESC,
        o_orderdate
    """,
    """
    SELECT
        o_orderpriority,
        count(*) as order_count
    FROM
        orders
    WHERE
        o_orderdate >= date '1990-01-01'
        AND o_orderdate < date '1996-05-04' + interval '3' month
        AND EXISTS (
            SELECT
                *
            FROM
                lineitem
            WHERE
                l_orderkey = o_orderkey
                AND l_commitdate < l_receiptdate
        )
    GROUP BY
        o_orderpriority
    ORDER BY
        o_orderpriority
    """,
    """
    SELECT
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND l_suppkey = s_suppkey
        AND c_nationkey = s_nationkey
        AND s_nationkey = n_nationkey
        AND n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND o_orderdate >= date '1992-01-01'
        AND o_orderdate < date '1998-04-02' + interval '1' year
    GROUP BY
        n_name
    ORDER BY
        revenue DESC
    """,
    """
    SELECT
        sum(l_extendedprice * l_discount) as revenue
    FROM
        lineitem
    WHERE
        l_shipdate >= date '1990-01-01'
        AND l_shipdate < date '1996-08-09' + interval '1' year
        AND l_discount BETWEEN 0.05 - 0.01 AND 0.05 + 0.01
        AND l_quantity < 40
    """
]

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute each query and measure the time taken
for i, query in enumerate(queries, 1):
    start_time = time.time()
    try:
        cur.execute(query)
        execution_time = time.time() - start_time
        output = cur.fetchall()
        print(f"Query {i}:\n \nOutput: {output}\nExecution Time: {execution_time} seconds\n")
    except psycopg2.Error as e:
        print(f"Error executing query: {query}")
        print(e)

# Close cursor and connection
cur.close()
conn.close()