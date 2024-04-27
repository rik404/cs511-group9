import psycopg2

# Define your PostgreSQL connection parameters
db_params = {
    'host': 'localhost',
    'port': '5432',
    'database': 'cs511',
    'user': 'postgres',
    'password': 'cs511'
}

# Define the list of ALTER TABLE statements
alter_statements = [
    "ALTER TABLE region ADD PRIMARY KEY (r_regionkey)",
    "ALTER TABLE nation ADD PRIMARY KEY (n_nationkey)",
    "ALTER TABLE nation ADD CONSTRAINT nation_region_fk FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey)",
    "ALTER TABLE part ADD PRIMARY KEY (p_partkey)",
    "ALTER TABLE customer ADD PRIMARY KEY (c_custkey)",
    "ALTER TABLE customer ADD CONSTRAINT customer_nation_fk FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey)",
    "ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey)",
    "ALTER TABLE orders ADD PRIMARY KEY (o_orderkey)",
    "ALTER TABLE orders ADD CONSTRAINT orders_customer__fk FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)",
    "ALTER TABLE partsupplier ADD CONSTRAINT ps_supplier__fk FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey)",
    "ALTER TABLE partsupplier ADD CONSTRAINT ps_part__fk FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey)",
    "ALTER TABLE lineitem ADD CONSTRAINT lineitem_orders__fk FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey)",
    "ALTER TABLE lineitem ADD CONSTRAINT lineitem_part__fk FOREIGN KEY (l_partkey) REFERENCES part (p_partkey)",
    "ALTER TABLE lineitem ADD CONSTRAINT lineitem_partsupplier__fk FOREIGN KEY (l_suppkey) REFERENCES supplier (s_suppkey)"
]

# Connect to PostgreSQL
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute ALTER TABLE statements
for statement in alter_statements:
    try:
        cur.execute(statement)
        conn.commit()
        print(f"Successfully executed: {statement}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error executing statement: {statement}")
        print(e)

# Close cursor and connection
cur.close()
conn.close()