import csv
import os
file_headers = {
    'snow_customer.csv': ['c_customerkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment'],
    'snow_supplier.csv': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'],
    'snow_part.csv': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'],
    'snow_nation.csv': ['n_nationkey', 'n_name', 'n_regionkey'],
    'snow_part_supplier.csv': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
    'snow_order.csv': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
    'snow_line_item.csv': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
    'snow_region.csv':['r_regionkey','r_name']
}
directory = 'Snow/'
for filename, headers in file_headers.items():
    file_path = os.path.join(directory, filename)
    if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            first_row = next(reader)
        if first_row != headers:
            with open(file_path, 'r', newline='') as file:
                data = file.readlines()
            data.insert(0, ','.join(headers) + '\n')
            with open(file_path, 'w', newline='') as file:
                file.writelines(data)
            print("Headers added successfully to", filename)
        else:
            print("Headers already exist in", filename)
    else:
        with open(file_path, 'w', newline='') as file:
            file.write(','.join(headers) + '\n')
        print("Headers added successfully to", filename)