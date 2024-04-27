from redis import Redis
r = Redis(host="0.0.0.0", port=6379, db=2)
revenue = 0
keys = r.scan_iter()

for key in keys:
    line_item_data = r.hgetall(key)
    if b'l_shipdate' in line_item_data and b'l_discount' in line_item_data and b'l_extendedprice' in line_item_data and b'l_quantity' in line_item_data:
        l_shipdate = line_item_data[b'l_shipdate'].decode()
        l_discount = float(line_item_data[b'l_discount'])
        l_extendedprice = float(line_item_data[b'l_extendedprice'])
        l_quantity = int(line_item_data[b'l_quantity'])
        if ('1990-01-01' <= l_shipdate < '1996-08-09' and
            0.04 <= l_discount <= 0.06 and
            l_quantity < 40):
            revenue += l_extendedprice * l_discount
print(f"Total revenue: {revenue}")