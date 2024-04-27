order_file = "/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/Snow/snow_order.csv"
removed_custkeys_file = "removed_custkeys.txt"
cleaned_order_file = "preprocessed_snow_order.csv"
removed_orderkeys_file = "removed_orderkeys.txt"
with open(removed_custkeys_file, "r") as f:
    removed_custkeys = set(map(int, f.readlines()))
header = None
lines_after = []
removed_orderkeys = set()
with open(order_file, "r") as f:
    for line in f:
        if header is None:
            header = line
            lines_after.append(header)
        else:
            orderkey = int(line.split(",")[0])
            if int(line.split(",")[1]) not in removed_custkeys:
                lines_after.append(line)
            else:
                removed_orderkeys.add(orderkey)
with open(cleaned_order_file, "w") as f:
    f.writelines(lines_after)
with open(removed_orderkeys_file, "w") as f:
    for orderkey in removed_orderkeys:
        f.write(f"{orderkey}\n")
num_lines_before = len(lines_after) if header else 0
num_lines_after = len(lines_after)
print("Number of lines before cleansing:", num_lines_before)
print("Number of lines after cleansing:", num_lines_after)