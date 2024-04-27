line_item_file = "/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/Snow/snow_line_item.csv"
removed_orderkeys_file = "removed_orderkeys.txt"
cleaned_line_item_file = "preprocessed_snow_line_item.csv"
with open(removed_orderkeys_file, "r") as f:
    removed_orderkeys = set(map(int, f.readlines()))
header = None
lines_after = []
with open(line_item_file, "r") as f:
    for line in f:
        if header is None:
            header = line
            lines_after.append(header)
        else:
            orderkey = int(line.split(",")[1])
            if orderkey not in removed_orderkeys and orderkey != 0:
                lines_after.append(line)
with open(cleaned_line_item_file, "w") as f:
    f.writelines(lines_after)
num_lines_before = len(lines_after) if header else 0
num_lines_after = len(lines_after)
print("Number of lines before cleansing:", num_lines_before)
print("Number of lines after cleansing:", num_lines_after)