import csv
input_file = "/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/Snow/snow_customer.csv"
output_file = "preprocessed_snow_customer.csv"
removed_custkeys_file = "removed_custkeys.txt"
removed_custkeys = []
with open(input_file, "r", newline="") as infile, open(output_file, "w", newline="") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    header = next(reader)
    writer.writerow(header)
    removed_count = 0
    for row in reader:
        if len(row[2]) >= 5:
            writer.writerow(row)
        else:
            removed_custkeys.append(row[0])
            removed_count += 1
with open(removed_custkeys_file, "w") as removed_file:
    for custkey in removed_custkeys:
        removed_file.write(f"{custkey}\n")
print("Rows with c_address length >= 5 filtered. Filtered data saved to:", output_file)
print("Rows removed:", removed_count)
print("c_custkey of removed rows saved to:", removed_custkeys_file)