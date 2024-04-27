import csv
csv_file_path = "/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/Snow/snow_line_item.csv"
ship_mode_values = set()
with open(csv_file_path, 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        ship_mode_values.add(row['shipMode'])
print("Unique shipMode values:")
for value in ship_mode_values:
    print(value)
num_unique_ship_modes = len(ship_mode_values)
print(f"\nNumber of unique shipMode values: {num_unique_ship_modes}")