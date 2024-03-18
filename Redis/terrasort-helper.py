import csv
import random
import pandas as pd

def generate_serial_number():
    return f"{random.randint(0, 9999):04}-{random.randint(0, 9999):04}-{random.randint(0, 99999):05}"
num_rows = 100000
csv_filename = 'TerraSort_Cap.csv'
with open(csv_filename, 'w', newline='') as csvfile:
    fieldnames = ['Year of manufacture', 'Serial number']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for _ in range(num_rows):
        year_of_manufacture = random.randint(1975, 2050)
        serial_number = generate_serial_number()
        writer.writerow({'Year of manufacture': year_of_manufacture, 'Serial number': serial_number})
print("TerraSort_Cap.csv file generated successfully.")
df = pd.read_csv(csv_filename)
df = df[df['Year of manufacture'] > 2023]
df_sorted = df.sort_values(by=['Year of manufacture', 'Serial number'], ascending=[False, True])
csv_sorted_filename = 'TerraSort_Cap_PreSorted.csv'
df_sorted.to_csv(csv_sorted_filename, index=False,header=False)
print(f"{csv_sorted_filename} file generated successfully.")