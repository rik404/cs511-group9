import os
import pandas as pd
original_csv_path = '/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/Snow/snow_line_item.csv'
column_names = ['orderKey', 'partKey', 'supplierKey', 'lineNumber', 'quantity', 'extendedPrice', 'discount', 'tax', 'returnFlag', 'status', 'shipDate', 'commitDate', 'receiptDate', 'shipInstructions', 'shipMode', 'comment']
df_original = pd.read_csv(original_csv_path, header=None, names=column_names)
batch_size = 1000
num_batches = len(df_original) // batch_size + (1 if len(df_original) % batch_size != 0 else 0)
output_dir = '/Users/krishnaanandan/Desktop/CS511/cs511-group9/Redis/lineitem_batches'
os.makedirs(output_dir, exist_ok=True)
for i in range(num_batches):
    start_index = i * batch_size
    end_index = min((i + 1) * batch_size, len(df_original))
    df_batch = df_original.iloc[start_index:end_index]
    batch_csv_path = os.path.join(output_dir, f'snow_lineitem_{i}.csv')
    df_batch.to_csv(batch_csv_path, index=False)
    print(f'Saved batch {i} to {batch_csv_path}')