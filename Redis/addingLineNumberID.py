import pandas as pd
import sys
def add_linenumber_id(input_file):
    df = pd.read_csv(input_file)
    df.insert(0, 'linenumber_id', range(1, len(df) + 1))
    output_file = 'modified_flat_line_item.csv'
    df.to_csv(output_file, index=False)
    print(f"Modified CSV saved as: {output_file}")
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file_path>")
        sys.exit(1)
    input_file = sys.argv[1]
    add_linenumber_id(input_file)