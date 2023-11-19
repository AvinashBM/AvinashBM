# Sample single list of data
data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

# Split the single list into 10 separate lists
num_splits = 2
split_size = len(data_list) // num_splits

split_lists = [data_list[i:i+split_size] for i in range(0, len(data_list), split_size)]

# Display the split lists
for i, split_list in enumerate(split_lists):
    print(f"Split List {i + 1}: {split_list}")

print(split_lists[1])
