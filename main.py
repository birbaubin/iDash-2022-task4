# import pandas as pd
# from bloom_filter import BloomFilter
#
#
# def run_A():
#     dataset_A = pd.read_csv("dataA_hash_hs.csv")
#     dataset_A = dataset_A.reset_index()
#     bloom = BloomFilter(max_elements=500000, error_rate=0.1)
#
#     dataset_A.describe()
#
#     # for index, row in dataset_A.iterrows():
#     #     if row["SSN"]:
#     #         print(row["SSN"])
#
#
# run_A()
