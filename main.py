import time
import pandas as pd
from bloom_filter import BloomFilter


tic_A = time.perf_counter()

dataset_A = pd.read_csv("dataA_hash_hs.csv")
dataset_A = dataset_A.reset_index()
bloom_filter = BloomFilter(max_elements=170000, error_rate=0.0001)

for index, row in dataset_A.iterrows():
    if not pd.isnull(row["SSN"]):
        bloom_filter.add(row["SSN"])


toc_A = time.perf_counter()
print("A took : ", toc_A-tic_A, "s")

tic_B = time.perf_counter()
dataset_B = pd.read_csv("dataB_hash_hs.csv")
dataset_B = dataset_B.reset_index()


ids = 0
for index, row in dataset_B.iterrows():
    if not pd.isnull(row["SSN"]):
        try:
            assert row["SSN"] not in bloom_filter
        except AssertionError:
            print(row["SSN"], "is in DB A" )
            ids = ids + 1

print("Number of identified elements ", ids)

toc_B = time.perf_counter()
print("B took : ", toc_B-tic_B, "s")
#%%

#%%
