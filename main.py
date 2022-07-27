import time
import pandas as pd
from bloom_filter import BloomFilter
from numpy import array,zeros


tic_A = time.perf_counter()

dataset_A = pd.read_csv("dataA_hash_hs.csv")
dataset_A = dataset_A.reset_index()
bloom_filter = BloomFilter(max_elements=2000000, error_rate=0.000001)

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
            ids = ids + 1

print("Number of identified elements ", ids)

toc_B = time.perf_counter()
print("B took : ", toc_B-tic_B, "s")
tic_C = time.perf_counter()
fName = []
lName = []
bDay = []
mail = []
phone = []
address = []
state = []

dataset_A = dataset_A.reset_index()
for index, row in dataset_A.iterrows():
     fName.append(row["First Name"]);
     lName.append(row["Last Name"]);
     bDay.append(row["Birth Date"]);
     mail.append(row["Email"]);
     phone.append(row["Phone"]);
     address.append(row["Address"]);
     state.append(row["State"]);

toc_C = time.perf_counter()

print("Lecture d'un dataset")
print("C took : ", toc_C-tic_C, "s")