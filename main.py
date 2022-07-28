import time
import pandas as pd
from bloom_filter import BloomFilter
from numpy import array,zeros


#tic_A = time.perf_counter()

dataset_A = pd.read_csv("dataA_hash_hs.csv")
dataset_A = dataset_A.reset_index()
""""
bloom_filter = BloomFilter(max_elements=2000000, error_rate=0.000001)

for index, row in dataset_A.iterrows():
    if not pd.isnull(row["SSN"]):
        bloom_filter.add(row["SSN"])


toc_A = time.perf_counter()
print("A took : ", toc_A-tic_A, "s")

tic_B = time.perf_counter()
"""
dataset_B = pd.read_csv("dataB_hash_hs.csv")
dataset_B = dataset_B.reset_index()

""""
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
"""

registre = ["First Name","Last Name","Birth Date","Email","Phone","Address","State"]

print("Registres créés")
for i in range(0,7,1):
    for j in range(i+1,7,1):
        print("i=",i,"j=",j)
        dataset_A = dataset_A.reset_index()
        dataset_B = dataset_B.reset_index()
        compteur = 0
        lNum = 0
        upletA = []
        upletB = []
        idA = []
        idB = []
        for index, row in dataset_A.iterrows():
            if pd.isnull(row[registre[i]]) or pd.isnull(row[registre[j]]):
                upletA.append("")
            else:
                upletA.append(row[registre[i]]+row[registre[j]]) #hasher directement ici
        print("upletA créé")
        for index, row in dataset_B.iterrows():
            if pd.isnull(row[registre[i]]) or pd.isnull(row[registre[j]]):
                upletB.append("")
            else:
             upletB.append(row[registre[i]]+row[registre[j]]) #hasher directement ici
        print("upletB créé")
        for k in range(len(upletA)):
            if not upletA[k] == "":
                for l in range(len(upletB)):
                    if upletA[k] == upletA[l]:
                        idA.append(k)
                        idB.append(l)
            lNum+=1
            print(lNum)
        C={'idA':idA,'idB':idB}
        donnees = pd.DataFrame(C,columns=['idA','idB'])
        export_csv = pd.dt.to_csv(str(i)+str(j)+'linked.csv', index = None, header=True, encoding='utf-8', sep=';')
        print(donnees)

