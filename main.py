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


tic_C = time.perf_counter()
fNameA = []
lNameA = []
bDayA = []
mailA = []
phoneA = []
addressA = []
stateA = []

dataset_A = dataset_A.reset_index()
for index, row in dataset_A.iterrows():
     fNameA.append(row["First Name"]);
     lNameA.append(row["Last Name"]);
     bDayA.append(row["Birth Date"]);
     mailA.append(row["Email"]);
     phoneA.append(row["Phone"]);
     addressA.append(row["Address"]);
     stateA.append(row["State"]);

fNameB = []
lNameB = []
bDayB = []
mailB = []
phoneB = []
addressB = []
stateB = []

dataset_B = dataset_B.reset_index()
for index, row in dataset_B.iterrows():
    fNameB.append(row["First Name"]);
    lNameB.append(row["Last Name"]);
    bDayB.append(row["Birth Date"]);
    mailB.append(row["Email"]);
    phoneB.append(row["Phone"]);
    addressB.append(row["Address"]);
    stateB.append(row["State"]);

toc_C = time.perf_counter()

print("Lecture des deux datasets")
print("C took : ", toc_C-tic_C, "s")


registreA = [fNameA,lNameA,bDayA,mailA,phoneA,addressA,stateA]
registreB = [fNameB,lNameB,bDayB,mailB,phoneB,addressB,stateB]
print("Registres créés")
for i in range(0,7,1):
    for j in range(i+1,7,1):
        print("i=",i,"j=",j)
        compteur = 0
        lNum = 0
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[i][k] == "" or registreA[j][k] == "":
                upletA.append("")
            else:
                upletA.append(registreA[i][k]+registreA[j][k]) #hasher directement ici
        print("upletA créé")
        for k in range(len(fNameA)):
            if registreB[i][k] == "" or registreB[j][k] == "":
                upletB.append("")
            else:
             upletB.append(registreB[i][k]+registreB[j][k]) #hasher directement ici
        print("upletB créé")
        for k in upletA:
            if not k == "":
                for l in upletB: #traiter le cas du k et l vides et le faire dès leur selection
                    if k == l:
                        compteur+=1 #plutot que juste faire un compteur, faire la liste # des ids
            lNum+=1
            print(lNum)
        print("nombre d'occurences communes =",compteur, "pour i, j =",i,j)











