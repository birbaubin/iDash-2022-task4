import time
import pandas as pd
from bloom_filter import BloomFilter


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
fNameA = []
lNameA = []
bDayA = []
mailA = []
phoneA = []
addressA = []
stateA = []

#%%
dataset_A = dataset_A.reset_index()
for index, row in dataset_A.iterrows():
    fNameA.append(row["First Name"])
    lNameA.append(row["Last Name"])
    bDayA.append(row["Birth Date"])
    mailA.append(row["Email"])
    phoneA.append(row["Phone"])
    addressA.append(row["Address"])
    stateA.append(row["State"])

fNameB = []
lNameB = []
bDayB = []
mailB = []
phoneB = []
addressB = []
stateB = []

dataset_B = dataset_B.reset_index()
for index, row in dataset_B.iterrows():
    fNameB.append(row["First Name"])
    lNameB.append(row["Last Name"])
    bDayB.append(row["Birth Date"])
    mailB.append(row["Email"])
    phoneB.append(row["Phone"])
    addressB.append(row["Address"])
    stateB.append(row["State"])

registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA]
registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB]


for i in range(0, 7, 1): #2-uplets
    for j in range(i+1, 7, 1):
        print("i=", i, "j=", j)
        lNum = 0
        upletA = []
        upletB = []
        idA = []
        idB = []
        for k in range(len(fNameA)):
            if registreA[i][k] == "" or registreA[j][k] == "":
                upletA.append("")
            else:
                upletA.append(registreA[i][k]+registreA[j][k]) #hasher directement ici
        print("upletA créé")
        for k in range(len(fNameB)):
            if registreB[i][k] == "" or registreB[j][k] == "":
                upletB.append("")
            else:
             upletB.append(registreB[i][k]+registreB[j][k]) #hasher directement ici
        print("upletB créé")
        for k in range(len(upletA)):
            if not upletA[k] == "":
                for l in range(len(upletB)):
                    if upletA[k] == upletA[l]:
                        idA.append(k)
                        idB.append(l)
            lNum += 1
            print(lNum)
        C = {'idA': idA, 'idB': idB}
        donnees = pd.DataFrame(C,columns=['idA','idB'])
        export_csv = pd.dt.to_csv(str(i)+str(j)+'linked.csv', index=None, header=True, encoding='utf-8', sep=';')
        print(donnees)

for i in range(0, 7, 1): #3-uplets
    for j in range(i+1, 7, 1):
        for a in range(j+1, 7, 1):
            print("i=", i, "j=", j, "a", a)
            lNum = 0
            upletA = []
            upletB = []
            idA = []
            idB = []
            for k in range(len(fNameA)):
                if registreA[i][k] == "" or registreA[j][k] == "" or registreA[a][k] == "":
                    upletA.append("")
                else:
                    upletA.append(registreA[i][k]+registreA[j][k]+registreA[a][k]) #hasher directement ici
            print("upletA créé")
            for k in range(len(fNameB)):
                if registreB[i][k] == "" or registreB[j][k] == "" or registreB[a][k] == "":
                    upletB.append("")
                else:
                    upletB.append(registreB[i][k]+registreB[j][k]+registreB[a][k]) #hasher directement ici
            print("upletB créé")
            for k in range(len(upletA)):
                if not upletA[k] == "":
                    for l in range(len(upletB)):
                        if upletA[k] == upletA[l]:
                            idA.append(k)
                            idB.append(l)
                lNum += 1
                print(lNum)
            C = {'idA': idA, 'idB': idB}
            donnees = pd.DataFrame(C, columns=['idA', 'idB'])
            export_csv = pd.dt.to_csv(str(i)+str(j)+str(a)+'linked.csv', index=None, header=True, encoding='utf-8', sep=';')
            print(donnees)

for i in range(0, 7, 1): #4-uplets
    for j in range(i+1, 7, 1):
        for a in range(j+1, 7, 1):
            for b in range(a+1, 7, 1):
                print("i=", i, "j=", j, "a", a, "b", b)
                lNum = 0
                upletA = []
                upletB = []
                idA = []
                idB = []
                for k in range(len(fNameA)):
                    if registreA[i][k] == "" or registreA[j][k] == "" or registreA[a][k] == "" or registreA[b][k] == "":
                        upletA.append("")
                    else:
                        upletA.append(registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]) #hasher directement ici
                print("upletA créé")
                for k in range(len(fNameB)):
                    if registreB[i][k] == "" or registreB[j][k] == "" or registreB[a][k] == "" or registreB[b][k] == "":
                        upletB.append("")
                    else:
                        upletB.append(registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]) #hasher directement ici
                print("upletB créé")
                for k in range(len(upletA)):
                    if not upletA[k] == "":
                        for l in range(len(upletB)):
                            if upletA[k] == upletA[l]:
                                idA.append(k)
                                idB.append(l)
                    lNum += 1
                    print(lNum)
                C = {'idA': idA, 'idB': idB}
                donnees = pd.DataFrame(C,columns=['idA', 'idB'])
                export_csv = pd.dt.to_csv(str(i)+str(j)+str(a)+str(b)+'linked.csv', index=None, header=True, encoding='utf-8', sep=';')
                print(donnees)

for i in range(0, 7, 1): #5-uplets
    for j in range(i+1, 7, 1):
        for a in range(j+1, 7, 1):
            for b in range(a+1, 7, 1):
                for c in range(b+1, 7, 1):
                    print("i=", i, "j=", j, "a", a, "b", b, "c", c)
                    lNum = 0
                    upletA = []
                    upletB = []
                    idA = []
                    idB = []
                    for k in range(len(fNameA)):
                        if registreA[i][k] == "" or registreA[j][k] == "" or registreA[a][k] == "" or registreA[b][k] == "" or registreA[c][k] == "":
                            upletA.append("")
                        else:
                            upletA.append(registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]+registreA[c][k]) #hasher directement ici
                    print("upletA créé")
                    for k in range(len(fNameB)):
                        if registreB[i][k] == "" or registreB[j][k] == "" or registreB[a][k] == "" or registreB[b][k] == "" or registreB[c][k] == "":
                            upletB.append("")
                        else:
                            upletB.append(registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]+registreB[c][k]) #hasher directement ici
                    print("upletB créé")
                    for k in range(len(upletA)):
                        if not upletA[k] == "":
                            for l in range(len(upletB)):
                                if upletA[k] == upletA[l]:
                                    idA.append(k)
                                    idB.append(l)
                        lNum += 1
                        print(lNum)
                    C = {'idA': idA, 'idB': idB}
                    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
                    export_csv = pd.dt.to_csv(str(i)+str(j)+str(a)+str(b)+str(c)+'linked.csv', index=None, header=True, encoding='utf-8', sep=';')
                    print(donnees)