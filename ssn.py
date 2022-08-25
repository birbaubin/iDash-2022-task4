import random
import hashlib
import pandas as pd
import time

tic = time.perf_counter()
SEED=20938
MOD = pow(2, 40)
dataset_B = pd.read_csv("dataB_hash.csv", sep=";").reset_index()
random.seed(SEED)
alpha = random.randint(1, 10)

def txi(element):

    if element == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855":
        return 0
    else:
        hash = pow(int(element, 16), alpha, MOD)
        txi_ = hashlib.sha256(hex(hash).encode('utf-8')).hexdigest()
        return txi_

txi_ = dataset_B['SSN'].apply(txi)
txi_ = txi_[txi_!=0]
txi_result = pd.DataFrame(txi_)

print("txi calculated")

dataset_A = pd.read_csv("dataA_hash.csv", sep=";").reset_index()
beta_array = []
inv_beta_array = []

def a_j(element):

    if element == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855":
        beta_array.append(0)
        inv_beta_array.append(0)
        return 0
    else:
        found = False
        while not found:
            beta_j = random.randint(1, 10)
            try:
                inv_beta_j = pow(beta_j, -1, MOD)
                found = True
                beta_array.append(beta_j)
                inv_beta_array.append(inv_beta_j)
            except ValueError:
                continue

        return pow(int(element, 16), beta_j, MOD)

a_j_ = dataset_A["SSN"].apply(a_j)
a_j_ = a_j_[a_j_!=0]
print("a j calculated")


def a_prime_j(element):
    a = pow(int(element), alpha, MOD)
    return a

a_prime_j_ = a_j_.apply(a_prime_j)
print("a prime j calculated")


def tyi(row):
    tmp = pow(int(row['SSN']), inv_beta_array[row.name], MOD)
    tyi_ = hashlib.sha256(hex(tmp).encode('utf-8')).hexdigest()
    return tyi_

tyi_result = pd.DataFrame(a_prime_j_, columns=['SSN'])

tyi_result["tyi"] = tyi_result.apply(tyi, axis=1)

print("tyi calculated")

common_hashes = set(txi_result['SSN']).intersection(set(tyi_result['tyi']))
len(common_hashes)

index_A_list= []
index_B_list= []
SSN_A_list= []
SSN_B_list= []
for val in common_hashes:
    index_B = int(txi_result[txi_result['SSN'] == val].index[0])
    index_A = int(tyi_result[tyi_result['tyi'] == val].index[0])
    index_B_list.append(index_B)
    index_A_list.append(index_A)
    SSN_A_list.append(dataset_A.iloc[index_A]['SSN'])
    SSN_B_list.append(dataset_B.iloc[index_B]['SSN'])

data = {'id_A':index_A_list, 'id_B':index_B_list, 'SSN_A':SSN_A_list, 'SSN_B':SSN_B_list}
result = pd.DataFrame(data, columns=['id_A', 'id_B', 'SSN_A', 'SSN_B'])

toc = time.perf_counter()
print("SSN linked in ", toc-tic, "s")
print(result)





#%%
