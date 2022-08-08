import random
import hashlib
import pandas as pd

SEED=20938

MOD = pow(2, 1024)

dataset_B = pd.read_csv("dataB_hash.csv", sep=";").reset_index()
random.seed(SEED)
alpha = random.randint(1, 10)


def txi(element):

    if element == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855":
        return 0
    else:
        hash = pow(int(element, 16), alpha, MOD)
        txi_ = hashlib.sha256(hex(hash).encode('utf-8')).hexdigest()
        hash = hash[:4] + hash[len(hash)//2-1:len(hash)//2+1] + hash[-2:]
        return txi_

txi_ = dataset_B['SSN'].apply(txi)
txi_ = txi_[txi_!=0]
txi_ = txi_.sort_values()
# txi_

dataset_A = pd.read_csv("dataA_hash.csv", sep=";").reset_index()
beta_array = []
def a_j(element):

    beta_j = random.randint(1, 10)
    beta_array.append(beta_j)
    if element == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855":
        return "0"
    else:
        return pow(int(element, 16), beta_j, MOD)

a_j_ = dataset_A["SSN"].apply(a_j)
# a_j_

def a_prime_j(element):
    a = pow(int(element), alpha, MOD)
    return a

a_prime_j_ = a_j_.apply(a_prime_j)
# a_prime_j_[0]

def tyi(element, beta_j):

    tmp = round(pow(element, 1/beta_j))
    # print("tmp for B = ", tmp)
    tyi_ = hashlib.sha256(hex(tmp).encode('utf-8')).hexdigest()
    # tyi_ = hash1([tmp])
    return tyi_

tyi_ = []
for i in range(0,500000):
    if a_prime_j_[i]!=0:
        tyi_.append(tyi(a_prime_j_[i], beta_array[i]))

tyi_.sort()

# tyi_

number_of_match = 0

for i in range(0, len(txi_)-1):
    x_i = txi_.iloc[i]
    j = 0
    while j < len(tyi_) and tyi_[j] < x_i:
        j+=1
    if x_i==tyi_[j]:
        print("i=", i, " j=", j)
        number_of_match += 1
        continue

print(number_of_match)