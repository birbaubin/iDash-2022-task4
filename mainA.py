import math
import time
import numpy as np
import pandas as pd
import hashlib
import numpy
import secrets


dataset_A = pd.read_csv("dataAEn.csv")  # Opening dataset A
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty

alpha = (secrets.randbits(50)+1)*2 #choose the security value



def extratingData(dataset):
    fName = []  # first name of Dataset
    lName = []  # last name of Dataset
    bDay = []  # birth date of Dataset
    mail = []  # email of Dataset
    phone = []  # phone number of Dataset
    address = []  # address of Dataset
    SSN = []  # SSN of Dataset
    boolean = []  # A value that allow the function to know whether an element of Dataset is already considered as linked
    missing = []  # A integer to map missing values
    missingCount = []  # A number of missing values
    # %%
    dataset = dataset.reset_index()
    for index, row in dataset.iterrows():  # Read of Dataset A
        fName.append(row["First Name"])
        lName.append(row["Last Name"])
        bDay.append(row["Birth Date"])
        mail.append(row["Email"])
        phone.append(row["Phone"])
        address.append(row["Address"])
        SSN.append(row["SSN"])
        boolean.append(False)
        missing.append(0)
        missingCount.append(0)

    registre = [fName, lName, bDay, mail, phone, address, SSN, missing, boolean, missingCount]

    for i in range(len(missing)):
        missing = 0
        missingCount = 0
        if registre[2][i] == empty:
            missing += 1
            missingCount += 1
        if registre[3][i] == empty:
            missing += 2
            missingCount += 1
        if registre[4][i] == empty:
            missing += 4
            missingCount += 1
        if registre[5][i] == empty:
            missing += 8
            missingCount += 1
        if registre[6][i] == empty:
            missing += 16
            missingCount += 1

        registre[7][i] = str(missing)
        registre[9][i] = missingCount
    return registre

def creatingTuple2(registre, tuple,p):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty  or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            uplet.append(hashlib.sha256((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),alpha,p))).encode('utf-8')).hexdigest())
    return(uplet)

def creatingTuple3(registre, tuple,p):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append(hashlib.sha256((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),alpha,p))).encode('utf-8')).hexdigest()) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

#get p,q from B
p=0
q=0

def createTupleA(dataset_A,p):
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreA = extratingData(dataset_A)

    list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]])

    for f in range(len(list)):

        if len(list[f]) == 2:
            upletA = creatingTuple2(registreA,list[f],p)
        else:
            upletA = creatingTuple3(registreA,list[f],p)
        #send upletA to B

        #get tuple from B (H(x)^beta)
        tuple = []
        for i in range(len(tuple)):
            tuple[i] = pow(tuple[i],alpha,p)
        #send tuple to B
        #get idA from B
        idA = []
        for i in range(len(idA)):
            registreA[idA[i]] = True

    C = {'Value': registreA[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset A. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')






