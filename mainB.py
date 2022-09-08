import math
import time
import numpy as np
import pandas as pd
import hashlib
import numpy
import secrets
import sympy

dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty
size_q =60 #choose the security value
beta = (secrets.randbits(50)+1)*2 #choose the security value

def param(size_q):
    q = sympy.nextprime(pow(2, size_q)+secrets.randbits(size_q))
    p = 2*q+1
    while sympy.isprime(p)==False:
        q = sympy.nextprime(q)
        p = 2*q+1
    return p,q
p,q = param(size_q)
#transfer p,q to A

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

def creatingTuple2(registre, tuple):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8'))
    return(uplet)

def creatingTuple3(registre, tuple):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8')) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked or with less than missingCount missing values
        else:
            uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8')) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def creatingTupleMissing3(registre, tuple,missingCount):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] + registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8'))  # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def compareTuple(upletA, upletB, idA, idB, BooleanA, BooleanB):
    indexA = numpy.argsort(upletA) #Sorting the hashes while keeping in memory the ID of the hashes
    upletA = numpy.sort(upletA)
    indexB = numpy.argsort(upletB) #Sorting the hashes while keeping in memory the ID of the hashes
    upletB = numpy.sort(upletB)


    l = 0
    for k in range(0, 500000, 1):  # Efficient comparison of sorted list
        if not upletA[k] == "":  # verifying that the k-th tuple wasn't already linked or that one of its component was empty
            while l < 500000 and upletA[k] > upletB[l]:
                l += 1
            if l < 500000 and upletA[k] == upletB[l]:  # if the hashes are equals, the IDs are linked
                idA.append(indexA[k] + 2)  # we add the ID to the lists of linked IDs
                idB.append(indexB[l] + 2)
                BooleanA[indexA[k]] = True  # We set the ID as already linked
                BooleanB[indexB[l]] = True
                l += 1
    return

def createTupleBp1(dataset_B):
    #get the tuples from A
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreB = extratingData(dataset_B)

    list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]])

    for f in range(len(list)):

        if len(list[f]) == 2:
            upletB = creatingTuple2(registreB,list[f],p)
        else:
            upletB = creatingTuple3(registreB,list[f],p)
        #send upletB to A

    list = np.array([[2,7],[5,7],[0,1,7],[0,4,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3,3]

    for f in range(len(list)):
        if len(list[f]) ==2:
            upletB = creatingTupleMissing2(registreB,list[f],missing[f])
        else:
            upletB = creatingTupleMissing3(registreB,list[f],missing[f])
        #send upletB to A

def createTupleBp2(tupleListA):
    invBeta = pow(beta, -1,q)
    #get the tuples from A
    tupleListB = []

    idA = []  # The two list that will save the ID of linked elements
    idB = []

    BooleanA = []

    for i in range(len(tupleListB[0])):
        BooleanA.append(False)

    BooleanB = BooleanA

    for i in range(len(tupleListB)):
        for j in range(len(tupleListB[0])):
            tupleListB[i][j]=pow(tupleListB[i][j],invBeta,p)

    for i in range(len(tupleListB)):
        compareTuple((tupleListA,tupleListB,idA,idB,BooleanA,BooleanB))

    #send outputA, linkage to A






