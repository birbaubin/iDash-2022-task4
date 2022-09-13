import json
import math
import socket
import time
import numpy as np
import pandas as pd
import hashlib
import numpy
import secrets

start = time.perf_counter()
dataset_A = pd.read_csv("dataAEn.csv")  # Opening dataset A
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'
s = socket.socket()         # Create a socket object
host = socket.gethostbyname("") # Get local machine name
port = 12345
s.connect((host, port))

def receiveUplet():
    uplet = []
    for i in range(5000):
        # print(i)
        result = s.recv(1048576)
        # print(result)
        json_data = json.loads(result.decode())
        uplet = uplet + json_data.get(str(i))
        s.sendall(b'ok')


    return uplet

def sendUplet(uplet):
    for i in range(5000):
        #send upletA to B
        # print(upletA)
        json_data = json.dumps({str(i): uplet[i*100:i*100+100]})
        # print(json_data.encode())
        s.sendall(json_data.encode())
        ok = s.recv(16)


def receiveIdA():

    array = []
    end = False
    i = 0
    while not end:
        result = s.recv(1048576)
        # print(result)
        json_data = json.loads(result.decode())
        id = json_data.get(str(i))
        if id != "end":
            array = array + id
        else:
            end = True

        i+=1

    return array
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
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty  or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest())  # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def creatingTuple3(registre, tuple):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] + registre[tuple[2]][k]).encode('utf-8')).hexdigest()) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked or with less than missingCount missing values
        else:
            uplet.append(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest()) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def creatingTupleMissing3(registre, tuple,missingCount):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] + registre[tuple[2]][k]).encode('utf-8')).hexdigest())  # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def linkage(dataset_A):

    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreA = extratingData(dataset_A)
    BooleanA = []
    for i in range(0,500000):
        BooleanA.append(False)


    list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]])

    for f in range(len(list)):
        print("######## ",time.perf_counter() - start," : Tuple number ", f + 1, "###########")

        if len(list[f]) == 2:
            upletA = creatingTuple2(registreA,list[f])
        else:
            upletA = creatingTuple3(registreA,list[f])

        sendUplet(upletA)

        idA = receiveIdA()
        print(time.perf_counter() - start, " : 4/4 : IdA received")
        print("Number of linked records received for this tuple : ", len(idA))

        for i in range(len(idA)):
            registreA[8][int(idA[i])] = True

    list = np.array([[2,7],[5,7],[0,1,7],[0,4,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3,3]

    for f in range(len(list)):

        print("######## ",time.perf_counter() - start," : Tuple number ", f + 9, "###########")
        if len(list[f]) ==2:
            upletA = creatingTupleMissing2(registreA,list[f],missing[f])
        else:
            upletA = creatingTupleMissing3(registreA,list[f],missing[f])

        sendUplet(upletA)

        idA = receiveIdA()
        print(time.perf_counter() - start, " : 4/4 : IdA received")
        print("Number of linked records received for this tuple : ", len(idA))

        for i in range(len(idA)):
            registreA[8][int(idA[i])] = True

    C = {'Value': registreA[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset A. True means linked, False the opposite

    count = 0
    for x in registreA[8]:
        count += 1 if x is True else 0

    print("Total linked : ", count)
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')

    s.close()
    return

linkage(dataset_A)