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
dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'
sock = socket.socket()
host = socket.gethostbyname("")
port = 12345
sock.bind((host, port))
sock.listen(5)
c, addr = sock.accept()     # Establish connection with client.
print('Got connection from ', addr)

def receiveUplet():
    uplet = []
    for i in range(5000):
        result = c.recv(1048576)
        json_data = json.loads(result.decode())
        uplet = uplet + json_data.get(str(i))
        c.sendall(b'ok')

    return uplet

def sendUplet(uplet):
    for i in range(5000):
        #send upletA to B
        # print(upletA)
        json_data = json.dumps({str(i): uplet[i*100:i*100+100]})
        # print(json_data.encode())
        c.sendall(json_data.encode())
        ok = c.recv(16)


def sendIdA(idA):

    end = False
    i = 0
    newIdA = []
    for id in idA:
        newIdA.append(str(id))

    while not end:
        time.sleep(0.005)

        if i*1000+1000 >= len(idA):
            # print("end")
            end = True
            json_data = json.dumps({str(i): newIdA[i*1000:len(newIdA)]})
            c.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newIdA[i*1000:i*1000+1000]})
            c.sendall(json_data.encode())
        # except Exception:
        #     print("exception")
        #     json_data = json.dumps({str(i): newIdA[i*1000:len(newIdA)]})
        #     c.sendall(json_data.encode())
        #     c.sendall(json.dumps({str(i+1): "end"}).encode())
        #     end = True

        i+=1

    time.sleep(0.005)
    json_data = json.dumps({str(i): "end"})
    c.sendall(json_data.encode())
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

def compareTuple(upletA, upletB, idA, idB, BooleanA, registreB):
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
                idA.append(indexA[k])  # we add the ID to the lists of linked IDs
                idB.append(indexB[l])
                BooleanA[indexA[k]] = True  # We set the ID as already linked
                registreB[8][indexB[l]] = True
                l += 1
    return

def linkage(dataset_B):

    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreB = extratingData(dataset_B)
    idB = [] # The list that will save the ID of linked elements of B
    BooleanA = []
    for i in range(0,500000):
        BooleanA.append(False)


    list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]])

    for f in range(len(list)):
        idA = []
        print("######## ",time.perf_counter() - start," : Tuple number ", f + 1, "###########")

        tupleListA = receiveUplet()

        if len(list[f]) == 2:
            upletB = creatingTuple2(registreB,list[f])
        else:
            upletB = creatingTuple3(registreB,list[f])

        compareTuple(tupleListA,upletB,idA,idB,BooleanA,registreB)

        sendIdA(idA)
        print("Number of linked records for this tuple : ", len(idA))

    list = np.array([[2,7],[5,7],[0,1,7],[0,4,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3,3]

    for f in range(len(list)):
        idA = []
        print("######## ",time.perf_counter() - start," : Tuple number ", f + 9, "###########")
        tupleListA = receiveUplet()
        if len(list[f]) ==2:
            upletB = creatingTupleMissing2(registreB,list[f],missing[f])
        else:
            upletB = creatingTupleMissing3(registreB,list[f],missing[f])

        compareTuple(tupleListA, upletB, idA, idB, BooleanA, registreB)

        sendIdA(idA)
        print("Number of linked records for this tuple : ", len(idA))

    C = {'Value': registreB[8]}  # We output the file OutputB.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')
    c.close()
    return


linkage(dataset_B)
#%%
