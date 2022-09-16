import json
import time
import numpy as np
import pandas as pd
import hashlib
import secrets
import socket
import argparse
import threading

from Crypto.PublicKey import ECC


port = 12345
port1 = [18376, 18346, 18347, 18348, 18349, 18000, 18800, 18880]
port2 = [19376, 19346, 19347, 19348, 19349, 19350]


start = time.perf_counter()

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-n', type=int, help='The number of uplet processed')
parser.add_argument('-d', type=str, help='Path to dataset')

args = parser.parse_args()
dataset_B = pd.read_csv(args.d)
numberOfTuple = args.n

beta = secrets.randbits(256)
p = ECC._curves['p256'].p
order = int(ECC._curves['p256'].order)

sock = socket.socket()
host = socket.gethostbyname("")
sock.bind((host, port))
sock.listen(5)
c, addr = sock.accept()     # Establish connection with client.
print('Got connection from ', addr)
Total_idA = []
batch_size = 50

if numberOfTuple > 14:
    print("Maximum number of tuple is 14, numberOfTuple set to 14")
    numberOfTuple=14
if numberOfTuple < 3:
    print("Minimum number of tuple is 4, numberOfTuple set to 3")
    numberOfTuple=3



def hashPoint(P):
    return hashlib.sha256((str(P.x) + str(P.y)).encode('utf-8')).hexdigest()


def reconstructPointFromXY(upletX,upletY):
    uplet = []
    for (x,y) in zip(upletX,upletY) :
        P = ECC.EccPoint(int(x),int(y),'p256')
        uplet.append(P)
    return uplet



def receiveUplet(s):
    uplet = []
    end = False
    i = 0
    while not end:
        result = s.recv(1048576)
        json_data = json.loads(result.decode())
        id = json_data.get(str(i))

        if id != "end":
            uplet = uplet + id
            s.sendall(b'ok')
        else:
            end = True
        i+=1
    return uplet


def receiveUpletPoint(s):
    upletX = []
    upletY = []
    end = False
    while not end:
        result = s.recv(1048576)
        json_data = json.loads(result.decode())
        x = json_data.get('x')
        y = json_data.get('y')

        if x != "end":
            upletX = upletX + x
            upletY = upletY + y
            s.sendall(b'ok')
        else:
            end = True
    uplet = reconstructPointFromXY(upletX,upletY)
    return uplet


def splitXY(uplet) :
    upletX = []
    upletY = []
    for P in uplet:
        upletX.append(str(P.x))
        upletY.append(str(P.y))
    
    return upletX, upletY
    

def sendUplet(uplet, s):

    end = False
    i = 0
    newUplet = []
    for id in uplet:
        newUplet.append(str(id))

    while not end:
        if i*batch_size+batch_size >= len(newUplet):
            end = True
            json_data = json.dumps({str(i): newUplet[i*batch_size:len(newUplet)]})
            s.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newUplet[i*batch_size:i*batch_size+batch_size]})
            s.sendall(json_data.encode())

        s.recv(16)

        i+=1

    json_data = json.dumps({str(i): "end"})
    s.sendall(json_data.encode())

def sendNumberOfUplet(uplet, s):
    s.sendall(str(uplet).encode())
    s.recv(16)

def sendUpletPoint(uplet, s):

    upletX,upletY = splitXY(uplet)
    end = False
    i = 0
    while not end:
        if i*batch_size+batch_size >= len(upletX):
            end = True
            json_data = json.dumps({'x': upletX[i*batch_size:len(upletX)], 'y' : upletY[i*batch_size:len(upletY)]})
            s.sendall(json_data.encode())
        else:
            json_data = json.dumps({'x': upletX[i*batch_size:i*batch_size+batch_size], 'y' : upletY[i*batch_size:i*batch_size+batch_size]})
            s.sendall(json_data.encode())

        s.recv(16)
        i+=1
    json_data = json.dumps({'x': "end"})
    s.sendall(json_data.encode())


def sendIdA(idA,c):
    end = False
    i = 0
    newIdA = []
    for id in idA:
        newIdA.append(str(id))

    while not end:

        if i*1000+1000 >= len(idA):
            end = True
            json_data = json.dumps({str(i): newIdA[i*1000:len(newIdA)]})
            c.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newIdA[i*1000:i*1000+1000]})
            c.sendall(json_data.encode())
            
        c.recv(16)
        i+=1

    json_data = json.dumps({str(i): "end"})
    c.sendall(json_data.encode())
    c.recv(16)



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
    empty=getEmpty(registre)
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

def creatingTuple2(registre, tuple, G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k]:
            uplet.append(G.point_at_infinity())  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16)
            # uplet.append(pow(yi,beta,p))
            # tranformer yi en Qi puis calcul betaQi
            Qi = yi*G
            uplet.append(beta*Qi)
    return(uplet)

def creatingTuple3(registre, tuple,G,empty):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append(G.point_at_infinity())  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16)
            Qi = yi*G
            uplet.append(beta*Qi)
            
    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append(G.point_at_infinity())
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + str(registre[tuple[1]][k])).encode('utf-8')).hexdigest(),16)
            # tranformer yi en Qi puis calcul betaQi
            Qi = yi*G
            uplet.append(beta*Qi)
    return(uplet)

def creatingTupleMissing3(registre, tuple,missingCount,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append(G.point_at_infinity())
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +str(registre[tuple[2]][k])).encode('utf-8')).hexdigest(),16)
            Qi = yi*G
            uplet.append(beta*Qi)
    return(uplet)

def compareTuple(upletA, upletB, idA, idB, BooleanA, BooleanB):
    indexA = np.argsort(upletA) #Sorting the hashes while keeping in memory the ID of the hashes
    upletA = np.sort(upletA)
    indexB = np.argsort(upletB) #Sorting the hashes while keeping in memory the ID of the hashes
    upletB = np.sort(upletB)


    l = 0
    sizeofdataset = len(BooleanB)
    for k in range(0, sizeofdataset, 1):  # Efficient comparison of sorted list
        if not upletA[k] == "":  # verifying that the k-th tuple wasn't already linked or that one of its component was empty
            while l < sizeofdataset and upletA[k] > upletB[l]:
                l += 1
            if l < sizeofdataset and upletA[k] == upletB[l]:  # if the hashes are equals, the IDs are linked
                idA.append(indexA[k])  # we add the ID to the lists of linked IDs
                idB.append(indexB[l])
                BooleanA[indexA[k]] = True  # We set the ID as already linked
                BooleanB[indexB[l]] = True
                l += 1
    return

def timer(commit):
    print(time.perf_counter() - start, commit)


def link_one_tuple(f,registreB,BooleanA,idB,beta,G,Total_idA,empty):

    sock = socket.socket()
    host = socket.gethostbyname("")
    # print(host)
    port = port1[f]
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)

    list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]

    num_thread = threading.get_ident()
    tupleListA = receiveUplet(c)


    if len(list[f]) == 2:
        upletB = creatingTuple2(registreB,list[f],G,empty)
    else:
        upletB = creatingTuple3(registreB,list[f],G,empty)


    #send upletB to A
    sendUpletPoint(upletB,c) # peut être à changer pour ECC
    
    invBeta = pow(beta, order-2 ,order) # a voir ECC

    #get the tupleB from A
    tupleListB = receiveUpletPoint(c) # here tupleListB is a list of ECC points


    idA = []  # The list that will save the ID of new linked elements of A
    for i in range(len(tupleListB)):
        if (tupleListB[i].is_point_at_infinity() == False):
            ri = tupleListB[i] 
            fi = invBeta*ri 
            tupleListB[i] = hashPoint(fi) 
        else :
            tupleListB[i] = ""

    compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])

    # send idA to A
    sendIdA(idA,c)


def link_one_tuple_missing(f,registreB,BooleanA,idB,beta,G,Total_idA,empty):

    sock = socket.socket()
    host = socket.gethostbyname("")
    port = port2[f]
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)

    list = np.array([[2,7],[5,7],[0,1,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3]

    num_thread = threading.get_ident()
    tupleListA = receiveUplet(c)


    # upletB is a list of ECC points
    if len(list[f]) == 2:
        # upletB = creatingTuple2(registreB,list[f],p)
        upletB = creatingTupleMissing2(registreB,list[f],missing[f],G,empty)
    else:
        # upletB = creatingTuple3(registreB,list[f],p)
        upletB = creatingTupleMissing3(registreB,list[f],missing[f],G,empty)
    # upletB is a list of ECC points


    #send upletB to A
    sendUpletPoint(upletB,c)

    invBeta = pow(beta, order-2 ,order)

    #get the tupleB from A
    tupleListB = receiveUpletPoint(c) # here tupleListB is a list of ECC points


    idA = []  # The list that will save the ID of new linked elements of A
    for i in range(len(tupleListB)):
        if (tupleListB[i].is_point_at_infinity() == False):
            ri = tupleListB[i]
            fi = invBeta*ri 
            tupleListB[i] = hashPoint(fi)
        else :
            tupleListB[i] = ""

    compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])

    # send idA to A
    sendIdA(idA,c)


def getEmpty(registreB):
    empty =""
    for i in range(len(registreB[6])):
        if registreB[6][i]==registreB[6][i+1]:
            empty = registreB[6][i]
            break
    return empty


def linkage(dataset_B):

    sendNumberOfUplet(numberOfTuple, c)
    jobs = []
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreB = extratingData(dataset_B)
    empty=getEmpty(registreB)

    G = ECC.EccPoint(ECC._curves['p256'].Gx,ECC._curves['p256'].Gy,"p256")

    idB = [] # The list that will save the ID of linked elements of B

    BooleanA = []

    for i in range(len(registreB[0])):
        BooleanA.append(False)


    if numberOfTuple < 9:
        tuple1 = numberOfTuple
        tuple2 = 0
    else:
        tuple1 = 8
        tuple2 = numberOfTuple-8

    for f in range(tuple1):
        new_thread = threading.Thread(target=link_one_tuple,args=(f,registreB,BooleanA,idB,beta,G,Total_idA,empty))
        jobs.append(new_thread)

    for f in range(tuple2):
        new_thread = threading.Thread(target=link_one_tuple_missing,args=(f,registreB,BooleanA,idB,beta,G,Total_idA,empty))
        jobs.append(new_thread)

    for job in jobs:
        job.start()
    
    for job in jobs:
        job.join()

    C = {'Value': registreB[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')

linkage(dataset_B)

