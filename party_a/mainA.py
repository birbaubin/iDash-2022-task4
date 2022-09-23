import json
import socket
import time
import numpy as np
import pandas as pd
import hashlib
import secrets
import argparse

import threading

from Crypto.PublicKey import ECC


port = 10000
port1 = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008]
port2 = [19376, 19346, 19347, 19348, 19349, 19350]


#starting a clock at the beginning of the program
start = time.perf_counter()
#fetching the name of the file of dataset A from command line
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-d', type=str, help='Path to dataset')
args = parser.parse_args()

dataset_A = pd.read_csv(args.d)  # Opening dataset A with the name we just got from the parser

alpha = secrets.randbits(256) #generation of alpha for the private set intersection.
s = socket.socket()        # Create a socket object
lock = threading.Lock()

stop = False

while not stop:
    try:
        # host = socket.gethostbyname("")
        host = "192.168.1.3"
        s.connect((host, port))    # Establish connection with client.
        print("Connected to", host, ":", port)
        stop = True
    except Exception:
        print("Trying to reconnect...")
        time.sleep(1)


#s.connect((host, port))
batch_size = 5

#A function that transform a point into a hash
def hashPoint(P):
    return hashlib.sha256((str(P.x) + str(P.y)).encode('utf-8')).hexdigest()

#A function to reconstruct a point from two integers
def reconstructPointFromXY(upletX,upletY):
    uplet = []
    for (x,y) in zip(upletX,upletY) :
        P = ECC.EccPoint(int(x),int(y),'p256')
        uplet.append(P)
    return uplet

#A function to receive the tuples from B
def receiveUplet(s):

    uplet = []
    end = False
    i = 0
    while not end:
        result = s.recv(1048576)
        json_data = json.loads(result.decode())
        id = json_data.get(str(i))

        if id != "end":
            array = array + id
            s.sendall(b'ok')
        else:
            end = True
        i+=1

    return uplet

#A function to receive the number of tuples that will be considered from B
def receiveNumberOfUplet(s):

    uplet = s.recv(128).decode()
    s.sendall(b'ok')

    return uplet

#A function to receive a list of points (A tuple with the format of points from the ECC) from B
def receiveUpletPoint(s):

    lock.acquire()
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
    lock.release()
    return uplet


# def receiveUpletPoint(s):
#     upletX = []
#     upletY = []
#     end = False
#     while not end:
#         x = s.recv(1048576).decode()
#         if x != "end":
#             s.sendall(b'ok')
#             y = s.recv(1048576).decode()
#             #s.sendall(b'ok')
#
#             upletX.append(x)
#             upletY.append(y)
#             s.sendall(b'ok')
#         else:
#             end = True
#     uplet = reconstructPointFromXY(upletX,upletY)
#     return uplet


#A function that get a point and return its coordinates as integers
def splitXY(uplet) :
    upletX = []
    upletY = []
    for P in uplet:
        upletX.append(str(P.x))
        upletY.append(str(P.y))
    return upletX, upletY


#A function to send a tuple to B
def sendUplet(uplet, s):

    end = False
    i = 0
    newUplet = []
    for id in uplet:
        newUplet.append(str(id))

    while not end:
        if i*batch_size+batch_size >= len(newUplet):
            # print("end")
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


#A function to send a list of points to B
def sendUpletPoint(uplet, s):

    lock.acquire()
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

    lock.release()

# def sendUpletPoint(uplet, s):
#
#     upletX,upletY = splitXY(uplet)
#     end = False
#     for i in range(len(upletX)):
#         # json_data = json.dumps({'x': upletX[i*batch_size:i*batch_size+batch_size], 'y' : upletY[i*batch_size:i*batch_size+batch_size]})
#         s.sendall(upletX[i].encode())
#         s.recv(16)
#         s.sendall(upletY[i].encode())
#         s.recv(16)
#
#     # json_data = json.dumps({'x': "end"})
#     s.sendall(b'end')

#A function to receive a list of the still permutated linked IdA from A
def receiveIdA(s):

    array = []
    end = False
    i = 0
    while not end:
        result = s.recv(1048576)
        json_data = json.loads(result.decode())
        id = json_data.get(str(i))
        if id != "end":
            array = array + id
        else:
            end = True

        i+=1

        s.sendall(b"ok")

    return array

#A function to extract the data from the datasets
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

    registre = [np.array(fName), np.array(lName), np.array(bDay), np.array(mail), np.array(phone), np.array(address), np.array(SSN), np.array(missing), np.array(boolean), np.array(missingCount)] 
    empty = getEmpty(registre)
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

def creatingTuple2(registre, tuple,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty  or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:

            xi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16) # transformer xi en Pi
            Pi = xi*G
            xialpha = alpha*Pi
            uplet.append(hashPoint(xialpha))
    return(uplet)

def creatingTuple3(registre, tuple,G,empty):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            xi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16)
            Pi = xi*G
            xialpha = alpha*Pi
            uplet.append(hashPoint(xialpha)) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation

    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked or with less than missingCount missing values
        else:
            xi = int(hashlib.sha256((registre[tuple[0]][k] + str(registre[tuple[1]][k])).encode('utf-8')).hexdigest(),16) # transformer xi en Pi
            Pi = xi*G
            xialpha = alpha*Pi
            uplet.append(hashPoint(xialpha))
    return(uplet)

def creatingTupleMissing3(registre, tuple,missingCount,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            xi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] + str(registre[tuple[2]][k])).encode('utf-8')).hexdigest(),16)
            Pi = xi*G
            xialpha = alpha*Pi
            uplet.append(hashPoint(xialpha)) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def shuffling(registreA):

    data_length = registreA[0].shape[0]
    # Here we create an array of shuffled indices
    shuf_order = np.arange(data_length)
    np.random.shuffle(shuf_order)

    for i in range(len(registreA)):
        registreA[i] = registreA[i][shuf_order] # Shuffle the original data

    # Create an inverse of the shuffled index array (to reverse the shuffling operation, or to "unshuffle")
    unshuf_order = np.zeros_like(shuf_order)
    unshuf_order[shuf_order] = np.arange(data_length)
    return unshuf_order

def timer(commit):
    print(time.perf_counter() - start, commit)


def create_one_tuple(f,registreA,G,empty):

    list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]

    sock = socket.socket()
    port = port1[f]

    stop = False

    while not stop:
        try:
            sock.connect((host, port))    # Establish connection with client.
            print("Connected to", host, ":", port)
            stop = True
        except Exception:
            print("Trying to reconnect...")
            time.sleep(1)

    num_thread = threading.get_ident()
    if len(list[f]) == 2:
        upletA = creatingTuple2(registreA,list[f],G,empty)
    else:
        upletA = creatingTuple3(registreA,list[f],G,empty)
    # uplet A is a list of hash


    sendUplet(upletA,sock)
    print("uplet A sent")

    #get tuple from B (y^beta)
    tuple = receiveUpletPoint(sock)
    print("uplet B received")

    for i in range(len(tuple)):
        if (tuple[i].is_point_at_infinity() == False) :
            tuple[i] = alpha*tuple[i]

    #send tuple to B
    sendUpletPoint(tuple,sock)
    print("uplet B sent")

    idA  = receiveIdA(sock)
    print("id A received")

    for i in range(len(idA)):
        registreA[8][int(idA[i])] = True

def create_one_tuple_missing(f,registreA,G,empty):

    list = np.array([[2,7],[5,7],[0,1,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3]

    sock = socket.socket()
    port = port2[f]

    stop = False

    while not stop:
        try:
            print("Current port : ", port)
            sock.connect((host, port))    # Establish connection with client.
            stop = True
        except Exception:
            print("Trying to reconnect...")
            time.sleep(1)

    num_thread = threading.get_ident()
    if len(list[f]) == 2:
        upletA = creatingTupleMissing2(registreA,list[f],missing[f],G,empty)
    else:
        upletA = creatingTupleMissing3(registreA,list[f],missing[f],G,empty)
    # uplet A is a list of hash


    sendUplet(upletA,sock)


    #get tuple from B (y^beta)
    tuple = receiveUpletPoint(sock)


    for i in range(len(tuple)):
        if (tuple[i].is_point_at_infinity() == False) :
            tuple[i] = alpha*tuple[i]

    #send tuple to B
    sendUpletPoint(tuple,sock)

    idA  = receiveIdA(sock)

    for i in range(len(idA)):
        registreA[8][int(idA[i])] = True

def getEmpty(registreA):
    empty =""
    for i in range(len(registreA[6])):
        if registreA[6][i]==registreA[6][i+1]:
            empty = registreA[6][i]
            break
    return empty

def createTupleA(dataset_A):

    numberOfTuple = int(receiveNumberOfUplet(s))
    jobs = []
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreA = extratingData(dataset_A)
    empty=getEmpty(registreA)
    unshuf_order = shuffling(registreA)

    G = ECC.EccPoint(ECC._curves['p256'].Gx,ECC._curves['p256'].Gy,"p256")

    if numberOfTuple < 9:
        tuple1 = numberOfTuple
        tuple2 = 0
    else:
        tuple1 = 8
        tuple2 = numberOfTuple-8

    for f in range(tuple1):

        new_thread = threading.Thread(target=create_one_tuple,args=(f,registreA,G,empty))
        jobs.append(new_thread)

    for f in range(tuple2):

        new_thread = threading.Thread(target=create_one_tuple_missing,args=(f,registreA,G,empty))
        jobs.append(new_thread)

    for job in jobs:
        job.start()
    
    for job in jobs:
        job.join()

    registreA[8] = registreA[8][unshuf_order] # Unshuffle the shuffled data
    C = {'Value': registreA[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset A. True means linked, False the opposite


    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')

    s.close()

createTupleA(dataset_A)


