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

port = 18345
port1 = [18376, 18346, 18347, 18348, 18349, 18000, 18800, 18880]
port2 = [19376, 19346, 19347, 19348, 19349, 19350]


#starting a clock at the beginning of the program
start = time.perf_counter()
#fetching the name of the file of dataset A from command line
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-d', type=str, help='Path to dataset')
parser.add_argument('-ipB', type=str, help='IP adress of B')
args = parser.parse_args()

dataset_A = pd.read_csv(args.d)  # Opening dataset A with the name we just got from the parser
host = args.ipB

alpha = secrets.randbits(256) #generation of alpha for the private set intersection.
s = socket.socket()        # Create a socket object
# host = socket.gethostbyname("") # Get local machine name
# print(type(host))


s.connect((host, port))

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
        # print(result)
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

#A function that get a point and return its coordinates as integers
def splitXY(uplet) :
    upletX = []
    upletY = []
    for P in uplet:
        upletX.append(str(P.x))
        upletY.append(str(P.y))
    return upletX, upletY

#EST CE UTILE ENCORE ??
batch = 5000 #5000
batch_size = 100

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

    registre = [np.array(fName), np.array(lName), np.array(bDay), np.array(mail), np.array(phone), np.array(address), np.array(SSN), np.array(missing), np.array(boolean), np.array(missingCount)] #np.array pour chaque élement
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
            xialpha = alpha*Pi # alpha*Pi
            uplet.append(hashPoint(xialpha)) #H(alphaPi) = H(alphaPi.n || alphaPi.m)
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

            # a changer comme dans creatingTuple2
    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount,G,empty):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked or with less than missingCount missing values
        else:
            xi = int(hashlib.sha256((registre[tuple[0]][k] + str(registre[tuple[1]][k])).encode('utf-8')).hexdigest(),16) # transformer xi en Pi
            Pi = xi*G
            xialpha = alpha*Pi # alpha*Pi
            uplet.append(hashPoint(xialpha)) #H(alphaPi) = H(alphaPi.n || alphaPi.m)
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
    # port1 = [18376, 18346, 18347, 18348, 18349, 18000, 18800, 18880]

    sock = socket.socket()
    # host = socket.gethostbyname("")
    port = port1[f]

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
    print("######### Tuple number ", f + 1, "########### for ", num_thread)
    timer("Begin of constructing UpletA")
    if len(list[f]) == 2:
        upletA = creatingTuple2(registreA,list[f],G,empty)
    else:
        upletA = creatingTuple3(registreA,list[f],G,empty)
    # uplet A is a list of hash
    timer("End of constructing UpletA")


    timer("Begin of sending UpletA")
    sendUplet(upletA,sock)
    timer("End of sending UpletA")


    #get tuple from B (y^beta)
    timer("Begin of receiving UpletB")
    tuple = receiveUpletPoint(sock)
    timer("End of receiving UpletB")


    timer("Begin of computing alpha*beta*y")
    for i in range(len(tuple)):
        if (tuple[i].is_point_at_infinity() == False) :
            tuple[i] = alpha*tuple[i]
    timer("End of computing alpha*beta*y")

    #send tuple to B
    timer("Begin of sending alpha*beta*y")
    sendUpletPoint(tuple,sock)
    timer("End of sending alpha*beta*y")

    timer("Begin of receiving Total_IdA")
    idA  = receiveIdA(sock)
    timer("End of receiving Total_IdA")

    for i in range(len(idA)):
        registreA[8][int(idA[i])] = True

def create_one_tuple_missing(f,registreA,G,empty):

    list = np.array([[2,7],[5,7],[0,1,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3]

    sock = socket.socket()
    # host = socket.gethostbyname("")
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
    print("######### Tuple number ", f + 9, "########### for ", num_thread)
    timer("Begin of constructing UpletA")
    if len(list[f]) == 2:
        upletA = creatingTupleMissing2(registreA,list[f],missing[f],G,empty)
    else:
        upletA = creatingTupleMissing3(registreA,list[f],missing[f],G,empty)
    # uplet A is a list of hash
    timer("End of constructing UpletA")


    timer("Begin of sending UpletA")
    sendUplet(upletA,sock)
    timer("End of sending UpletA")


    #get tuple from B (y^beta)
    timer("Begin of receiving UpletB")
    tuple = receiveUpletPoint(sock)
    timer("End of receiving UpletB")


    timer("Begin of computing alpha*beta*y")
    for i in range(len(tuple)):
        if (tuple[i].is_point_at_infinity() == False) :
            tuple[i] = alpha*tuple[i]
    timer("End of computing alpha*beta*y")

    #send tuple to B
    timer("Begin of sending alpha*beta*y")
    sendUpletPoint(tuple,sock)
    timer("End of sending alpha*beta*y")

    timer("Begin of receiving Total_IdA")
    idA  = receiveIdA(sock)
    timer("End of receiving Total_IdA")

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

    count = 0
    for x in registreA[8]:
        count += 1 if x is True else 0

    print("Total linked : ", count)
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')

    s.close()

createTupleA(dataset_A)



