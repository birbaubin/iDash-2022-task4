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



start = time.perf_counter()

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('integers', metavar='N', type=int, nargs='+', help='an integer for the accumulator')
args = parser.parse_args()
# dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty
# size_q = 256 #choose the security value
beta = secrets.randbits(256)#choose the security value
p = 115792089210356248762697446949407573530086143415290314195533631308867097853951 # prime of the p-256 curve
order = 115792089210356248762697446949407573529996955224135760342422259061068512044369
sock = socket.socket()
host = socket.gethostbyname("")
port = 12345
sock.bind((host, port))
sock.listen(5)
c, addr = sock.accept()     # Establish connection with client.
print('Got connection from ', addr)
Total_idA = []



def hashPoint(P):
    return hashlib.sha256((str(P.x) + str(P.y)).encode('utf-8')).hexdigest()



def reconstructPointFromXY(upletX,upletY):
    uplet = []
    for (x,y) in zip(upletX,upletY) :
        P = ECC.EccPoint(int(x),int(y),'p256')
        uplet.append(P)
    return uplet


batch = 50 #5000
# def receiveUplet():
#     uplet = []
#     for i in range(batch):
#         result = c.recv(1048576)
#         json_data = json.loads(result.decode())
#         uplet = uplet + json_data.get(str(i))
#         c.sendall(b'ok')

#     return uplet

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
            uplet = uplet + id
            s.sendall(b'ok')
        else:
            end = True
        i+=1
    return uplet

# def receiveUpletPoint():
#     upletX = []
#     upletY = []
#     for i in range(batch):
#         # print(i)
#         result = c.recv(1048576)
#         # print(result)
#         json_data = json.loads(result.decode())
#         upletX = upletX + json_data.get('x')
#         upletY = upletY + json_data.get('y')
#         c.sendall(b'ok')
    
#     uplet = reconstructPointFromXY(upletX,upletY)
#     return uplet

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


# def sendUplet(uplet):
#     for i in range(batch):
#         #send upletA to B
#         # print(upletA)
#         json_data = json.dumps({str(i): uplet[i*100:i*100+100]})
#         # print(json_data.encode())
#         c.sendall(json_data.encode())
#         ok = c.recv(16)
batch_size = 100

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


# def sendUpletPoint(uplet):
#     upletX,upletY = splitXY(uplet)
#     for i in range(batch):
#         #send upletA to B
#         # print(upletA)
        
#         json_data = json.dumps({'x': upletX[i*100:i*100+100], 'y' : upletY[i*100:i*100+100]})
#         # print(json_data.encode())
#         c.sendall(json_data.encode())
#         ok = c.recv(16)

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
        #time.sleep(0.005)

        if i*1000+1000 >= len(idA):
            # print("end")
            end = True
            json_data = json.dumps({str(i): newIdA[i*1000:len(newIdA)]})
            c.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newIdA[i*1000:i*1000+1000]})
            c.sendall(json_data.encode())
            
        c.recv(16)
        i+=1

    #time.sleep(0.005)
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

def creatingTuple2(registre, tuple, G):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k]:
            uplet.append(G.point_at_infinity())  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            # uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8'))
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16)
            # uplet.append(pow(yi,beta,p))

            # tranformer yi en Qi puis calcul betaQi
            Qi = yi*G
            uplet.append(beta*Qi)
    return(uplet)

def creatingTuple3(registre, tuple,G):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append(G.point_at_infinity())  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8')) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16)
            # uplet.append(pow(yi,beta,p)) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation

            Qi = yi*G
            uplet.append(beta*Qi)
            
    return(uplet)

def creatingTupleMissing2(registre, tuple,missingCount,G):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append(G.point_at_infinity())
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16)
            # tranformer yi en Qi puis calcul betaQi
            Qi = yi*G
            uplet.append(beta*Qi)
            return(uplet)

def creatingTupleMissing3(registre, tuple,missingCount,G):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k] or registre[9][k] < missingCount:
            uplet.append(G.point_at_infinity())
        else:
            yi = int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16)
            Qi = yi*G
            uplet.append(beta*Qi)
            return(uplet)

def compareTuple(upletA, upletB, idA, idB, BooleanA, BooleanB):
    indexA = np.argsort(upletA) #Sorting the hashes while keeping in memory the ID of the hashes
    upletA = np.sort(upletA)
    indexB = np.argsort(upletB) #Sorting the hashes while keeping in memory the ID of the hashes
    upletB = np.sort(upletB)


    l = 0
    sizeofdataset = 500000 #5000
    for k in range(0, sizeofdataset, 1):  # Efficient comparison of sorted list
        if not upletA[k] == "":  # verifying that the k-th tuple wasn't already linked or that one of its component was empty
            while l < sizeofdataset and upletA[k] > upletB[l]:
                l += 1
            if l < sizeofdataset and upletA[k] == upletB[l]:  # if the hashes are equals, the IDs are linked
                # idA.append(indexA[k] + 2)  # we add the ID to the lists of linked IDs
                # idB.append(indexB[l] + 2)
                idA.append(indexA[k])  # we add the ID to the lists of linked IDs
                idB.append(indexB[l])
                BooleanA[indexA[k]] = True  # We set the ID as already linked
                BooleanB[indexB[l]] = True
                l += 1
    return


def timer(commit):
    print(time.perf_counter() - start, commit)




def link_one_tuple(f,registreB,BooleanA,idB,beta,G,Total_idA):

    
    ports = [12376, 12346, 12347, 12348, 12349, 15000, 17000, 14000]
    sock = socket.socket()
    host = socket.gethostbyname("")
    port = ports[f]
    print("Current port : ", port)
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)

    list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]
    #list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6]])

    num_thread = threading.get_ident()
    print("######### Tuple number ", f + 1, "###########", num_thread)
    timer("Begin of receiving UpletA")
    tupleListA = receiveUplet(c)
    timer("End of receiving UpletA")


    timer("Begin of constructing UpletB")
    # upletB is a list of ECC points
    if len(list[f]) == 2:
        # upletB = creatingTuple2(registreB,list[f],p)
        upletB = creatingTuple2(registreB,list[f],G)
    else:
        # upletB = creatingTuple3(registreB,list[f],p)
        upletB = creatingTuple3(registreB,list[f],G)
    # upletB is a list of ECC points
    timer("End of constructing UpletB")


    #send upletB to A
    timer("Begin of sending UpletB")
    sendUpletPoint(upletB,c) # peut être à changer pour ECC
    timer("End of sending UpletB")
    # print(time.perf_counter() - start, " : 2/4 : y^beta sent ")

    invBeta = pow(beta, order-2 ,order) # a voir ECC

    #get the tupleB from A

    timer("Begin of receiving UpletB")
    tupleListB = receiveUpletPoint(c) # here tupleListB is a list of ECC points
    timer("End of receiving UpletB")


    timer("Begin of computing and hashing invbeta*alpha*beta*y")
    idA = []  # The list that will save the ID of new linked elements of A
    for i in range(len(tupleListB)):
        if (tupleListB[i].is_point_at_infinity() == False):
            # à changer en ECC
            ri = tupleListB[i] # Ri
            fi = invBeta*ri #Ri^invBeta
            tupleListB[i] = hashPoint(fi) #h(Fi_m||Fi_n)
        else :
            tupleListB[i] = ""
    timer("End of computing and hashing invbeta*alpha*beta*y")
    # here tupleListB is a list of hash of point


    # for i in range(len(tupleListB)):
    timer("Begin of comparison")
    compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])
    timer("Begin of comparison")

    # send idA to A
    # sendUplet(idA)
    timer("Begin of sending idA")
    sendIdA(idA,c)
    timer("End of sending idA")
    # Total_idA = Total_idA + idA
    print("Number of linked records for this tuple : ", len(idA))

def link_one_tuple_missing(f,registreB,BooleanA,idB,beta,G,Total_idA):


    ports = [12376, 12346, 12347, 12348, 12349, 15000, 17000, 14000] #Choisir d'autres ports
    sock = socket.socket()
    host = socket.gethostbyname("")
    port = ports[f]
    print("Current port : ", port)
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)

    list = np.array([[2,7],[5,7],[0,1,7],[0,5,7],[1,4,7],[1,5,7]])
    missing = [4,4,4,3,3,3]

    num_thread = threading.get_ident()
    print("######### Tuple number ", f + 1, "###########", num_thread)
    timer("Begin of receiving UpletA")
    tupleListA = receiveUplet(c)
    timer("End of receiving UpletA")


    timer("Begin of constructing UpletB")
    # upletB is a list of ECC points
    if len(list[f]) == 2:
        # upletB = creatingTuple2(registreB,list[f],p)
        upletB = creatingTupleMissing2(registreB,list[f],missing[f],G)
    else:
        # upletB = creatingTuple3(registreB,list[f],p)
        upletB = creatingTupleMissing3(registreB,list[f],missing[f],G)
    # upletB is a list of ECC points
    timer("End of constructing UpletB")


    #send upletB to A
    timer("Begin of sending UpletB")
    sendUpletPoint(upletB,c) # peut être à changer pour ECC
    timer("End of sending UpletB")
    # print(time.perf_counter() - start, " : 2/4 : y^beta sent ")

    invBeta = pow(beta, order-2 ,order) # a voir ECC

    #get the tupleB from A

    timer("Begin of receiving UpletB")
    tupleListB = receiveUpletPoint(c) # here tupleListB is a list of ECC points
    timer("End of receiving UpletB")


    timer("Begin of computing and hashing invbeta*alpha*beta*y")
    idA = []  # The list that will save the ID of new linked elements of A
    for i in range(len(tupleListB)):
        if (tupleListB[i].is_point_at_infinity() == False):
            # à changer en ECC
            ri = tupleListB[i] # Ri
            fi = invBeta*ri #Ri^invBeta
            tupleListB[i] = hashPoint(fi) #h(Fi_m||Fi_n)
        else :
            tupleListB[i] = ""
    timer("End of computing and hashing invbeta*alpha*beta*y")
    # here tupleListB is a list of hash of point


    # for i in range(len(tupleListB)):
    timer("Begin of comparison")
    compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])
    timer("Begin of comparison")

    # send idA to A
    # sendUplet(idA)
    timer("Begin of sending idA")
    sendIdA(idA,c)
    timer("End of sending idA")
    # Total_idA = Total_idA + idA
    print("Number of linked records for this tuple : ", len(idA))



def linkage(dataset_B):
    #get the tuple from A
    jobs = []
    tupleListA = []
    np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
    registreB = extratingData(dataset_B)

    list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]])
    #list = np.array([[0, 1,2], [0, 1,5], [1,3],[1,6]])
    G = ECC.EccPoint(ECC._curves['p256'].Gx,ECC._curves['p256'].Gy,"p256")

    idB = [] # The list that will save the ID of linked elements of B

    BooleanA = [] # registreB[8]

    for i in range(len(registreB[0])):
        BooleanA.append(False)


    if numberOfTuple < 9:
        tuple1 = numberOfTuple
        tuple2 = 0
    else:
        tuple1 = 8
        tuple2 = numberOfTuple-8

    for f in range(tuple1):

        new_thread = threading.Thread(target=link_one_tuple,args=(f,registreB,BooleanA,idB,beta,G,Total_idA))
        jobs.append(new_thread)

    for f in range(tuple2):

        new_thread = threading.Thread(target=link_one_tuple_missing,args=(f,registreB,BooleanA,idB,beta,G,Total_idA))
        jobs.append(new_thread)

    for job in jobs:
        job.start()
    
    for job in jobs:
        job.join()
    


    #timer("Begin of sending Total_idA")
    #sendIdA(Total_idA)
    #timer("Begin of sending Total_idA")

    C = {'Value': registreB[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')


linkage(dataset_B)



#%%
