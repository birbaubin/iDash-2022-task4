import multiprocessing as mp


import json
import time
import hashlib
import numpy
import secrets
import sympy
import socket

empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty


def param(size_q):
    q = sympy.nextprime(pow(2, size_q)+secrets.randbits(size_q))
    p = 2*q+1
    while sympy.isprime(p)==False:
        q = sympy.nextprime(q)
        p = 2*q+1
    return p,q


def sendParams(p,q, c):
    json_data = json.dumps({"p": p, "q":q})
    c.sendall(json_data.encode())


def receiveUplet(c):
    array = []
    end = False
    i = 0
    while not end:
        result = c.recv(1048576)
        # print(result)
        json_data = json.loads(result.decode())
        id = json_data.get(str(i))

        if id != "end":
            array = array + id
            c.sendall(b'ok')
        else:
            end = True
        i+=1

    return array



def sendUplet(uplet, c):
    end = False
    i = 0
    newUplet = []
    for id in uplet:
        newUplet.append(str(id))

    while not end:

        if i*200+200 >= len(newUplet):
            # print("end")
            end = True
            json_data = json.dumps({str(i): newUplet[i*200:len(newUplet)]})
            c.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newUplet[i*200:i*200+200]})
            c.sendall(json_data.encode())

        c.recv(16)

        i+=1

    # time.sleep(0.005)
    json_data = json.dumps({str(i): "end"})
    c.sendall(json_data.encode())


def sendIdA(idA, c):

    end = False
    i = 0
    newIdA = []
    for id in idA:
        newIdA.append(str(id))

    while not end:

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
        # missing.append(0)
        # missingCount.append(0)

    registre = [fName, lName, bDay, mail, phone, address, SSN, missing, boolean, missingCount]

    return registre

def creatingTuple2(registre, tuple, beta, p):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            # uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8'))
            uplet.append(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),beta,p))
    return(uplet)

def creatingTuple3(registre, tuple, beta, p):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # uplet.append((str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),beta,p))).encode('utf-8')) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            uplet.append(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),beta,p)) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
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
                idA.append(indexA[k])  # we add the ID to the lists of linked IDs
                idB.append(indexB[l])
                BooleanA[indexA[k]] = True  # We set the ID as already linked
                BooleanB[indexB[l]] = True
                l += 1
    return


def compareTuple2(upletA, upletB, idA, idB, BooleanA, BooleanB):
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

def linkageForOneUplet(f, registreB, BooleanA, idB, beta, q, p, lock):

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

    print("######### Tuple number ", f + 1, "###########")

    receptionUpletA = time.perf_counter()
    tupleListA = receiveUplet(c)
    print(time.perf_counter() - receptionUpletA, " : Time to receive H(x)^alpha")
    creationOfUpletB = time.perf_counter()
    if len(list[f]) == 2:
        # upletB = creatingTuple2(registreB,list[f],p)
        upletB = creatingTuple2(registreB,list[f], beta, p)
    else:
        # upletB = creatingTuple3(registreB,list[f],p)
        upletB = creatingTuple3(registreB,list[f], beta, p)
    print(time.perf_counter() - creationOfUpletB, " : Time to create uplet")
    #send upletB to A

    beforeSendUplet = time.perf_counter()
    sendUplet(upletB,c)
    print(time.perf_counter() - beforeSendUplet, " : Time to send upletB ")

    invBeta = pow(beta, -1,q)

    #get the tupleB from A

    beforeReceiveUplet = time.perf_counter()
    tupleListB = receiveUplet(c)

    print(time.perf_counter() - beforeReceiveUplet, " : Time to receive upletB ")

    idA = []  # The list that will save the ID of new linked elements of A

    beforeExpo = time.perf_counter()
    for i in range(len(tupleListB)):
        if tupleListB[i] != "":
            tupleListB[i] = hashlib.sha256(str(pow(int(tupleListB[i]),invBeta,p)).encode('utf-8')).hexdigest()

    # for i in range(len(tupleListB)):
    compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])

    afterExpo = time.perf_counter()
    print(afterExpo - beforeExpo, " : Time for comparison ")
    #send idA to A
    # sendUplet(idA)

    beforeSendIdA = time.perf_counter()
    sendIdA(idA, c)
    afterSendIdA = time.perf_counter()
    print(afterSendIdA - beforeSendIdA, " : Time to send idA")
    print("Number of linked records for this tuple : ", len(idA))

    return idA

