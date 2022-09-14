import json
import socket
import time
import pandas as pd
import hashlib
import secrets

start = time.perf_counter()
dataset_A = pd.read_csv("dataAEn.csv")  # Opening dataset A
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty

alpha = (secrets.randbits(50)+1)*2 #choose the security value

def receiveParams(s):
    result = s.recv(1024)
    json_data = json.loads(result.decode())
    p = json_data.get("p")
    q = json_data.get("q")
    print(time.perf_counter() - start, " : Parameters received : p=", p, " q=", q)
    return p, q


# p = 10

def receiveUplet(s):
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
            s.sendall(b'ok')
        else:
            end = True
        i+=1
    return array

def sendUplet(uplet, s):

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
            s.sendall(json_data.encode())
        else:
            json_data = json.dumps({str(i): newUplet[i*200:i*200+200]})
            s.sendall(json_data.encode())

        s.recv(16)

        i+=1

    # time.sleep(0.005)
    json_data = json.dumps({str(i): "end"})
    s.sendall(json_data.encode())


    # while not end:
    #
    #     if i*100+100 >= len(newUplet):
    #         # print("end")
    #         end = True
    #         json_data = json.dumps({str(i): newUplet[i*100:len(newUplet)]})
    #         s.sendall(json_data.encode())
    #     else:
    #         json_data = json.dumps({str(i): newUplet[i*100:i*100+100]})
    #         s.sendall(json_data.encode())
    #
    #     s.recv(16)
    #
    #     i+=1
    #
    # # time.sleep(0.005)
    # json_data = json.dumps({str(i): "end"})
    # s.sendall(json_data.encode())


def receiveIdA(s):

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
            s.sendall(b'ok')
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

    return registre

def creatingTuple2(registre, tuple,p):

    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty  or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
            uplet.append(hashlib.sha256(str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k]).encode('utf-8')).hexdigest(),16),alpha,p)).encode('utf-8')).hexdigest())
    return(uplet)

def creatingTuple3(registre, tuple,p):
    uplet = []  # The creation of the the tuple array
    for k in range(len(registre[0])):
        if registre[tuple[0]][k] == empty or registre[tuple[1]][k] == empty or registre[tuple[2]][k] == empty or registre[8][k]:
            uplet.append("")  # Completion of the tuple array, checking if it is not empty or already linked
        else:
            uplet.append(hashlib.sha256(str(pow(int(hashlib.sha256((registre[tuple[0]][k] + registre[tuple[1]][k] +registre[tuple[2]][k]).encode('utf-8')).hexdigest(),16),alpha,p)).encode('utf-8')).hexdigest()) # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
    return(uplet)

def linkOneTuple(f, registreA, p):

    list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]
    ports = [12376, 12346, 12347, 12348, 12349, 15000, 17000, 14000]

    sock = socket.socket()
    host = socket.gethostbyname("")
    port = ports[f]

    stop = False

    while not stop:
        try:
            print("Current port : ", port)
            sock.connect((host, port))    # Establish connection with client.
            stop = True
        except Exception:
            print("Trying to reconnect...")
            time.sleep(1)

    print("######### Tuple number ", f + 1, "###########")
    if len(list[f]) == 2:
        upletA = creatingTuple2(registreA,list[f],p)
    else:
        upletA = creatingTuple3(registreA,list[f],p)

    sendUplet(upletA, sock)
    print(time.perf_counter() - start, " : 1/4 : H(x)^alpha sent ")

    #get tuple from B (H(x)^beta)
    tuple = receiveUplet(sock)

    print(time.perf_counter() - start, " : 2/4 : H(y)^beta received ")

    for i in range(len(tuple)):
        if tuple[i] != "":
            tuple[i] = pow(int(tuple[i]),alpha,p)

    #send tuple to B
    sendUplet(tuple, sock)
    print(time.perf_counter() - start, " : 3/4 : H(y)^(beta*alpha) sent ")

    #get idA from B
    idA  = receiveIdA(sock)
    print(time.perf_counter() - start, " : 4/4 : IdA received")
    print("Number of linked records received for this tuple : ", len(idA))

    sock.close()


    for i in range(len(idA)):
        registreA[8][int(idA[i])] = True






