if __name__ == '__main__':

    import json
    import time
    import numpy as np
    import pandas as pd
    import hashlib
    import numpy
    import secrets
    import sympy
    import socket
    import runA as fc
    import multiprocessing as mp
    import mainB


    pool = mp.Pool(3)

    start = time.perf_counter()
    dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
    empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty
    size_q = 60 #choose the security value
    p,q = mainB.param(size_q)
    beta = (secrets.randbits(50)+1)*2 #choose the security value
    sock = socket.socket()
    host = socket.gethostbyname("")
    port = 12345
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)
    mainB.sendParams(p, q, c)


    def linkage(dataset_B):

        results = []

        preprocess = time.perf_counter()

        tupleListA = []
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
        registreB = mainB.extratingData(dataset_B)

        list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]

        idB = [] # The list that will save the ID of linked elements of B

        BooleanA = registreB[8]

        print(time.perf_counter() - preprocess, " : Time for data extraction")

        for f in range(len(list)):

            async_result = pool.apply_async(mainB.linkageForOneUplet, (f,registreB, BooleanA, idB, beta, q, p))
            results.append(async_result)
            # print("######### Tuple number ", f + 1, "###########")
            #
            # receptionUpletA = time.perf_counter()
            # tupleListA = receiveUplet()
            # print(time.perf_counter() - receptionUpletA, " : Time to receive H(x)^alpha")
            #
            # creationOfUpletB = time.perf_counter()
            # if len(list[f]) == 2:
            #     # upletB = creatingTuple2(registreB,list[f],p)
            #     upletB = creatingTuple2(registreB,list[f])
            # else:
            #     # upletB = creatingTuple3(registreB,list[f],p)
            #     upletB = creatingTuple3(registreB,list[f])
            #
            # print(time.perf_counter() - creationOfUpletB, " : Time to create uplet")
            # #send upletB to A
            #
            # beforeSendUplet = time.perf_counter()
            # sendUplet(upletB)
            # print(time.perf_counter() - beforeSendUplet, " : Time to send upletB ")
            #
            # invBeta = pow(beta, -1,q)
            #
            # #get the tupleB from A
            #
            # beforeReceiveUplet = time.perf_counter()
            # tupleListB = receiveUplet()
            #
            # print(time.perf_counter() - beforeReceiveUplet, " : Time to receive upletB ")
            #
            # idA = []  # The list that will save the ID of new linked elements of A
            #
            # beforeExpo = time.perf_counter()
            # for i in range(len(tupleListB)):
            #     if tupleListB[i] != "":
            #         tupleListB[i] = hashlib.sha256(str(pow(int(tupleListB[i]),invBeta,p)).encode('utf-8')).hexdigest()
            #
            # # for i in range(len(tupleListB)):
            # compareTuple(tupleListA,tupleListB,idA,idB,BooleanA,registreB[8])
            #
            # afterExpo = time.perf_counter()
            # print(afterExpo - beforeExpo, " : Time for comparison ")
            # #send idA to A
            # # sendUplet(idA)
            #
            # beforeSendIdA = time.perf_counter()
            # sendIdA(idA)
            # afterSendIdA = time.perf_counter()
            # print(afterSendIdA - beforeSendIdA, " : Time to send idA")
            # print("Number of linked records for this tuple : ", len(idA))

        results = [r.get() for r in results]
        print(results)

        C = {'Value': registreB[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
        donnees = pd.DataFrame(C, columns=['Value'])
        donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')

    linkage(dataset_B)
