import time
import numpy as np
import pandas as pd
import secrets
import socket
import mainB
import threading

if __name__ == '__main__':

    start = time.perf_counter()
    sock = socket.socket()
    host = socket.gethostbyname("")
    port = 12345
    sock.bind((host, port))
    sock.listen(5)
    c, addr = sock.accept()     # Establish connection with client.
    print('Got connection from ', addr)
    beta = (secrets.randbits(50)+1)*2 #choose the security value
    dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
    empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty
    size_q = 60 #choose the security value
    p,q = mainB.param(size_q)
    mainB.sendParams(p, q, c)

    def linkage(dataset_B):

        jobs = []
        preprocess = time.perf_counter()
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
        registreB = mainB.extratingData(dataset_B)
        list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]
        idB = [] # The list that will save the ID of linked elements of B
        BooleanA = registreB[8]
        lock = threading.Lock()
        print(time.perf_counter() - preprocess, " : Time for data extraction")

        for f in range(len(list)):
            new_thread = threading.Thread(target=mainB.linkageForOneUplet, args=(f,registreB, BooleanA, idB, beta, q, p, lock))
            jobs.append(new_thread)

        for job in jobs:
            job.start()

        for job in jobs:
            job.join()

        C = {'Value': registreB[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
        donnees = pd.DataFrame(C, columns=['Value'])
        donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')

    linkage(dataset_B)
