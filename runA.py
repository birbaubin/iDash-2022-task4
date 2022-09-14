if __name__ == '__main__':

    import time
    import numpy as np
    import multiprocessing as mp
    import mainA
    import pandas as pd
    import secrets
    import socket



    pool = mp.Pool(3)
    start = time.perf_counter()
    dataset_A = pd.read_csv("dataAEn.csv")  # Opening dataset A
    empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty

    alpha = (secrets.randbits(50)+1)*2 #choose the security value
    s = socket.socket()         # Create a socket object
    host = socket.gethostbyname("") # Get local machine name
    port = 12345
    s.connect((host, port))

    p, q = mainA.receiveParams(s)

    def linkage(dataset_A):

        results = []

        preprocess = time.perf_counter()

        tupleListA = []
        np.warnings.filterwarnings('ignore', category=np.VisibleDeprecationWarning)
        registreA = mainA.extratingData(dataset_A)

        list = [[0, 1,2], [0, 1,5], [1,3],[1,6],[0,1,4],[2,5],[2,4],[4,5]]

        idB = [] # The list that will save the ID of linked elements of B

        BooleanA = registreA[8]

        print(time.perf_counter() - preprocess, " : Time for data extraction")

        for f in range(len(list)):
            async_result = pool.apply_async(mainA.linkOneTuple, (f,registreA, p))
            results.append(async_result)
            print("######### Tuple number ", f + 1, "###########")

        results = [r.get() for r in results]
        print(results)

        C = {'Value': registreA[8]}  # We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
        donnees = pd.DataFrame(C, columns=['Value'])
        donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')

    linkage(dataset_A)

#%%

#%%
