import time
import pandas as pd
from bloom_filter import BloomFilter
import hashlib
import numpy

#tic_A = time.perf_counter()

dataset_A = pd.read_csv("dataA_hash.csv", sep=';')

dataset_B = pd.read_csv("dataB_hash.csv", sep=';')

def linkage(dataset_A ,dataset_B):
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    boolA = []
    SSNA =[]

    #%%
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        fNameA.append(row["First Name"])
        lNameA.append(row["Last Name"])
        bDayA.append(row["Birth Date"])
        mailA.append(row["Email"])
        phoneA.append(row["Phone"])
        addressA.append(row["Address"])
        stateA.append(row["State"])
        boolA.append(False)
        SSNA.append(row["SSN"])



    fNameB = []
    lNameB = []
    bDayB = []
    mailB = []
    phoneB = []
    addressB = []
    stateB = []
    boolB = []
    SSNB =[]

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        fNameB.append(row["First Name"])
        lNameB.append(row["Last Name"])
        bDayB.append(row["Birth Date"])
        mailB.append(row["Email"])
        phoneB.append(row["Phone"])
        addressB.append(row["Address"])
        stateB.append(row["State"])
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, boolA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, boolB]

    idA = []
    idB = []

    upletA = []
    upletB = []
    for k in range(len(fNameA)):
        if SSNA[k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[7][k]:
            upletA.append("")
        else:
            upletA.append(SSNA[k]) #hasher directement ico

    indexA = numpy.argsort(upletA)
    upletA = numpy.sort(upletA)
    for k in range(len(fNameB)):
        if SSNB[k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
            upletB.append("")
        else:
            upletB.append(SSNB[k]) #hasher directement ici
    indexB = numpy.argsort(upletB)
    upletB = numpy.sort(upletB)

    l = 0
    for k in range(len(upletA)):
        if not upletA[k] == "":
            while l < 500000 and upletA[k] > upletB[l]:
                l += 1
            if l < 500000 and upletA[k] == upletB[l]:
                idA.append(indexA[k]+2)
                idB.append(indexB[l]+2)
                registreA[7][indexA[k]] = True
                registreB[7][indexB[l]] = True
                l += 1




    listi = [2, 3, 3, 4]
    listj = [3, 4, 5, 5]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(len(upletA)):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1

    #listi = [2, 3, 3, 4]
    #listj = [3, 4, 5, 5]
    listi = [0, 0, 0, 0, 1, 1, 1]
    listj = [1, 2, 2, 3, 2, 2, 3]
    lista = [3, 4, 5, 6, 4, 5, 6]


    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[lista[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[lista[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(len(upletA)):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1

    listi = [0]
    listj = [1]
    lista = [4]
    listb = [6]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[listb[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]+registreA[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[listj[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[listb[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[lista[f]][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]+registreB[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(len(upletA)):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1

    C = {'idA': idA, 'idB': idB}
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")

linkage(dataset_A ,dataset_B)

def upletTest(dataset_A ,dataset_B):
    boolA = []
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        boolA.append(row["Booleen"])

    boolB = []
    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        boolB.append(row["Booleen"])

    upletList = []
    truePos = []
    trueNeg = []
    falseNeg = []
    falsePos = []

    for i in range(0, 7, 1): #2-uplets:
        for j in range(i+1, 7, 1):
            print("i=", i, "j=", j)

            idA = []
            idB = []
            uplet = pd.read_csv(str(i)+str(j)+"linked.csv", sep=';')
            uplet = uplet.reset_index()

            fp = 0
            tp = 0

            for index, row in uplet.iterrows():
                idA.append(row["idA"])
                idB.append(row["idB"])

            for k in range(len(idA)):

                if boolA[idA[k]-2] and boolB[idB[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                    tp += 1
                else:
                    fp += 1

            fn = 50000-tp
            tn = 500000-50000-fp

            upletList.append(str(j)+str(i))
            truePos.append(tp)
            falsePos.append(fp)
            trueNeg.append(tn)
            falseNeg.append(fn)

    for i in range(0, 7, 1): #3-uplets:
         for j in range(i+1, 7, 1):
              for a in range(j+1, 7, 1):

                print("i=", i, "j=", j, "a=", a)
                idA = []
                idB = []
                uplet = pd.read_csv(str(i)+str(j)+str(a)+"linked.csv", sep=';')
                uplet = uplet.reset_index()

                fp = 0
                tp = 0

                for index, row in uplet.iterrows():
                    idA.append(row["idA"])
                    idB.append(row["idB"])

                for k in range(len(idA)):

                    if boolA[idA[k]-2] and boolB[idB[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                        tp += 1
                    else:
                        fp += 1

                fn = 50000-tp
                tn = 500000-50000-fp

                upletList.append(str(a)+str(j)+str(i))
                truePos.append(tp)
                falsePos.append(fp)
                trueNeg.append(tn)
                falseNeg.append(fn)

    for i in range(0, 7, 1): #4-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    print("i=", i, "j=", j, "a=", a, "b=", b)
                    idA = []
                    idB = []
                    uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+"linked.csv", sep=';')
                    uplet = uplet.reset_index()

                    fp = 0
                    tp = 0

                    for index, row in uplet.iterrows():
                        idA.append(row["idA"])
                        idB.append(row["idB"])

                    for k in range(len(idA)):

                        if boolA[idA[k]-2] and boolB[idB[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                            tp += 1
                        else:
                            fp += 1

                    fn = 50000-tp
                    tn = 500000-50000-fp

                    upletList.append(str(b)+str(a)+str(j)+str(i))
                    truePos.append(tp)
                    falsePos.append(fp)
                    trueNeg.append(tn)
                    falseNeg.append(fn)

    for i in range(0, 7, 1): #5-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c)
                        idA = []
                        idB = []
                        uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+str(c)+"linked.csv", sep=';')
                        uplet = uplet.reset_index()

                        fp = 0
                        tp = 0

                        for index, row in uplet.iterrows():
                            idA.append(row["idA"])
                            idB.append(row["idB"])

                        for k in range(len(idA)):

                            if boolA[idA[k]-2] and boolB[idB[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                tp += 1
                            else:
                                fp += 1

                        fn = 50000-tp
                        tn = 500000-50000-fp

                        upletList.append(str(c)+str(b)+str(a)+str(j)+str(i))
                        truePos.append(tp)
                        falsePos.append(fp)
                        trueNeg.append(tn)
                        falseNeg.append(fn)

    C = {'uplet': upletList, 'vrai positif': truePos, 'vrai négatif': trueNeg, 'faux négatif': falseNeg, 'faux positif': falsePos}
    donnees = pd.DataFrame(C, columns=['uplet', 'vrai positif', 'vrai négatif', 'faux négatif', 'faux positif'])
    donnees.to_csv('UpletTest.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")

def linkedUplet(dataset_A ,dataset_B):
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    SSNA = []

    #%%
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        fNameA.append(row["First Name"])
        lNameA.append(row["Last Name"])
        bDayA.append(row["Birth Date"])
        mailA.append(row["Email"])
        phoneA.append(row["Phone"])
        addressA.append(row["Address"])
        stateA.append(row["State"])
        SSNA.append(row["SSN"])

    fNameB = []
    lNameB = []
    bDayB = []
    mailB = []
    phoneB = []
    addressB = []
    stateB = []
    SSNB = []

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        fNameB.append(row["First Name"])
        lNameB.append(row["Last Name"])
        bDayB.append(row["Birth Date"])
        mailB.append(row["Email"])
        phoneB.append(row["Phone"])
        addressB.append(row["Address"])
        stateB.append(row["State"])
        SSNB.append(row["SSN"])
    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, SSNB]
    for i in range(0, 7, 1): #2-uplets:
        for j in range(i+1, 7, 1):
            print("i=", i, "j=", j)
            upletA = []
            upletB = []
            idA = []
            idB = []

            for k in range(len(fNameA)):
                if registreA[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                    upletA.append("")
                else:
                    upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]).encode('utf-8')).hexdigest()) #hasher directement ico

            indexA = numpy.argsort(upletA)
            upletA = numpy.sort(upletA)
            for k in range(len(fNameB)):
                if registreB[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                    upletB.append("")
                else:
                 upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]).encode('utf-8')).hexdigest()) #hasher directement ici
            tic_A = time.perf_counter()
            indexB = numpy.argsort(upletB)
            upletB = numpy.sort(upletB)

            l = 0
            tic_A = time.perf_counter()
            for k in range(len(upletA)):
                if not upletA[k] == "":
                    while l < 500000 and upletA[k] > upletB[l]:
                        l += 1
                    if l < 500000 and upletA[k] == upletB[l]:
                        idA.append(indexA[k]+2)
                        idB.append(indexB[l]+2)
                        l += 1
            toc_A = time.perf_counter()
            print("temps tri : ", toc_A-tic_A)
            C = {'idA': idA, 'idB': idB}
            donnees = pd.DataFrame(C, columns=['idA', 'idB'])
            donnees.to_csv(str(i)+str(j)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
            print("export fait")


    for i in range(0, 7, 1): #3-uplets
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                print("i=", i, "j=", j, "a=", a)
                upletA = []
                upletB = []
                idA = []
                idB = []

                for k in range(len(fNameA)):
                    if registreA[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                        upletA.append("")
                    else:
                        upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                indexA = numpy.argsort(upletA)
                upletA = numpy.sort(upletA)
                for k in range(len(fNameB)):
                    if registreB[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                        upletB.append("")
                    else:
                        upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]+registreB[a][k]).encode('utf-8')).hexdigest()) #hasher directement ici
                tic_A = time.perf_counter()
                indexB = numpy.argsort(upletB)
                upletB = numpy.sort(upletB)
                l = 0
                tic_A = time.perf_counter()
                for k in range(len(upletA)):
                    if not upletA[k] == "":
                        while l < 500000 and upletA[k] > upletB[l]:
                            l += 1
                        if l < 500000 and upletA[k] == upletB[l]:
                            idA.append(indexA[k]+2)
                            idB.append(indexB[l]+2)
                            l += 1
                toc_A = time.perf_counter()
                print("temps tri : ", toc_A-tic_A)
                C = {'idA': idA, 'idB': idB}
                donnees = pd.DataFrame(C, columns=['idA', 'idB'])
                donnees.to_csv(str(i)+str(j)+str(a)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
                print("export fait")


    for i in range(0, 7, 1): #4-uplets
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    print("i=", i, "j=", j, "a=", a, "b=", b)
                    upletA = []
                    upletB = []
                    idA = []
                    idB = []

                    for k in range(len(fNameA)):
                        if registreA[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[b][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                            upletA.append("")
                        else:
                            upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                    indexA = numpy.argsort(upletA)
                    upletA = numpy.sort(upletA)
                    for k in range(len(fNameB)):
                        if registreB[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[b][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                            upletB.append("")
                        else:
                            upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]).encode('utf-8')).hexdigest()) #hasher directement ici
                    tic_A = time.perf_counter()
                    indexB = numpy.argsort(upletB)
                    upletB = numpy.sort(upletB)
                    l = 0
                    tic_A = time.perf_counter()
                    for k in range(len(upletA)):
                        if not upletA[k] == "":
                            while l < 500000 and upletA[k] > upletB[l]:
                                l += 1
                            if l < 500000 and upletA[k] == upletB[l]:
                                idA.append(indexA[k]+2)
                                idB.append(indexB[l]+2)
                                l += 1
                    toc_A = time.perf_counter()
                    print("temps tri : ", toc_A-tic_A)
                    C = {'idA': idA, 'idB': idB}
                    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
                    donnees.to_csv(str(i)+str(j)+str(a)+str(b)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
                    print("export fait")

    for i in range(0, 7, 1): #5-uplets
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c)
                        upletA = []
                        upletB = []
                        idA = []
                        idB = []

                        for k in range(len(fNameA)):
                            if registreA[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[b][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreA[c][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                                upletA.append("")
                            else:
                                upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]+registreA[c][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                        indexA = numpy.argsort(upletA)
                        upletA = numpy.sort(upletA)
                        for k in range(len(fNameB)):
                            if registreB[i][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[j][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[a][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[b][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855' or registreB[c][k] == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                                upletB.append("")
                            else:
                                upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]+registreB[c][k]).encode('utf-8')).hexdigest()) #hasher directement ici
                        tic_A = time.perf_counter()
                        indexB = numpy.argsort(upletB)
                        upletB = numpy.sort(upletB)
                        l = 0
                        tic_A = time.perf_counter()
                        for k in range(len(upletA)):
                            if not upletA[k] == "":
                                while l < 500000 and upletA[k] > upletB[l]:
                                    l += 1
                                if l < 500000 and upletA[k] == upletB[l]:
                                    idA.append(indexA[k]+2)
                                    idB.append(indexB[l]+2)
                                    l += 1
                        toc_A = time.perf_counter()
                        print("temps tri : ", toc_A-tic_A)
                        C = {'idA': idA, 'idB': idB}
                        donnees = pd.DataFrame(C, columns=['idA', 'idB'])
                        donnees.to_csv(str(i)+str(j)+str(a)+str(b)+str(c)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
                        print("export fait")