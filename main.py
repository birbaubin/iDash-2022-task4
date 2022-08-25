import math
import time
import pandas as pd
from bloom_filter import BloomFilter
import hashlib
import numpy

#tic_A = time.perf_counter()

dataset_A = pd.read_csv("dataAEn.csv") #Opening dataset A
dataset_B = pd.read_csv("dataBEn.csv") #Opening dataset B
empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0' #The hash value of empty
#linked = pd.read_csv("linkage.csv", sep=';')

def upletOrder(dataset_A ,dataset_B):
    tic_A = time.perf_counter()
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    boolA = []
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
        boolA.append(False)
        SSNA.append(row["SSN"])



    fNameB = []
    lNameB = []
    bDayB = []
    mailB = []
    phoneB = []
    addressB = []
    boolB = []
    SSNB = []

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        fNameB.append(row["First Name"])
        lNameB.append(row["Last Name"])
        bDayB.append(row["Birth Date"])
        mailB.append(row["Email"])
        phoneB.append(row["Phone"])
        addressB.append(row["Address"])
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, SSNA, boolA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, SSNB, boolB]

    idA = []
    idB = []

    listi = [0,0]
    listj = [1,1]
    lista = [2,5]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
    listi = [1,1]
    listj = [3,6]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
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

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1

    listi = [2,2,4]
    listj = [5,4,5]
    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
    listi = [0, 2,3, 4, 5,0,2,4,5]
    listj = [6, 6,6, 6, 6,3,3,3,3]

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)
        count = 0
        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    count += 1
                    l += 1
        print(count, " uplet ",listi[f],listj[f])
#upletOrder(dataset_A ,dataset_B)

def linkageTempsUplet(dataset_A ,dataset_B):
    tic_A = time.perf_counter()
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    boolA = []
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
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, boolA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, boolB, SSNB]

    idA = []
    idB = []

    upletA = []
    upletB = []





    listi = [2, 3, 3, 4, 0, 1, 2, 3, 4, 5]
    listj = [3, 4, 5, 5, 8, 8, 8, 8, 8, 8]

    for f in range(len(listi)):

        tic = time.perf_counter()
        for e in range(0,100,1):
            upletA = []
            upletB = []
            for k in range(len(fNameA)):
                if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                    upletA.append("")
                else:
                    upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

            indexA = numpy.argsort(upletA)
            upletA = numpy.sort(upletA)
            for k in range(len(fNameB)):
                if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                    upletB.append("")
                else:
                    upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
            indexB = numpy.argsort(upletB)
            upletB = numpy.sort(upletB)

            l = 0
            for k in range(0,500000,1):
                if not upletA[k] == "":
                    while l < 500000 and upletA[k] > upletB[l]:
                        l += 1
                    if l < 500000 and upletA[k] == upletB[l]:
                        idA.append(indexA[k]+2)
                        idB.append(indexB[l]+2)
                        registreA[7][indexA[k]] = True
                        registreB[7][indexB[l]] = True
                        l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], "temps total = ", (toc-tic)/100)

    #listi = [2, 3, 3, 4]
    #listj = [3, 4, 5, 5]
    listi = [0, 0, 0, 1, 1, 0, 0, 0]
    listj = [1, 2, 2, 2, 2, 1, 1, 1]
    lista = [3, 4, 5, 4, 5, 5, 4, 2]


    for f in range(len(listi)):
        tic = time.perf_counter()
        for e in range(0,100,1):
            upletA = []
            upletB = []
            for k in range(len(fNameA)):
                if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                    upletA.append("")
                else:
                    upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

            indexA = numpy.argsort(upletA)
            upletA = numpy.sort(upletA)
            for k in range(len(fNameB)):
                if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                    upletB.append("")
                else:
                    upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
            indexB = numpy.argsort(upletB)
            upletB = numpy.sort(upletB)

            l = 0
            for k in range(0,500000,1):
                if not upletA[k] == "":
                    while l < 500000 and upletA[k] > upletB[l]:
                        l += 1
                    if l < 500000 and upletA[k] == upletB[l]:
                        idA.append(indexA[k]+2)
                        idB.append(indexB[l]+2)
                        registreA[7][indexA[k]] = True
                        registreB[7][indexB[l]] = True
                        l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", (toc-tic)/100)

    C = {'idA': idA, 'idB': idB}
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
    print(-tic_A + time.perf_counter())
#linkageTempsUplet(dataset_A ,dataset_B)
"""
def nonLinked(dataset_A,dataset_B):
    errorA = []
    boolA = []
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    SSNA = []

    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        boolA.append(row["Overlapping"])
        errorA.append(row["Error Feature"])
        fNameA.append(row["First Name"])
        lNameA.append(row["Last Name"])
        bDayA.append(row["Birth Date"])
        mailA.append(row["Email"])
        phoneA.append(row["Phone"])
        addressA.append(row["Address"])
        SSNA.append(row["SSN"])

    idA = []

    linked = linked.reset_index()
    for index, row in linked.iterrows():
        idA.append(row["idA"])

    trueindexA = []
    trueerrorA = []
    truefNameA = []
    truelNameA = []
    truebDayA = []
    truemailA = []
    truephoneA = []
    trueaddressA = []
    trueSSNA = []

    for i in range(0, len(boolA)):
        if boolA[i]:
            trueindexA.append(i+2)
            trueerrorA.append(errorA[i])
            if lNameA[i] == empty:
                truelNameA.append('empty')
            else:
                truelNameA.append(lNameA[i])
            if fNameA[i] == empty:
                truefNameA.append('empty')
            else:
                truefNameA.append(fNameA[i])
            if bDayA[i] == empty:
                truebDayA.append('empty')
            else:
                truebDayA.append(bDayA[i])
            if mailA[i] == empty:
                truemailA.append('empty')
            else:
                truemailA.append(mailA[i])
            if phoneA[i] == empty:
                truephoneA.append('empty')
            else:
                truephoneA.append(phoneA[i])
            if addressA[i] == empty:
                trueaddressA.append('empty')
            else:
                trueaddressA.append(addressA[i])
            if SSNA[i] == empty:
                trueSSNA.append('empty')
            else:
                trueSSNA.append(SSNA[i])

    idA = numpy.sort(idA)
    print(len(idA))
    print(len(trueaddressA))

    ind=[]
    err=[]
    fName = []
    lname =[]
    bday=[]
    mail =[]
    phone =[]
    address = []
    SSN=[]

    for i in range(0, len(trueerrorA)):
        l = 0
        for j in range(0,len(idA)):
            if idA[j]==trueindexA[i]:
                l=1
                break
        if l==0:
            err.append(trueerrorA[i])
            ind.append(trueindexA[i])
            fName.append(truefNameA[i])
            lname.append(truelNameA[i])
            bday.append(truebDayA[i])
            mail.append(truemailA[i])
            phone.append(truephoneA[i])
            address.append(trueaddressA[i])
            SSN.append(trueSSNA[i])

    print(len(err))


    C = {'index': ind,'bday':bday,'mail':mail,'phone':phone,'address':address,'SSN':SSN, 'error': err,'fname':fName,'lname':lname}
    donnees = pd.DataFrame(C, columns=['index','bday','mail','phone','address','SSN', 'error','fname','lname'])
    donnees.to_csv('nonlinkedA.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export1 fait")

    errorA = []
    boolA = []
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    SSNA = []

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        boolA.append(row["Overlapping"])
        errorA.append(row["Error Feature"])
        fNameA.append(row["First Name"])
        lNameA.append(row["Last Name"])
        bDayA.append(row["Birth Date"])
        mailA.append(row["Email"])
        phoneA.append(row["Phone"])
        addressA.append(row["Address"])
        SSNA.append(row["SSN"])

    idA = []

    linked = linked.reset_index()
    for index, row in linked.iterrows():
        idA.append(row["idB"])

    trueindexA = []
    trueerrorA = []
    truefNameA = []
    truelNameA = []
    truebDayA = []
    truemailA = []
    truephoneA = []
    trueaddressA = []
    trueSSNA = []

    for i in range(0, len(boolA)):
        if boolA[i]:
            trueindexA.append(i+2)
            trueerrorA.append(errorA[i])
            if lNameA[i] == empty:
                truelNameA.append('empty')
            else:
                truelNameA.append(lNameA[i])
            if fNameA[i] == empty:
                truefNameA.append('empty')
            else:
                truefNameA.append(fNameA[i])
            if bDayA[i] == empty:
                truebDayA.append('empty')
            else:
                truebDayA.append(bDayA[i])
            if mailA[i] == empty:
                truemailA.append('empty')
            else:
                truemailA.append(mailA[i])
            if phoneA[i] == empty:
                truephoneA.append('empty')
            else:
                truephoneA.append(phoneA[i])
            if addressA[i] == empty:
                trueaddressA.append('empty')
            else:
                trueaddressA.append(addressA[i])
            if SSNA[i] == empty:
                trueSSNA.append('empty')
            else:
                trueSSNA.append(SSNA[i])

    idA = numpy.sort(idA)

    ind=[]
    err=[]
    fName = []
    lname =[]
    bday=[]
    mail =[]
    phone =[]
    address = []
    SSN=[]

    for i in range(0, len(trueerrorA)):
        l = 0
        for j in range(0,len(idA)):
            if idA[j]==trueindexA[i]:
                l=1
                break
        if l==0:
            err.append(trueerrorA[i])
            ind.append(trueindexA[i])
            fName.append(truefNameA[i])
            lname.append(truelNameA[i])
            bday.append(truebDayA[i])
            mail.append(truemailA[i])
            phone.append(truephoneA[i])
            address.append(trueaddressA[i])
            SSN.append(trueSSNA[i])

    print(len(err))


    C = {'index': ind,'bday':bday,'mail':mail,'phone':phone,'address':address,'SSN':SSN, 'error': err,'fname':fName,'lname':lname}
    donnees = pd.DataFrame(C, columns=['index','bday','mail','phone','address','SSN', 'error','fname','lname'])
    donnees.to_csv('nonlinkedB.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export1 fait")
#nonLinked(dataset_A,dataset_B,linked)
"""
def linkage(dataset_A ,dataset_B):
    tic_A = time.perf_counter() #time counter
    fNameA = [] #first name of Dataset A
    lNameA = [] #last name of Dataset A
    bDayA = [] #birth date of Dataset A
    mailA = [] #email of Dataset A
    phoneA = [] #phone number of Dataset A
    addressA = [] #address of Dataset A
    SSNA = [] #SSN of Dataset A
    boolA = [] #A value that allow the function to know whether an element of Dataset A is already considered as linked

    #%%
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows(): #Read of Dataset A
        fNameA.append(row["First Name"])
        lNameA.append(row["Last Name"])
        bDayA.append(row["Birth Date"])
        mailA.append(row["Email"])
        phoneA.append(row["Phone"])
        addressA.append(row["Address"])
        SSNA.append(row["SSN"])
        boolA.append(False)




    fNameB = [] #Same as before but for dataset B
    lNameB = []
    bDayB = []
    mailB = []
    phoneB = []
    addressB = []
    SSNB = []
    boolB = []

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        fNameB.append(row["First Name"])
        lNameB.append(row["Last Name"])
        bDayB.append(row["Birth Date"])
        mailB.append(row["Email"])
        phoneB.append(row["Phone"])
        addressB.append(row["Address"])
        SSNB.append(row["SSN"])
        boolB.append(False)


    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, SSNA, boolA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, SSNB, boolB]

    idA = [] # The two list that will save the ID of linked elements
    idB = []

    listi = [0,0] #The tuples we are considering
    listj = [1,1]
    lista = [2,5]

    for f in range(len(listi)):
        upletA = [] #The creation of the the tuple array
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("") #Completion of the tuple array, checking if it is not empty or already linked
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico
                # if the tuple is not empty or already linked, we concatenate its component and hash the concatenation
        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA) #Sorting the hashes while keeping in memory the ID of the hashes
        for k in range(len(fNameB)): #Same as the previous 'for'
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1): #Efficient comparison of sorted list
            if not upletA[k] == "": #verifying that the k-th tuple wasn't already linked or that one of its component was empty
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]: #if the hashes are equals, the IDs are linked
                    idA.append(indexA[k]+2) # we add the ID to the lists of linked IDs
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True  #We set the ID as already linked
                    registreB[7][indexB[l]] = True
                    l += 1

    listi = [1,1]#Same as previously
    listj = [3,6]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1


    listi = [0]#Same as previously
    listj = [1]
    lista = [4]

    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1

    listi = [2,2,4]#Same as previously
    listj = [5,4,5]
    for f in range(len(listi)):
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
    """
    listi = [0, 2,3, 4, 5,0,2,4,5] #Les uplets facultatifs en pratique
    listj = [6, 6,6, 6, 6,3,3,3,3]
    
    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico
    
        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)
        count = 0
        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    count += 1
                    l += 1
        print(count, " uplet ",listi[f],listj[f])
    """

    C = {'idA': idA, 'idB': idB} #We output the file linked.csv that contain the list of linked IDs
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export 1/3 fait")

    C = {'Value': registreA[7]} #We output the file OutputA.csv that contain the output True or False for all IDs of dataset A. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputA.csv', index=False, header=True, encoding='utf-8', sep=';')
    print("export 2/3 fait")

    C = {'Value': registreB[7]} #We output the file OutputA.csv that contain the output True or False for all IDs of dataset B. True means linked, False the opposite
    donnees = pd.DataFrame(C, columns=['Value'])
    donnees.to_csv('OutputB.csv', index=False, header=True, encoding='utf-8', sep=';')
    print("export 3/3 fait")
    print(-tic_A + time.perf_counter()) #Output of the execution time
linkage(dataset_A ,dataset_B)

def linkage64(dataset_A ,dataset_B):
    tic_A = time.perf_counter()
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    boolA = []
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
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, boolA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, boolB, SSNB]

    idA = []
    idB = []

    upletA = []
    upletB = []



    listi = []

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], "temps total = ", toc-tic)


    listi = [0,0,1,1,2,2,2,3,0,1,2,2,2,3,3,4]
    listj = [3,3,3,3,3,3,4,4,3,3,3,4,5,4,5,5]
    lista = [4,5,4,5,4,5,5,5,6,6,6,6,6,6,6,6]


    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)

    listi = [0,0]
    listj = [1,1]
    lista = [4,5]
    listb = [6,6]

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[listb[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]+registreA[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty or registreB[listb[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]+registreB[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)
    C = {'idA': idA, 'idB': idB}
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage64.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
    print(-tic_A + time.perf_counter())
#linkage64(dataset_A ,dataset_B)

def linkage80(dataset_A ,dataset_B):
    tic_A = time.perf_counter()
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    boolA = []
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
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, boolA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, boolB, SSNB]

    idA = []
    idB = []

    upletA = []
    upletB = []



    listi = []

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], "temps total = ", toc-tic)


    listi = [4,3,3,3]
    listj = [5,4,4,5]
    lista = [6,5,6,6]


    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)

    listi = [0,0,0,0,1,1,1]
    listj = [1,2,2,2,2,2,2]
    lista = [3,3,3,3,3,3,3]
    listb = [5,4,5,6,4,5,6]

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[listb[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]+registreA[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty or registreB[listb[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]+registreB[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)
    C = {'idA': idA, 'idB': idB}
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage80.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
    print(-tic_A + time.perf_counter())
#linkage80(dataset_A ,dataset_B)

def linkage100(dataset_A ,dataset_B):
    tic_A = time.perf_counter()
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
    stateA = []
    boolA = []
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
        boolB.append(False)
        SSNB.append(row["SSN"])

    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, stateA, boolA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, stateB, boolB, SSNB]

    idA = []
    idB = []

    upletA = []
    upletB = []



    listi = []

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], "temps total = ", toc-tic)


    listi = []
    listj = []
    lista = []


    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)

    listi = [5,6,6,6]
    listj = [4,5,4,5]
    lista = [3,4,3,3]
    listb = [2,3,2,2]

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[listb[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]+registreA[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[lista[f]][k] == empty or registreB[listb[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]+registreB[listb[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)

    listi = [5,6,6,6,5,6,6,6,5,6,6,6,5,6,6,6]
    listj = [4,4,5,5,4,4,5,5,4,4,5,5,4,4,5,5]
    lista = [3,3,3,4,3,3,3,4,3,3,3,4,3,3,3,4]
    listb = [1,1,1,1,2,2,2,2,2,2,1,1,2,2,2,2]
    listc = [0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1]

    for f in range(len(listi)):
        tic = time.perf_counter()
        upletA = []
        upletB = []
        for k in range(len(fNameA)):
            if registreA[listi[f]][k] == empty or registreA[listj[f]][k] == empty or registreA[listc[f]][k] == empty or registreA[lista[f]][k] == empty or registreA[listb[f]][k] == empty or registreA[7][k]:
                upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[listi[f]][k]+registreA[listj[f]][k]+registreA[lista[f]][k]+registreA[listb[f]][k]+registreA[listc[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[listi[f]][k] == empty or registreB[listj[f]][k] == empty or registreB[listc[f]][k] == empty or registreB[lista[f]][k] == empty or registreB[listb[f]][k] == empty:
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[listi[f]][k]+registreB[listj[f]][k]+registreB[lista[f]][k]+registreB[listb[f]][k]+registreB[listc[f]][k]).encode('utf-8')).hexdigest()) #hasher directement ici
        indexB = numpy.argsort(upletB)
        upletB = numpy.sort(upletB)

        l = 0
        for k in range(0,500000,1):
            if not upletA[k] == "":
                while l < 500000 and upletA[k] > upletB[l]:
                    l += 1
                if l < 500000 and upletA[k] == upletB[l]:
                    idA.append(indexA[k]+2)
                    idB.append(indexB[l]+2)
                    registreA[7][indexA[k]] = True
                    registreB[7][indexB[l]] = True
                    l += 1
        toc = time.perf_counter()
        print("Uplet i = ", listi[f], " j = ", listj[f], " k = ", lista[f], "temps total = ", toc-tic)

    C = {'idA': idA, 'idB': idB}
    donnees = pd.DataFrame(C, columns=['idA', 'idB'])
    donnees.to_csv('linkage100.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
    print(-tic_A + time.perf_counter())
#linkage100(dataset_A ,dataset_B)

def upletTest(dataset_A ,dataset_B):
    boolA = []
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        boolA.append(row["Overlapping"])

    boolB = []
    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        boolB.append(row["Overlapping"])

    dico = [1000, 1000, 36500, 10000000000, 100000000, 160000000, 50]
    upletList = []
    truePos = []
    trueNeg = []
    falseNeg = []
    falsePos = []
    dicoList = []

    for i in range(0, 6, 1): #1-uplets:
        print("i=", i)

        idA = []
        idB = []
        uplet = pd.read_csv(str(i)+"linked.csv", sep=';')
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

        attacDico = math.log2(dico[i])

        upletList.append(str(i))
        truePos.append(tp)
        falsePos.append(fp)
        trueNeg.append(tn)
        falseNeg.append(fn)
        dicoList.append(attacDico)

    for i in range(0, 6, 1): #2-uplets:
        for j in range(i+1, 6, 1):
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

            attacDico = math.log2(dico[i]*dico[j])

            upletList.append(str(j)+str(i))
            truePos.append(tp)
            falsePos.append(fp)
            trueNeg.append(tn)
            falseNeg.append(fn)
            dicoList.append(attacDico)

    for i in range(0, 6, 1): #3-uplets:
         for j in range(i+1, 6, 1):
              for a in range(j+1, 6, 1):

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

                attacDico = math.log2(dico[i]*dico[j]*dico[a])

                upletList.append(str(a)+str(j)+str(i))
                truePos.append(tp)
                falsePos.append(fp)
                trueNeg.append(tn)
                falseNeg.append(fn)
                dicoList.append(attacDico)

    for i in range(0, 6, 1): #4-uplets:
        for j in range(i+1, 6, 1):
            for a in range(j+1, 6, 1):
                for b in range(a+1, 6, 1):
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

                    attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b])

                    upletList.append(str(b)+str(a)+str(j)+str(i))
                    truePos.append(tp)
                    falsePos.append(fp)
                    trueNeg.append(tn)
                    falseNeg.append(fn)
                    dicoList.append(attacDico)

    for i in range(0, 6, 1): #5-uplets:
        for j in range(i+1, 6, 1):
            for a in range(j+1, 6, 1):
                for b in range(a+1, 6, 1):
                    for c in range(b+1, 6, 1):
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

                        attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c])

                        upletList.append(str(c)+str(b)+str(a)+str(j)+str(i))
                        truePos.append(tp)
                        falsePos.append(fp)
                        trueNeg.append(tn)
                        falseNeg.append(fn)
                        dicoList.append(attacDico)

    C = {'uplet': upletList, 'vrai positif': truePos, 'vrai négatif': trueNeg, 'faux négatif': falseNeg, 'faux positif': falsePos, 'attaque par dictionnaire': dicoList}
    donnees = pd.DataFrame(C, columns=['uplet', 'vrai positif', 'vrai négatif', 'faux négatif', 'faux positif', 'attaque par dictionnaire'])
    donnees.to_csv('UpletTest.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
#upletTest(dataset_A ,dataset_B)

def upletTestA(dataset_A ,dataset_B):
    boolA = []
    dataset_A = dataset_A.reset_index()
    for index, row in dataset_A.iterrows():
        boolA.append(row["Overlapping"])

    dico = [1000, 1000, 36500, 10000000000, 100000000, 160000000, 100000000]
    upletList = []
    truePos = []
    trueNeg = []
    falseNeg = []
    falsePos = []
    dicoList = []

    for i in range(0, 7, 1): #1-uplets:
        print("i=", i)

        idA = []
        idB = []
        uplet = pd.read_csv(str(i)+"linked.csv", sep=';')
        uplet = uplet.reset_index()

        fp = 0
        tp = 0

        for index, row in uplet.iterrows():
            idA.append(row["idA"])


        for k in range(len(idA)):

            if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                tp += 1
            else:
                fp += 1

        fn = 50000-tp
        tn = 500000-50000-fp

        attacDico = math.log2(dico[i])

        upletList.append(str(i))
        truePos.append(tp)
        falsePos.append(fp)
        trueNeg.append(tn)
        falseNeg.append(fn)
        dicoList.append(attacDico)

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


            for k in range(len(idA)):

                if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                    tp += 1
                else:
                    fp += 1

            fn = 50000-tp
            tn = 500000-50000-fp

            attacDico = math.log2(dico[i]*dico[j])

            upletList.append(str(j)+str(i))
            truePos.append(tp)
            falsePos.append(fp)
            trueNeg.append(tn)
            falseNeg.append(fn)
            dicoList.append(attacDico)

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


                for k in range(len(idA)):

                    if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                        tp += 1
                    else:
                        fp += 1

                fn = 50000-tp
                tn = 500000-50000-fp

                attacDico = math.log2(dico[i]*dico[j]*dico[a])

                upletList.append(str(a)+str(j)+str(i))
                truePos.append(tp)
                falsePos.append(fp)
                trueNeg.append(tn)
                falseNeg.append(fn)
                dicoList.append(attacDico)

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


                    for k in range(len(idA)):

                        if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                            tp += 1
                        else:
                            fp += 1

                    fn = 50000-tp
                    tn = 500000-50000-fp

                    attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b])

                    upletList.append(str(b)+str(a)+str(j)+str(i))
                    truePos.append(tp)
                    falsePos.append(fp)
                    trueNeg.append(tn)
                    falseNeg.append(fn)
                    dicoList.append(attacDico)

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

                        for k in range(len(idA)):

                            if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                tp += 1
                            else:
                                fp += 1

                        fn = 50000-tp
                        tn = 500000-50000-fp

                        attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c])

                        upletList.append(str(c)+str(b)+str(a)+str(j)+str(i))
                        truePos.append(tp)
                        falsePos.append(fp)
                        trueNeg.append(tn)
                        falseNeg.append(fn)
                        dicoList.append(attacDico)


    for i in range(0, 7, 1): #6-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c, "d=", d)
                            idA = []
                            idB = []
                            uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+"linked.csv", sep=';')
                            uplet = uplet.reset_index()

                            fp = 0
                            tp = 0

                            for index, row in uplet.iterrows():
                                idA.append(row["idA"])

                            for k in range(len(idA)):

                                if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                    tp += 1
                                else:
                                    fp += 1

                            fn = 50000-tp
                            tn = 500000-50000-fp

                            attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c]*dico[d])

                            upletList.append(str(d)+str(c)+str(b)+str(a)+str(j)+str(i))
                            truePos.append(tp)
                            falsePos.append(fp)
                            trueNeg.append(tn)
                            falseNeg.append(fn)
                            dicoList.append(attacDico)

    for i in range(0, 7, 1): #7-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            for e in range(d+1, 7, 1):
                                print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c, "d=", d, "e=", e)
                                idA = []
                                idB = []
                                uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+str(e)+"linked.csv", sep=';')
                                uplet = uplet.reset_index()

                                fp = 0
                                tp = 0

                                for index, row in uplet.iterrows():
                                    idA.append(row["idA"])

                                for k in range(len(idA)):

                                    if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                        tp += 1
                                    else:
                                        fp += 1

                                fn = 50000-tp
                                tn = 500000-50000-fp

                                attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c]*dico[d])

                                upletList.append(str(e)+str(d)+str(c)+str(b)+str(a)+str(j)+str(i))
                                truePos.append(tp)
                                falsePos.append(fp)
                                trueNeg.append(tn)
                                falseNeg.append(fn)
                                dicoList.append(attacDico)

    C = {'uplet': upletList, 'vrai positif': truePos, 'vrai négatif': trueNeg, 'faux négatif': falseNeg, 'faux positif': falsePos, 'attaque par dictionnaire': dicoList}
    donnees = pd.DataFrame(C, columns=['uplet', 'vrai positif', 'vrai négatif', 'faux négatif', 'faux positif', 'attaque par dictionnaire'])
    donnees.to_csv('UpletTestA.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
#upletTestA(dataset_A ,dataset_B)

def upletTestB(dataset_A ,dataset_B):
    boolA = []
    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        boolA.append(row["Overlapping"])

    dico = [1000, 1000, 36500, 10000000000, 100000000, 160000000, 100000000]
    upletList = []
    truePos = []
    trueNeg = []
    falseNeg = []
    falsePos = []
    dicoList = []

    for i in range(0, 7, 1): #1-uplets:
        print("i=", i)

        idA = []
        idB = []
        uplet = pd.read_csv(str(i)+"linked.csv", sep=';')
        uplet = uplet.reset_index()

        fp = 0
        tp = 0

        for index, row in uplet.iterrows():
            idA.append(row["idB"])


        for k in range(len(idA)):

            if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                tp += 1
            else:
                fp += 1

        fn = 50000-tp
        tn = 500000-50000-fp

        attacDico = math.log2(dico[i])

        upletList.append(str(i))
        truePos.append(tp)
        falsePos.append(fp)
        trueNeg.append(tn)
        falseNeg.append(fn)
        dicoList.append(attacDico)

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
                idA.append(row["idB"])


            for k in range(len(idA)):

                if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                    tp += 1
                else:
                    fp += 1

            fn = 50000-tp
            tn = 500000-50000-fp

            attacDico = math.log2(dico[i]*dico[j])

            upletList.append(str(j)+str(i))
            truePos.append(tp)
            falsePos.append(fp)
            trueNeg.append(tn)
            falseNeg.append(fn)
            dicoList.append(attacDico)

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
                    idA.append(row["idB"])


                for k in range(len(idA)):

                    if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                        tp += 1
                    else:
                        fp += 1

                fn = 50000-tp
                tn = 500000-50000-fp

                attacDico = math.log2(dico[i]*dico[j]*dico[a])

                upletList.append(str(a)+str(j)+str(i))
                truePos.append(tp)
                falsePos.append(fp)
                trueNeg.append(tn)
                falseNeg.append(fn)
                dicoList.append(attacDico)

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
                        idA.append(row["idB"])


                    for k in range(len(idA)):

                        if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                            tp += 1
                        else:
                            fp += 1

                    fn = 50000-tp
                    tn = 500000-50000-fp

                    attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b])

                    upletList.append(str(b)+str(a)+str(j)+str(i))
                    truePos.append(tp)
                    falsePos.append(fp)
                    trueNeg.append(tn)
                    falseNeg.append(fn)
                    dicoList.append(attacDico)

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
                            idA.append(row["idB"])

                        for k in range(len(idA)):

                            if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                tp += 1
                            else:
                                fp += 1

                        fn = 50000-tp
                        tn = 500000-50000-fp

                        attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c])

                        upletList.append(str(c)+str(b)+str(a)+str(j)+str(i))
                        truePos.append(tp)
                        falsePos.append(fp)
                        trueNeg.append(tn)
                        falseNeg.append(fn)
                        dicoList.append(attacDico)


    for i in range(0, 7, 1): #6-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c, "d=", d)
                            idA = []
                            idB = []
                            uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+"linked.csv", sep=';')
                            uplet = uplet.reset_index()

                            fp = 0
                            tp = 0

                            for index, row in uplet.iterrows():
                                idA.append(row["idB"])

                            for k in range(len(idA)):

                                if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                    tp += 1
                                else:
                                    fp += 1

                            fn = 50000-tp
                            tn = 500000-50000-fp

                            attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c]*dico[d])

                            upletList.append(str(d)+str(c)+str(b)+str(a)+str(j)+str(i))
                            truePos.append(tp)
                            falsePos.append(fp)
                            trueNeg.append(tn)
                            falseNeg.append(fn)
                            dicoList.append(attacDico)

    for i in range(0, 7, 1): #7-uplets:
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            for e in range(d+1, 7, 1):
                                print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c, "d=", d, "e=", e)
                                idA = []
                                idB = []
                                uplet = pd.read_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+str(e)+"linked.csv", sep=';')
                                uplet = uplet.reset_index()

                                fp = 0
                                tp = 0

                                for index, row in uplet.iterrows():
                                    idA.append(row["idB"])

                                for k in range(len(idA)):

                                    if boolA[idA[k]-2]: #Le -2 vient du passage d'un tableau d'index minimal 2 à 0
                                        tp += 1
                                    else:
                                        fp += 1

                                fn = 50000-tp
                                tn = 500000-50000-fp

                                attacDico = math.log2(dico[i]*dico[j]*dico[a]*dico[b]*dico[c]*dico[d])

                                upletList.append(str(e)+str(d)+str(c)+str(b)+str(a)+str(j)+str(i))
                                truePos.append(tp)
                                falsePos.append(fp)
                                trueNeg.append(tn)
                                falseNeg.append(fn)
                                dicoList.append(attacDico)

    C = {'uplet': upletList, 'vrai positif': truePos, 'vrai négatif': trueNeg, 'faux négatif': falseNeg, 'faux positif': falsePos, 'attaque par dictionnaire': dicoList}
    donnees = pd.DataFrame(C, columns=['uplet', 'vrai positif', 'vrai négatif', 'faux négatif', 'faux positif', 'attaque par dictionnaire'])
    donnees.to_csv('UpletTestB.csv', index=True, header=True, encoding='utf-8', sep=';')
    print("export fait")
#upletTestB(dataset_A ,dataset_B)

def linkedUplet(dataset_A ,dataset_B):
    fNameA = []
    lNameA = []
    bDayA = []
    mailA = []
    phoneA = []
    addressA = []
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
        SSNA.append(row["SSN"])

    fNameB = []
    lNameB = []
    bDayB = []
    mailB = []
    phoneB = []
    addressB = []
    SSNB = []

    dataset_B = dataset_B.reset_index()
    for index, row in dataset_B.iterrows():
        fNameB.append(row["First Name"])
        lNameB.append(row["Last Name"])
        bDayB.append(row["Birth Date"])
        mailB.append(row["Email"])
        phoneB.append(row["Phone"])
        addressB.append(row["Address"])
        SSNB.append(row["SSN"])
    registreA = [fNameA, lNameA, bDayA, mailA, phoneA, addressA, SSNA]
    registreB = [fNameB, lNameB, bDayB, mailB, phoneB, addressB, SSNB]


    for i in range(0, 7, 1): #1-uplets:

        print("i=", i,)
        upletA = []
        upletB = []
        idA = []
        idB = []

        for k in range(len(fNameA)):
            if registreA[i][k] == empty:
                 upletA.append("")
            else:
                upletA.append(hashlib.sha256((registreA[i][k]).encode('utf-8')).hexdigest()) #hasher directement ico

        indexA = numpy.argsort(upletA)
        upletA = numpy.sort(upletA)
        for k in range(len(fNameB)):
            if registreB[i][k] == empty :
                upletB.append("")
            else:
                upletB.append(hashlib.sha256((registreB[i][k]).encode('utf-8')).hexdigest()) #hasher directement ici

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
                    l += 1


        C = {'idA': idA, 'idB': idB}
        donnees = pd.DataFrame(C, columns=['idA', 'idB'])
        donnees.to_csv(str(i)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
        print("export fait")


    for i in range(0, 7, 1): #2-uplets:
        for j in range(i+1, 7, 1):
            print("i=", i, "j=", j)
            upletA = []
            upletB = []
            idA = []
            idB = []

            for k in range(len(fNameA)):
                if registreA[i][k] == empty or registreA[j][k] == empty:
                    upletA.append("")
                else:
                    upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]).encode('utf-8')).hexdigest()) #hasher directement ico

            indexA = numpy.argsort(upletA)
            upletA = numpy.sort(upletA)
            for k in range(len(fNameB)):
                if registreB[i][k] == empty or registreB[j][k] == empty:
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
                    if registreA[i][k] == empty or registreA[j][k] == empty or registreA[a][k] == empty:
                        upletA.append("")
                    else:
                        upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                indexA = numpy.argsort(upletA)
                upletA = numpy.sort(upletA)
                for k in range(len(fNameB)):
                    if registreB[i][k] == empty or registreB[j][k] == empty or registreB[a][k] == empty:
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
                        if registreA[i][k] == empty or registreA[j][k] == empty or registreA[a][k] == empty or registreA[b][k] == empty:
                            upletA.append("")
                        else:
                            upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                    indexA = numpy.argsort(upletA)
                    upletA = numpy.sort(upletA)
                    for k in range(len(fNameB)):
                        if registreB[i][k] == empty or registreB[j][k] == empty or registreB[a][k] == empty or registreB[b][k] == empty:
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
                            if registreA[i][k] == empty or registreA[j][k] == empty or registreA[a][k] == empty or registreA[b][k] == empty or registreA[c][k] == empty:
                                upletA.append("")
                            else:
                                upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]+registreA[c][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                        indexA = numpy.argsort(upletA)
                        upletA = numpy.sort(upletA)
                        for k in range(len(fNameB)):
                            if registreB[i][k] == empty or registreB[j][k] == empty or registreB[a][k] == empty or registreB[b][k] == empty or registreB[c][k] == empty:
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

    for i in range(0, 7, 1): #5-uplets
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c,"d=",d)
                            upletA = []
                            upletB = []
                            idA = []
                            idB = []

                            for k in range(len(fNameA)):
                                if registreA[i][k] == empty or registreA[j][k] == empty or registreA[a][k] == empty or registreA[b][k] == empty or registreA[c][k] == empty or registreA[d][k] == empty:
                                    upletA.append("")
                                else:
                                    upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]+registreA[c][k]+registreA[d][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                            indexA = numpy.argsort(upletA)
                            upletA = numpy.sort(upletA)
                            for k in range(len(fNameB)):
                                if registreB[i][k] == empty or registreB[j][k] == empty or registreB[a][k] == empty or registreB[b][k] == empty or registreB[c][k] == empty or registreB[d][k] == empty:
                                    upletB.append("")
                                else:
                                    upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]+registreB[c][k]+registreB[d][k]).encode('utf-8')).hexdigest()) #hasher directement ici
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
                            donnees.to_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
                            print("export fait")

    for i in range(0, 7, 1): #5-uplets
        for j in range(i+1, 7, 1):
            for a in range(j+1, 7, 1):
                for b in range(a+1, 7, 1):
                    for c in range(b+1, 7, 1):
                        for d in range(c+1, 7, 1):
                            for e in range(d+1, 7, 1):
                                print("i=", i, "j=", j, "a=", a, "b=", b, "c=", c,"d=",d)
                                upletA = []
                                upletB = []
                                idA = []
                                idB = []

                                for k in range(len(fNameA)):
                                    if registreA[i][k] == empty or registreA[j][k] == empty or registreA[a][k] == empty or registreA[b][k] == empty or registreA[c][k] == empty or registreA[d][k] == empty or registreA[e][k] == empty:
                                        upletA.append("")
                                    else:
                                        upletA.append(hashlib.sha256((registreA[i][k]+registreA[j][k]+registreA[a][k]+registreA[b][k]+registreA[c][k]+registreA[d][k]+registreA[e][k]).encode('utf-8')).hexdigest()) #hasher directement ico

                                indexA = numpy.argsort(upletA)
                                upletA = numpy.sort(upletA)
                                for k in range(len(fNameB)):
                                    if registreB[i][k] == empty or registreB[j][k] == empty or registreB[a][k] == empty or registreB[b][k] == empty or registreB[c][k] == empty or registreB[d][k] == empty or registreB[e][k] == empty:
                                        upletB.append("")
                                    else:
                                        upletB.append(hashlib.sha256((registreB[i][k]+registreB[j][k]+registreB[a][k]+registreB[b][k]+registreB[c][k]+registreB[d][k]+registreB[e][k]).encode('utf-8')).hexdigest()) #hasher directement ici
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
                                donnees.to_csv(str(i)+str(j)+str(a)+str(b)+str(c)+str(d)+str(e)+'linked.csv', index=True, header=True, encoding='utf-8', sep=';')
                                print("export fait")
#linkedUplet(dataset_A ,dataset_B)