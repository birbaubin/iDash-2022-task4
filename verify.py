import pandas as pd


empty = '9b2d5b4678781e53038e91ea5324530a03f27dc1d0e5f6c9bc9d493a23be9de0'  # The hash value of empty
#linked = pd.read_csv("linkage.csv", sep=';')

def verifyOutput(dataset,output,str):
    bool = []
    for index, row in dataset.iterrows():  # Read of Dataset A
        bool.append(row["Overlapping"])
    value = []
    for index, row in output.iterrows():  # Read of Dataset A
        value.append(row["Value"])
    Vn = 0
    Vp = 0
    Fn = 0
    Fp = 0
    for i in range(len(bool)):
        if bool[i]==True:
            if value[i]==True:
                Vp+=1
            else:
                Fn+=1
        else:
            if value[i]==True:
                Fp+=1
            else:
                Vn+=1
    print(str+"Vrai positif : ",Vp)
    print(str+"Vrai négatif : ",Vn)
    print(str+"Faux positif : ",Fp)
    print(str+"Faux négatif : ",Fn)
    print(str+"accuracy : ", (Vp+Vn)/len(bool)*100, "%")


#### Verification of the results from A

#dataset_A = pd.read_csv("dataAEn.csv")  # Opening dataset A
dataset_A = pd.read_csv("datasetAUniTest.csv")  # Opening dataset A
output_A = pd.read_csv("OutputA.csv")  # Opening dataset A
verifyOutput(dataset_A,output_A,"A : ")


#### Verification of the results from B

#output_B = pd.read_csv("OutputB.csv")  # Opening dataset B
#dataset_B = pd.read_csv("dataBEn.csv")  # Opening dataset B
#verifyOutput(dataset_B,output_B,"B : ")

