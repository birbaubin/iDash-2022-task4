SIZE = 100000

datasetA = open("party_a/dataAEn.csv", "r")
datasetAUniTest = open("party_a/datasetAUniTest.csv", "w")
for i in range(SIZE):
    datasetAUniTest.write(datasetA.readline())

datasetA.close()
datasetAUniTest.close()

datasetB = open("party_b/dataBEn.csv", "r")
datasetBUniTest = open("party_b/datasetBUniTest.csv", "w")
for i in range(SIZE):
    datasetBUniTest.write(datasetB.readline())

datasetB.close()
datasetBUniTest.close()

