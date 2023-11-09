import json
import re

def storeIndices(indicies: list):
    indexNum = 0
    for index in indicies:
        indexNum += 1
        with open(f"partial_index{indexNum}.json", "w") as f:
            json.dump(index, f, indent=0, separators=(", ", ": "))
            f.write("\n")

def createIndexMap():
    byteIndexMap = {}

    for fileNumber in range(1,4):
        fileName = f"partial_index{fileNumber}.json"
        with open(fileName, 'rb') as fileObject:
            while line := fileObject.readline():
                # check if the line starts with a quote 
                if line.startswith(b'"'):
                    # clean up line by removing non-word chars
                    cleanKey = re.sub(r'[^w]', '', line.decode('utf-8'))
                    # store 
                    byteIndexMap[cleanKey] = [fileNumber, fileObject.tell() - len(line)]
    
    # need to figure out how to save index map to JSON file 

    # return map 
    return byteIndexMap

def makeIndex(path : str) -> list:
    # unfinished
    partialIndex1 = {}
    partialIndex2 = {}
    partialIndex3 = {}

if __name__ == "__main__":
    # call our functions from these files to create our inverse index 
    print("starting inverse index")

    