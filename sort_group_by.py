import pdb
import string
import functools
import random
import math
import uuid

# max file merged count once
MAX_RUN = 8                 

# max row count in cache buff, write to disk when outnumber this value
MAX_CACHED_ROW = 10000       

TMP_FILE_PATH = "./tmp/"
TMP_FILE_PREFIX = "chched_file_"


FUNC_SUM = 0
FUNC_MAX = 1
FUNC_MIN = 2
FUNC_COUNT = 3

# sort tow rows according sort by column
# return value:
# 0 row1 == row2
# 1 row1 > row2
# -1 row1 < row2
def CompareRow(row1, row2, sortBy):
        for i in sortBy:
            if row1[i] > row2[i]:
                return 1
            elif row1[i] < row2[i]:
                return -1
        return 0

#file used to save rows int cache
class RecordFile:
    def __init__(self, path=""):
        if path == "":
            path = TMP_FILE_PATH + str(uuid.uuid1())
        self.Path = path
        
    def OpenForWrite(self):
        self.WriteFd = open(self.Path, 'w')
        return True
    
    def OpenForRead(self):
        self.ReadFd = open(self.Path, "r")
        return True
    
    def CloseWrite(self):
        self.WriteFd.flush()
        self.WriteFd.close()
        
    def PutRow(self, row):
        line = ''
        for item in row:
            line = line + str(item) + ' '
            
        self.WriteFd.write(line.strip() + '\n')
        
    def GetRow(self):
        line = self.ReadFd.readline()
        if line == "":
            return None
        
        row = []
        for item in (line.strip('\n').split(' ')):
            row.append(int(item))
            
        return row
    
    def Open(self):
        return self.OpenForRead()
    
    def Fetch(self):
        return self.GetRow()
    
# x is a complete tree index
def TREE_IS_LEFT(x):
    return x & 1

def TREE_IS_RIGHT(x):
    return not TREE_IS_LEFT(x)

def TREE_PARENT(x):
    if TREE_IS_LEFT(x):
        return math.floor(x / 2)
    else:
        return math.floor(x / 2 - 1)
    
def TREE_SIBLING(x):
    if TREE_IS_LEFT(x):
        return int(x + 1)
    else:
        return int(x - 1)
    
class SelectTree:
    def __init__(self, cachedFiles, sortBy):
        self.RunList = cachedFiles
        self.SortBy = sortBy
        
        log2upbound = int(math.log(len(cachedFiles), 2))
        log2upbound = int(math.pow(2, log2upbound))
        if log2upbound < len(cachedFiles):
            log2upbound = log2upbound * 2
          
        self.LoserIdx = [-1] * int(log2upbound - 1)  # run index list
        self.WinnerIdx = [-1] * int(log2upbound - 1)  # run index list
        self.RowList = [None] * log2upbound
        
        for i in range(0, len(cachedFiles)):
            cachedFiles[i].Open()
            self.RowList[i] = cachedFiles[i].Fetch()
        
        # the first round compete
        treeIdx = int((log2upbound - 2) / 2)
        for i in range(0, len(cachedFiles), 2):
            result = self.__Compete(i, i + 1)
            self.WinnerIdx[treeIdx] = result[0]
            self.LoserIdx[treeIdx] = result[1]
            treeIdx = treeIdx + 1
        
        # loop compete till get the final winner 
        last = len(self.LoserIdx) - 1;
        while last > 0:
            first = int(last / 2);
            for i in range(first, last + 1, 2):
                parent = TREE_PARENT(i)
                result = self.__Compete(self.WinnerIdx[i], self.WinnerIdx[i+1])
                self.WinnerIdx[parent] = result[0]
                self.LoserIdx[parent] = result[1]
            last = TREE_PARENT(last)
        
    def Pop(self):
        # popIdx is RUN INDEX
        popIdx = self.WinnerIdx[0]
        row = self.RowList[popIdx]
        if row == None:
            return None
        
        self.RowList[popIdx] = self.RunList[popIdx].Fetch()
        
        # update parent is TREE INDEX
        update = popIdx + len(self.LoserIdx)
        parent = TREE_PARENT(update)
        result = self.__Compete(popIdx, self.LoserIdx[parent])
        self.WinnerIdx[parent] = result[0]
        self.LoserIdx[parent] = result[1]
        update = parent
        
        while update > 0:
            parent = TREE_PARENT(update)
            result = self.__Compete(self.WinnerIdx[update], self.LoserIdx[parent])
            self.WinnerIdx[parent] = result[0]
            self.LoserIdx[parent] = result[1]
            update = parent
            
        return row

    # return value: (winner, loser) pair    
    def __Compete(self, x, y):
        if self.RowList[x] == None:
            return (y, x)
        elif self.RowList[y] == None:
            return (x, y)
        else:
            if self.__CmpRow(self.RowList[x], self.RowList[y]) >= 0:
                return (y, x)
            else:
                return (x, y)
                
    def __CmpRow(self, x, y):
        return CompareRow(x, y, self.SortBy)

# base class of all EXEC OBJECTs
class Exec:
    def Open(self):
        return None

    def Fetch(self):
        return None

# read result set in file secquencial      
class Scan(Exec):
        def __init__(self, path):
                self.Path = path
                self.ScanFile = RecordFile(self.Path)

        def Open(self):
                self.ScanFile.OpenForRead()

        def Fetch(self):
                return self.ScanFile.GetRow()

# sort the result set according sort by column
class Sort(Exec):
    def __init__(self,  SubExec, SortBy):
        self.SubPlan = SubExec
        self.SortBy = SortBy
        self.SortBuff = []
        self.FileList = []
        self.Index = 0
        
    def Open(self):
        self.SubPlan.Open()
        self.__Sort()
        self.__MergeAll()
        self.IsFileSort = True
        
        if len(self.FileList) == 0:
            self.IsFileSort = False
        else:
            if len(self.FileList) > 1:
                self.SelTree = SelectTree(self.FileList, self.SortBy)
            else:
                self.FileList[0].Open()

    def Fetch(self):
        if not self.IsFileSort:
            if self.Index == len(self.SortBuff):
                return None
    
            self.Index = self.Index + 1
            return self.SortBuff[self.Index - 1]
        else:
            if len(self.FileList) == 1:
                return self.FileList[0].Fetch()
            else:
                return self.SelTree.Pop()
            
    def __Sort(self):
        eof = False;
        while not eof:
            for i in range(0, MAX_CACHED_ROW):
                row = self.SubPlan.Fetch()
                if not row:
                    eof = True
                    break
                else:
                    self.SortBuff.append(row)
            
            self.SortBuff.sort(key = functools.cmp_to_key(self.__CmpRow))
            
            if not eof or len(self.FileList) > 0:
                file = RecordFile()
                file.OpenForWrite()
                for row in self.SortBuff:
                    file.PutRow(row)
                file.CloseWrite()
                self.FileList.append(file)
                self.SortBuff = []

    def __MergeAll(self):
        while len(self.FileList) > 1:
            tmpFiles1 = []
            tmpFiles2 = []
            i = 0
            for f in self.FileList:
                tmpFiles1.append(f)
                i = i + 1
                if i % MAX_RUN == 0:
                    file = self.__MergeToOne(tmpFiles1)
                    tmpFiles1 = []
                    tmpFiles2.append(file)
            if len(tmpFiles1) > 1:
                file = self.__MergeToOne(tmpFiles1)
                tmpFiles2.append(file)
            elif len(tmpFiles1) == 1:
                tmpFiles2.append(tmpFiles1[0])
            
            self.FileList = tmpFiles2

    def __MergeToOne(self, cachedFiles):
        tree = SelectTree(cachedFiles, self.SortBy)
        file = RecordFile()
        file.OpenForWrite()
        while True:
            row = tree.Pop()
            if row == None:
                break

            file.PutRow(row)
        
        file.CloseWrite()
        return file       

    def __CmpRow(self, row1, row2):
        return CompareRow(row1, row2, self.SortBy)

# merge all the sub EXEC OBJECTs which is ordered
class Merge(Exec):
    def __init__(self, subList, sortBy):
        self.SubList = subList
        self.SortBy = sortBy
        
    def Open(self):
        self.SelTree = SelectTree(self.SubList, self.SortBy)

    def Fetch(self):
        return self.SelTree.Pop()

# aggregate the rows from sub EXEC OBJECT which is ordered    
class Aggregate(Exec):
    def __init__(self, sub, groupBy, aggIndex, aggFunc):
        self.Sub = sub
        self.GroupBy = groupBy
        self.AggIndex = aggIndex
        self.AggFunc = aggFunc
    
    def Open(self):
        self.Sub.Open()
        self.NextRow = self.Sub.Fetch()
        
    def Fetch(self):   
        if self.NextRow == None:
            return None

        aggRow = self.NextRow
        
        while True:
            self.NextRow = self.Sub.Fetch()
            if self.NextRow == None:
                break
            
            if self.__CmpRow(aggRow, self.NextRow) == 0:
                aggRow = self.__AggregateRow(aggRow, self.NextRow)
            else:
                break
        
        return aggRow
    
    def __AggregateRow(self, row1, row2):
        row = row1
        for i in range (0, len(self.AggFunc)):
            aggCol = self.AggIndex[i]
            if self.AggFunc[i] == FUNC_SUM:
                row[aggCol] = row1[aggCol] + row2[aggCol]
            #TODO(other aggregate function)
        return row
            
            
    def __CmpRow(self, row1, row2):
        return CompareRow(row1, row2, self.GroupBy)

# create a test .csv file
def RandomCreateTestFile(path, rows, columns):
    file = RecordFile(path)
    file.OpenForWrite()
    for row in range(0, rows):
        record = []
        for column in range(0, columns):
            record.append(random.randint(0,1000))
        file.PutRow(record)
    file.CloseWrite()
    

def Main():
    file1 = "./tmp/t1.csv"
    file2 = "./tmp/t2.csv"
    
    RandomCreateTestFile(file1, 100000, 3)
    RandomCreateTestFile(file2, 100000, 3)
    
    sortBy = [0, 1]
    groupBy = sortBy
    aggIndex = [2]
    aggFunc = [FUNC_SUM]

    scanExec1 = Scan(file1)
    scanExec2 = Scan(file2)
    sortExec1 = Sort(scanExec1, sortBy)
    sortExec2 = Sort(scanExec2, sortBy) 
    mergeExec = Merge([sortExec1, sortExec2], sortBy)
    aggExec = Aggregate(mergeExec, groupBy, aggIndex, aggFunc)
    
    run = RecordFile("./tmp/result.csv")
    run.OpenForWrite()
    
    count = 0

    aggExec.Open()
    while True:
        row = aggExec.Fetch()
        if row == None:
            break
        
        run.PutRow(row)
        count = count + 1
        #print (row)
    print (str(count) + " rows fetched")
    
    run.CloseWrite()
    run.OpenForRead()
  
Main()
