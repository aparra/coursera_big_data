#!/usr/bin/env python

def split_fileA(line):
    word, count = map(lambda e: e.strip(), line.strip().split(','))
    return (word, int(count))

def split_fileB(line):
    data, count = map(lambda e: e.strip(), line.strip().split(','))
    date, word = data.split(' ')
    return (word, date + " " + count)
 
fileA = sc.textFile("input/join1_FileA.txt")    
fileA_data = fileA.map(split_fileA)

fileB = sc.textFile("input/join1_FileB.txt")
fileB_data = fileB.map(split_fileB)

fileB_joined_FileA = fileB_data.join(fileA_data)

print fileB_joined_FileA.collect()
