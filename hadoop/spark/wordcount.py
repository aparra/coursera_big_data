#!/usr/bin/env python

def split_words(line):
    return line.split()

def create_pair(word):
    return (word, 1)

text_RDD = sc.textFile("file:///home/cloudera/testfile1") # from local filesystem
pairs_RDD = text_RDD.flatMap(split_words).map(create_pair)

def sum_counts(a, b):
    return a + b

wordcounts_RDD = pairs_RDD.reduceByKey(sum_counts)

print wordcounts_RDD.collect()

