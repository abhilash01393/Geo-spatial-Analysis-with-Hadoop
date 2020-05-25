
import psycopg2
import os
import sys
import math
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cursor = openconnection.cursor()
    #RangeQuery for Range Partitions
    ratingMaxValueCeil = math.ceil(ratingMaxValue)  
    ratingsrange = (ratingMinValue, ratingMaxValueCeil)
    f = open(outputPath, "w")
    cursor.execute("SELECT PARTITIONNUM FROM RANGERATINGSMETADATA WHERE MAXRATING >= %s and MAXRATING <= %s;", ratingsrange)
    records = cursor.fetchall()
    if ratingMinValue == 0 and ratingMaxValue == 0:
        records.append((0,))

    for partitions in records:
        partition = partitions[0]
        partitionName = "RangeRatingsPart" + str(partition)
        query = "SELECT * FROM " + partitionName + " WHERE RATING >= " + str(ratingMinValue) + " AND RATING <= " + str(ratingMaxValue) + ";"
        cursor.execute(query)
        records = cursor.fetchall()
        
        for row in records:
            line = partitionName + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n"
            f.write(line)
    #RangeQuery for Round Robin Partition
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'roundrobinratingspart%';")
    roundRobinParts = cursor.fetchall()
    i = 0
    for partition in roundRobinParts:
        roundRobinPart = partition[0]
        partitionName = "RoundRobinRatingsPart" + str(i)
        i = i + 1
        query = "SELECT * FROM " + roundRobinPart + " WHERE RATING >= " + str(ratingMinValue) + " AND RATING <= " + str(ratingMaxValue) + ";"
        cursor.execute(query)
        records = cursor.fetchall()
        
        for row in records:
            line = partitionName + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n"
            f.write(line)
        
    f.close()


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    f = open(outputPath, "w")
    cursor = openconnection.cursor()
    #PointQuery for Range Partitions
    query = "SELECT partitionnum FROM rangeratingsmetadata where minrating <= " + str(ratingValue) + " AND maxrating >= " + str(ratingValue) + ";"
    cursor.execute(query)
    records = cursor.fetchall()
    partition = records[0][0]
    partitionName = "RangeRatingsPart" + str(partition)
    query = "SELECT * FROM " + partitionName + " WHERE RATING = " + str(ratingValue) +";"
    cursor.execute(query)

    records = cursor.fetchall()

    for row in records:
        line = partitionName + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n"
        f.write(line)
    #PointQuery for Round Robin Partition
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'roundrobinratingspart%';")
    roundRobinParts = cursor.fetchall()
    i = 0
    for partition in roundRobinParts:
        roundRobinPart = partition[0]
        partitionName = "RoundRobinRatingsPart" + str(i)
        i = i + 1
        query = "SELECT * FROM " + roundRobinPart + " WHERE RATING = " + str(ratingValue) + ";"
        cursor.execute(query)
        records = cursor.fetchall()
        
        for row in records:
            line = partitionName + "," + str(row[0]) + "," + str(row[1]) + "," + str(row[2]) + "\n"
            f.write(line)
        
    f.close()

