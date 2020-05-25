#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.\
    try:
       
        cur = openconnection.cursor()        
        interval, rangeMin = findRange(InputTable,SortingColumnName,openconnection)
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "'")
        records = cur.fetchall() 
        for i in range(5):
            
            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
            cur.execute("CREATE TABLE " + tableName + " ("+records[0][0]+" "+records[0][1]+")")	
            for d in range(1, len(records)):
                cur.execute("ALTER TABLE " + tableName + " ADD COLUMN " + records[d][0] + " " + records[d][1] + ";")
  
        thread = [0,0,0,0,0]
        for i in range(5):
            if i == 0:
                lower = rangeMin
                upper = rangeMin + interval
            else:
                lower = upper
                upper = upper + interval

            thread[i] = threading.Thread(target=range_insert_sort, args=(InputTable, SortingColumnName, i, lower, upper, openconnection))
            thread[i].start()

        for x in range(0,5):
            thread[i].join()
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " ("+records[0][0]+" "+records[0][1]+")")
        for i in range(1, len(records)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + records[i][0] + " " + records[i][1] + ";")
        for i in range(5):
            query = "INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + ""
            cur.execute(query)
    except Exception as message:
        print("Exception handles:", message)

    finally:    
        
        for i in range(5):
            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
        openconnection.commit()
        cur.close()
        
def findRange(InputTable, SortingColumnName,openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
    minRange=cur.fetchone()
    range_min_value = (float)(minRange[0])
    
    cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
    maxRange=cur.fetchone()
    range_max_value = (float)(maxRange[0])
    
    interval = (range_max_value - range_min_value)/5
    return interval , range_min_value

def range_insert_sort(InputTable, SortingColumnName, index, lower, upper, openconnection):

    cur=openconnection.cursor()
    table_name = "range_part" + str(index)
    if index == 0:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(lower) + " AND " + SortingColumnName + " <= " + str(upper) + " ORDER BY " + SortingColumnName + " ASC"
    else:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(lower) + " AND " + SortingColumnName + " <= " + str(upper) + " ORDER BY " + SortingColumnName + " ASC"

    cur.execute(query)
    cur.close()
    return


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    try:
        cur = openconnection.cursor()
        interval , range_min_value = find_Min_Max(InputTable1, InputTable2 , Table1JoinColumn , Table2JoinColumn, openconnection)
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        records1 = cur.fetchall()
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        records2 = cur.fetchall()
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        column1name = InputTable1 + "." + records1[0][0]
        cur.execute("CREATE TABLE " + OutputTable + " (\""+column1name+"\" "+records2[0][1]+")")
        
        for i in range(1, len(records1)):
            columnName = InputTable1 + "." + records1[i][0]
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN \"" + columnName + "\" " + records1[i][1] + ";")
            
        for i in range(len(records2)):
            columnName = InputTable2 + "." + records2[i][0]
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN \"" + columnName + "\"" +" " + records2[i][1] + ";")
            
        OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, records1, records2 , interval , range_min_value, openconnection)

        thread = [0,0,0,0,0]
        for i in range(5):
            thread[i] = threading.Thread(target=range_insert_join, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
            thread[i].start()
            
        for x in range(0,5):
            thread[i].join()

        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table_temp" + str(i))

    except Exception as detail:
        print ("Exception handled:", detail)

    finally:
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS table1_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS table2_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS output_table_temp" + str(i))
        openconnection.commit()
        cur.close()


def range_insert_join(Table1JoinColumn, Table2JoinColumn, openconnection, TempTableId):
	
    cur=openconnection.cursor()
    queryString = "INSERT INTO output_table_temp" + str(TempTableId) + " SELECT * FROM table1_range" + str(TempTableId) + " INNER JOIN table2_range" + str(TempTableId) + " ON table1_range" + str(TempTableId) + "." + Table1JoinColumn + "=" + "table2_range" + str(TempTableId) + "." + Table2JoinColumn + ";"
    cur.execute(queryString)
    cur.close()
    return

def find_Min_Max(InputTable1, InputTable2 , Table1JoinColumn , Table2JoinColumn, openconnection):
        cur = openconnection.cursor()
        cur.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        minimum1=cur.fetchone()
        Min1 = (float)(minimum1[0])

        cur.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        minimum2=cur.fetchone()
        Min2 = (float)(minimum2[0])
	
        cur.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
        maximum1=cur.fetchone()
        Max1 = (float)(maximum1[0])

        cur.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
        maximum2=cur.fetchone()
        Max2 = (float)(maximum2[0])

        if Max1 > Max2:
            rangeMax = Max1
        else:
            rangeMax = Max2

        if Min1 > Min2:
            rangeMin = Min2
        else:
            rangeMin = Min1    

        interval = (rangeMax - rangeMin)/5

        return interval , rangeMin

def OutputRangeTable(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, records1 , records2 , interval , range_min_value, openconnection):
    cur = openconnection.cursor();
    for i in range(5):

            table1_range_name = "table1_range" + str(i)
            table2_range_name = "table2_range" + str(i)

            if i==0:
                lowerValue = range_min_value
                upperValue = range_min_value + interval
            else:
                lowerValue = upperValue
                upperValue = upperValue + interval

            cur.execute("DROP TABLE IF EXISTS " + table1_range_name + ";")
            cur.execute("DROP TABLE IF EXISTS " + table2_range_name + ";")
        
            if i == 0:
                cur.execute("CREATE TABLE " + table1_range_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(lowerValue) + ") AND (" + Table1JoinColumn + " <= " + str(upperValue) + ");")
                cur.execute("CREATE TABLE " + table2_range_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(lowerValue) + ") AND (" + Table2JoinColumn + " <= " + str(upperValue) + ");")
                
            else:
                cur.execute("CREATE TABLE " + table1_range_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(lowerValue) + ") AND (" + Table1JoinColumn + " <= " + str(upperValue) + ");")
                cur.execute("CREATE TABLE " + table2_range_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(lowerValue) + ") AND (" + Table2JoinColumn + " <= " + str(upperValue) + ");")
            
            
            
            output_range_table_name = "output_table_temp" + str(i)
            column1Name = InputTable1 + "." + records1[0][0]
            cur.execute("DROP TABLE IF EXISTS " + output_range_table_name + "")
            cur.execute("CREATE TABLE " + output_range_table_name + " (\""+column1Name+"\" "+records2[0][1]+")")

            for j in range(1, len(records1)):
                columnName = InputTable1 + "." + records1[j][0]
                cur.execute("ALTER TABLE " + output_range_table_name + " ADD COLUMN \"" + columnName + "\" " + records1[j][1] + ";")

            for j in range(len(records2)):
                columnName = InputTable2 + "." + records2[j][0]
                cur.execute("ALTER TABLE " + output_range_table_name + " ADD COLUMN \"" + columnName + "\"" +" "+ records2[j][1] + ";")
 
    


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


