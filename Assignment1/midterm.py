import psycopg2

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def getOpenConnection(user='postgres', password='1234', dbname='midterm'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, venuetablename, venuefilepath, ratingsfilepath, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute('DROP TABLE IF EXISTS FRIEND')
    	cursor.execute('DROP TABLE IF EXISTS VISIT')
        drop_table_query = 'DROP TABLE IF EXISTS ' + ratingstablename
        cursor.execute(drop_table_query)
        
        create_table_query = '''CREATE TABLE ''' + ratingstablename + '''  
                    (PersonID VARCHAR(5),
                    PersonName VARCHAR(100),
                    PersonAge INT,
                    PRIMARY KEY (PersonID))'''
                    
        cursor.execute(create_table_query) 
        openconnection.commit()
        drop_table_query = 'DROP TABLE IF EXISTS ' + venuetablename
        cursor.execute(drop_table_query)
        create_table_query = '''CREATE TABLE ''' + venuetablename + '''  
                    (VenueID VARCHAR(5),
                    VenueName VARCHAR(100),
                    PRIMARY KEY (VenueID))'''
                    
        cursor.execute(create_table_query) 
        openconnection.commit()
        cursor = openconnection.cursor()
        f = open(ratingsfilepath, 'r')
        f1 = open(venuefilepath, 'r')
        i = 0
        for line in f:
        # Split on any whitespace (including tab characters)
            row = line.split(':')
    #    print (row)     
        # Convert strings to numeric values:
            #row[0] = int(row[0])
            #row[2] = float(row[2])
            row[4] = float(row[4])
    
            
            insert_query = ''' INSERT INTO PERSON (PersonID, PersonName, PersonAge)
                                VALUES (%s, %s, %s)'''
            values = (row[0], row[2], row[4])
            
            cursor.execute(insert_query, values)
            i = i+1
            if i%10000 == 0:
                print (i, " rows inserted")
            
            openconnection.commit()
    #        print (rowcount, "Record inserted successfully into Ratings table")
    	i = 0
    	for line in f1:
        # Split on any whitespace (including tab characters)
            row = line.split(':')
    #    print (row)     
        # Convert strings to numeric values:
            #row[0] = int(row[0])
            #row[2] = float(row[2])
            #row[4] = float(row[4])
    
            
            insert_query = ''' INSERT INTO VENUE (VenueID, VenueName)
                                VALUES (%s, %s)'''
            values = (row[0], row[2])
            
            cursor.execute(insert_query, values)
            i = i+1
            if i%10000 == 0:
                print (i, " rows inserted")
            
            openconnection.commit()
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()


def createrelations(friendfilepath, visitfilepath, openconnection):
    try:
    	cursor = openconnection.cursor()
    	cursor.execute('DROP TABLE IF EXISTS FRIEND')
    	cursor.execute('DROP TABLE IF EXISTS VISIT')
    	create_table_query = '''CREATE TABLE FRIEND (Person1ID VARCHAR(5), Person2ID VARCHAR(5), PRIMARY KEY (Person1ID, Person2ID), FOREIGN KEY (Person1ID) REFERENCES PERSON (PersonID), FOREIGN KEY (Person2ID) REFERENCES PERSON (PersonID))'''
    	cursor.execute(create_table_query)
    	i = 0
    	f = open(friendfilepath, 'r')
    	for line in f:
    		row = line.split(':')
    		insert_query = ''' INSERT INTO FRIEND (Person1ID, Person2ID) VALUES (%s, %s)'''
    		values = (row[2], row[4])
    		cursor.execute(insert_query, values)
    		#values = (row[4], row[2])
    		#cursor.execute(insert_query, values)
    		i = i+1
    		if i%10000 == 0:
    			print (i, " rows inserted")
    		
    		openconnection.commit()
    	
    	create_table_query = '''CREATE TABLE VISIT (PersonID VARCHAR(5), VenueID VARCHAR(5), PRIMARY KEY (PersonID, VenueID), FOREIGN KEY (PersonID) REFERENCES PERSON (PersonID), FOREIGN KEY (VenueID) REFERENCES VENUE (VenueID))'''
    	cursor.execute(create_table_query)
    	i = 0
    	f1 = open(visitfilepath, 'r')
    	
    	for line in f1:
    		row = line.split(':')
    		insert_query = ''' INSERT INTO VISIT (PersonID, VenueID) VALUES (%s, %s)'''
    		row[0] = float(row[0])
    		values = (row[2], row[4])
    		
    		cursor.execute(insert_query, values)
    		i = i+1
    		if i%10000 == 0:
    			print (i, " rows inserted")
    		openconnection.commit()
    except Exception as e:
    	if openconnection:
    		openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()
def rangePartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cursor = openconnection.cursor()
        
        if numberofpartitions == 0:
            cursor.close()
            return
        else:
            ratingRange = 5.0/numberofpartitions
        partition=0
        nextRating = 0
        
        while nextRating<5.0:
            increment = nextRating + ratingRange
            partition_name = RANGE_TABLE_PREFIX + str(partition)
            if nextRating == 0:
                cursor.execute('DROP TABLE IF EXISTS '+partition_name)
                cursor.execute('''CREATE TABLE '''+partition_name+ ''' AS 
                               SELECT * FROM '''+ratingstablename+''' WHERE 
                               Rating>='''+str(nextRating)+ ''' 
                               AND Rating<='''+str(increment)+';')
                partition=partition+1
                nextRating = nextRating + ratingRange
            else:
                cursor.execute("DROP TABLE IF EXISTS "+partition_name)
                cursor.execute('''CREATE TABLE '''+partition_name+ ''' AS 
                               SELECT * FROM '''+ratingstablename+''' WHERE 
                               Rating>'''+str(nextRating)+ ''' 
                               AND Rating<='''+str(increment)+';')
                partition=partition+1 
                nextRating = nextRating + ratingRange
            
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()
                        


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    try:
        cursor = openconnection.cursor()
        partition = 0
        if numberofpartitions == 0:
            cursor.close()
            return
        cursor.execute("SELECT * FROM RATINGS;")
        records = cursor.fetchall()
        
        while partition < numberofpartitions:
            partition_name = RROBIN_TABLE_PREFIX + str(partition)
            cursor.execute('DROP TABLE IF EXISTS '+partition_name)
            create_table_query = '''CREATE TABLE ''' + partition_name + '''  
                    (UserID INT,
                    MovieID INT,
                    Rating REAL)'''
                    
            cursor.execute(create_table_query) 
            partition = partition + 1
            
            
        partition = 0
        partition_name = ''
        for row in records:
            if partition == 0:
                partition_name = RROBIN_TABLE_PREFIX + str(partition)
                values = row
                insert_query = "INSERT INTO " +str(partition_name) + " VALUES (%s, %s, %s)"
                cursor.execute(insert_query, values)
    
            if partition < numberofpartitions and partition != 0:
                partition_name = RROBIN_TABLE_PREFIX + str(partition)
                values = row
                insert_query = "INSERT INTO " +str(partition_name) + " VALUES (%s, %s, %s)"
                cursor.execute(insert_query, values)
    
            partition = partition +1
            if partition == numberofpartitions:
                partition = 0
            partition_name = ''
        
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()    


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        cursor = openconnection.cursor()
        
        values = (userid, itemid, rating)
        
        cursor.execute("SELECT COUNT(*) FROM RATINGS;")
        rows = cursor.fetchone()[0]
    
        cursor.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
                RROBIN_TABLE_PREFIX))
        rRangePart = cursor.fetchone()[0]
        if rRangePart == 0:
            values = (userid, itemid, rating)
            insert_query = ''' INSERT INTO RATINGS (UserID, MovieID, Rating)
                                VALUES (%s, %s, %s)'''
            cursor.execute(insert_query, values)
            cursor.close()
            return
        partition = rows%rRangePart
        
        
        partition_name = RROBIN_TABLE_PREFIX + str(partition)
        insert_query = "INSERT INTO " +str(partition_name) + " VALUES (%s, %s, %s)"
        cursor.execute(insert_query, values)
    
        insert_query = ''' INSERT INTO RATINGS (UserID, MovieID, Rating)
                                VALUES (%s, %s, %s)'''
        cursor.execute(insert_query, values)
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    try:
        values = (userid, itemid, rating)
        insert_query = ''' INSERT INTO RATINGS (UserID, MovieID, Rating)
                                VALUES (%s, %s, %s)'''
        cursor = openconnection.cursor()
        cursor.execute(insert_query, values)
        cursor.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format(
                RANGE_TABLE_PREFIX))
        RangePart = cursor.fetchone()[0]
        if RangePart == 0:
            cursor.close()
            return
        range2 = 5.0 / RangePart
        
    
        Lower = 0
        partitionnumber = 0
        Upper = range2
        
        while Lower<5.0:
            if Lower == 0:
                if rating >= Lower and rating <= Upper:
                    break
                partitionnumber = partitionnumber + 1
                Lower = Lower + range2
                Upper = Upper + range2
            else: 
                if rating > Lower and rating <= Upper:
                    break
                partitionnumber = partitionnumber + 1
                Lower = Lower + range2
                Upper = Upper + range2
                
                
        
        cursor.execute("INSERT INTO range_part"+str(partitionnumber)+" (UserID,MovieID,Rating) VALUES (%s, %s, %s)", values)
    
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
            
    finally:
        if cursor:
            cursor.close()

def createDB(dbname='midterm'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
if __name__ == '__main__':
    try:

        createDB("midterm")

        with getOpenConnection() as con:
            
            # Here is where I will start calling your functions to test them. For example,

            loadRatings('PERSON', 'VENUE', 'venue.txt', 'person.txt', con)
            createrelations('friend.txt', 'visit.txt', con)
            #rangePartition(ratingstablename = 'Ratings', numberofpartitions = 25, openconnection = con)
            

            #rangeInsert(ratingstablename = 'Ratings', userid = 2, itemid = 2, rating = 4, openconnection = con)
            #rangeInsert(ratingstablename = 'Ratings', userid = 3, itemid = 3, rating = 3, openconnection = con)
            #roundRobinPartition(ratingstablename = 'Ratings', numberofpartitions = 25, openconnection = con)
            #roundRobinInsert(ratingstablename = 'Ratings', userid = 1, itemid = 2, rating = 5, openconnection = con)
            

                
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
#            deleteTables('ALL', con)

    except Exception as detail:
        print ('OOPS! This is the error ==> ', detail)
        
