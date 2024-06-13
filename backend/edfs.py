import sys
from textwrap import indent
import requests
import json
import pandas as pd
import mysql.connector
from pymongo import MongoClient

BASE_URL = "https://dsci-project-79-default-rtdb.firebaseio.com/"
NAME_NODE = BASE_URL + "Namenode"
DATA_NODE = BASE_URL + "Datanode"

def Firebase(command):

    def put(command):
        command = command.split(" ")[1:]
        if len(command) < 3:
            print("Enter correct arguments")
            exit(1)

        filename = command[0]
        location = command[1]
        no_of_partations = int(command[2])

        only_filename = filename.split('.')[0]

        url = NAME_NODE + location + "/" + only_filename

        # Deleting previous entires if any
        exisiting_file = requests.get(url + '.json')
        exisiting_file = json.loads(exisiting_file.text)
        if exisiting_file:
            for x,y in exisiting_file.items():
                response = requests.delete(y + '.json')

     
        # Populating data in Namenode
        obj = {}
        for i in range(1,no_of_partations+1):
            obj["p"+str(i)] = DATA_NODE + "/" + "p" + str(i)  + "/" + only_filename
        final_obj = json.dumps(obj)
        response = requests.put(url+".json",final_obj)
        
        # Inserting data into partitions
        df = pd.read_csv(filename)
      
        final_data = {}
        for i in range(1, no_of_partations+1):
            final_data["p"+str(i)] = []
        
        for i,row in df.iterrows():
            row = row.to_dict()
            partition_no = int(row['subject_id']) % no_of_partations + 1
            final_data["p" + str(partition_no)].append(row)
   
        for i in range(1,no_of_partations+1):
            url = DATA_NODE + "/" + "p" + str(i)  + "/" + only_filename
            final_obj = json.dumps(final_data["p" + str(i)])
            response = requests.put(url+".json",final_obj)
            print(response.status_code)
            if response.status_code != 200:
                return 'Error in loading data'
        return 'File loaded successfully'

    def mkdir(command):
        path = command.split(" ")[1]
        url = NAME_NODE + path
        # print(url)
        exisiting_file = requests.get(url + '.json')
        print(exisiting_file.text, type(exisiting_file.text), exisiting_file.status_code)
        if exisiting_file.text!="null":
            print("Directory already exists")
            return "Directory already exists"

        response = requests.put(url + '.json', json.dumps({"init": 0}))
        if response.status_code != 200:
            print('Error in executing mkdir. Please try again')
            return 'Error in executing mkdir. Please try again'
        
        return "Directory created"
    
    def ls(command):
        path = command.split(" ")[1]
        url = NAME_NODE + path

        response = requests.get(url + '.json')
        # print(response.status_code)
        if response.status_code!=200:
            print("directory not found")
            return "directory not found"
        contents = json.loads(response.text)
        # ans = []
        if not contents:
            print("directory not found")
            return "directory not found"
        isEmpty = True
        ans = []
        for i in contents:
            if i!='init':
                if 'init' not in contents[i]:
                    print(i+'.csv')
                    ans.append(i+'.csv')
                else:
                    print(i)
                    ans.append(i)
                isEmpty = False
        if isEmpty:
            print("Empty directory")
            return "Empty directory"
        return ans
        # print(ans)

    
    def cat(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention path to a csv file")
            return "Please mention path to a csv file" 
        url = NAME_NODE + ''.join(path.split('.')[:-1])
        response = requests.get(url + '.json')     
        if response.status_code!=200:
            print("File not found")
            return "File not found"
        contents = json.loads(response.text)
        if not contents:
            print("File not found")
            return "File not found"

        for partition in contents:
            partition_contents = requests.get(contents[partition]+'.json')
            partition_contents = json.loads(partition_contents.text)
            return(partition_contents)

    def rm(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention path to a csv file")
            return "Please mention path to a csv file" 
        url = NAME_NODE + ''.join(path.split('.')[:-1])
        # print(url)
        exists = requests.get(url+'.json')
        if exists.text=='null':
            print("File doesn't exist. Enter a valid file name")
            return "File doesn't exist. Enter a valid file name"
        response = requests.delete(url + '.json')
        if response.status_code != 200:
            print('Error in executing rm. Please try again')
            return 'Error in executing rm. Please try again'
        return "File sucessfully deleted"

    def getPartitionLocations(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"
        url = NAME_NODE + ''.join(path.split('.')[:-1])
        # print(url)
        response = requests.get(url + '.json')
        if response.status_code!=200:
            print("file not found")
            return "file not found"
        
        contents = json.loads(response.text)
        if not contents:
            print("Some error occured. Please try again")
            return "Some error occured. Please try again"

        # print(contents, response.status_code)
        print("Partition\t\tLocation")
        for partition in contents:
            print(partition, "\t\t", contents[partition])
            print("\n\n")
        return contents

    def readPartition(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"
        partition_number = command.split(" ")[-1]
        url = NAME_NODE + ''.join(path.split('.')[:-1])
        # print(url)
        response = requests.get(url + '.json') 

        if response.status_code!=200:
            print("file not found")
            return "file not found"
        
        contents = json.loads(response.text)
        # print(contents, partition_number)
        if not contents:
            print("file not found")
            return "file not found"
        for partition in contents:
            if partition[1:]==partition_number:
                partition_contents = requests.get(contents[partition]+'.json')
                partition_contents = json.loads(partition_contents.text)
                print(json.dumps(partition_contents, indent=4))
                return partition_contents

    command_name = command.split(" ")[0]
    # python3 edfs.py 1 "put cars.csv /Users 5"
    if command_name == 'put':
        return put(command)
    # python3 edfs.py 1 "mkdir /purvil/dir4" 
    elif command_name == 'mkdir':
        return mkdir(command)
    # python3 edfs.py 1 "ls /purvil" 
    elif command_name == 'ls':
        return ls(command)
    # python3 edfs.py 1 "rm /purvil/patients.csv" 
    elif command_name == 'rm':
        return rm(command)
    # python3 edfs.py 1 "cat /purvil/patients.csv" 
    elif command_name == 'cat':
        return cat(command)
    # python3 edfs.py 1 "getPartitionLocations /Users/icustays_data.csv"
    elif command_name == 'getPartitionLocations':
        return getPartitionLocations(command)
    # python3 edfs.py 1 "readPartition /purvil/admissions_data.csv 1"
    elif command_name == 'readPartition':
        return readPartition(command)

def MySQL(command):

    def put(command):
        command = command.split(" ")[1:]
        if len(command) < 3:
            print("Enter correct arguments")
            exit(1)
     
        filename = command[0]
        location = command[1]
        no_of_partations = int(command[2])

        only_filename = filename.split('.')[0]

        df = pd.read_csv(filename)
      
        final_data = {}
        for i in range(1, no_of_partations+1):
            final_data["p"+str(i)] = []
        
        for i,row in df.iterrows():
            row = row.to_dict()
            partition_no = int(row['subject_id']) % no_of_partations + 1
            final_data["p" + str(partition_no)].append(row)

        # Check if data already exists, if yes then delete the record
        query = ("DELETE FROM `Datanode` where filename=%s;")
        val = [only_filename]
        cursor.execute(query, val)

        query = ("DELETE FROM `Namenode` where child=%s and path=%s;")
        val = [only_filename, location]
        cursor.execute(query, val)
        cnx.commit()

        for i in range(1, no_of_partations+1):
            p = "p" + str(i)
            data = final_data[p]
            data = json.dumps(data)

            query = ("INSERT INTO `Datanode` (`filename`, `partition`, `data` ) VALUES (%s, %s, %s)")
            val = [only_filename, p, data]
            cursor.execute(query, val)
            cnx.commit()

        query = ("SELECT id FROM `Datanode` WHERE filename=%s")
        val = [only_filename]
        cursor.execute(query, val)
        res = []
        for x in cursor:
            res.append(list(x)[0])
        
        res = json.dumps(res)
 
        # Insert data into Namenode
        path = location[1:].split("/")
        print(path)
        path[:] = [x for x in path if x]
        parent = '/'
        if len(path)>=1:
            parent = path[-1]
        # if len(path) <= 2:
        #     if len(path) == 2:
        #         parent = path[1]
        #     else:
        #         parent = '/'
        # else:
        #     parent = path[-2]
        
        query = ("INSERT INTO `Namenode` (`parent`, `child`, `partition_location`, `path` ) VALUES (%s, %s, %s, %s)")
        val = [parent, only_filename, res, location]
        cursor.execute(query, val)
        cnx.commit()

        return 'File loaded successfully'

    def mkdir(command, cnx):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to mkdir")
            return
        
        path = commands[1]
        dirs = path.split('/')
        # remove empty directories.
        dirs[:] = [x for x in dirs if x]
        if len(dirs)<1:
            print("Invalid arguments to mkdir")
            return
        child = dirs[-1]
        parent = '/'
        if len(dirs)>1:
            parent = dirs[-2]
        absPath = '/' + '/'.join(dirs)
        cursor = cnx.cursor()
        query = ("SELECT path FROM `Namenode` WHERE path=%s LIMIT 1")
        val = [absPath]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        if len(rows)==1:
            print("Directory already exists")
            return
        
        cursor = cnx.cursor()
        query = ("INSERT INTO `Namenode` (`parent`, `child`, `path` ) VALUES (%s, %s, %s)")
        val = [parent, child, absPath]
        cursor.execute(query, val)
        cnx.commit()

        return "Directory created"
            
    def ls(command, cnx):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to mkdir")
            return

        path = commands[1]
        dirs = path.split('/')
        # remove empty directories.
        dirs[:] = [x for x in dirs if x]
        parent = '/'
        if len(dirs)>=1:
            parent = dirs[-1]
        absPath = '/' + '/'.join(dirs)
        cursor = cnx.cursor()
        query = ("SELECT child, partition_location FROM `Namenode` WHERE parent = %s AND path LIKE %s ")
        val = [parent, (absPath+'%')]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        ans = []
        for x in rows:
            x = list(x)
            if x[1]:
                print(x[0]+'.csv')
                ans.append(x[0]+'.csv')
            else:
                print(x[0])
                ans.append(x[0])
        return ans

    def rm(commands,cnx):
        command = commands.split(" ")
        if len(command) != 2:
            print("Invalid arguments to rm")
            return "Invalid arguments to rm"
        
        path = commands.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"

        dirs = path.split('/')

        path = '/'.join(dirs[:-1])
        only_filename = dirs[-1].split('.')[0]
     
        cursor = cnx.cursor()
        query = ("SELECT child FROM `Namenode` WHERE child=%s LIMIT 1")
        val = [only_filename]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        
        if len(rows)!=1:
            print("File does not exist")
            return "File does not exist"

        cursor = cnx.cursor()
        query = ("DELETE FROM `Namenode` where child =%s")
        val = [only_filename]
        cursor.execute(query, val)
        cnx.commit()
        return "File deleted successfully" 

    def cat(command, cnx):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to cat")
            return "Invalid arguments to cat"
        
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"
        
        dirs = path.split('/')

        path = '/'.join(dirs[:-1])
        only_filename = dirs[-1].split('.')[0]

        cursor = cnx.cursor()
        query = ("SELECT partition_location FROM `Namenode` WHERE child = %s AND path = %s ")
        val = [only_filename, path]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        
        if len(rows)!=1:
            print("File does not exist")
            return "File does not exist"
        
        for x in rows:
            query = ("SELECT * FROM `Datanode`")
            cursor.execute(query)
            ans = cursor.fetchall()
            for a in ans:
                return a 

    def getPartitionLocations(command, cnx):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to mkdir")
            return

        path = commands[1]
        dirs = path.split('/')

        path = '/'.join(dirs[:-1])
        only_filename = dirs[-1].split('.')[0]

        print(path,only_filename)
        cursor = cnx.cursor()
        query = ("SELECT partition_location FROM `Namenode` WHERE child = %s AND path = %s ")
        val = [only_filename, path]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        for x in rows:
            # x = list(x[0])
            print(json.loads(x[0]))
            return json.loads(x[0])

    # Connecting to the database
    # cnx = mysql.connector.connect(user='ec2-user', password='qwe123!@#',
    #                           host='127.0.0.1',
    #                           database='DSCI')
    def readPartition(command, cnx):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return
        partition_number = command.split(" ")[-1]
        # print(partition_number)

        # path = commands[1]
        dirs = path.split('/')

        path = '/'.join(dirs[:-1])
        only_filename = dirs[-1].split('.')[0]

        # print(path,only_filename)
        cursor = cnx.cursor()
        query = ("SELECT partition_location FROM `Namenode` WHERE child = %s AND path = %s ")
        val = [only_filename, path]
        cursor.execute(query, val)
        rows = cursor.fetchall()
        for x in rows:
            # x = list(x[0])
            if int(partition_number)>len(json.loads(x[0])):
                print("invalid partition number")
                return
            partition_id = json.loads(x[0])[int(partition_number)-1]
            # print(partition_id)
            query = ("SELECT * FROM `Datanode` WHERE id = %s ")
            val = [partition_id]
            cursor.execute(query, val)
            ans = cursor.fetchall()
            for a in ans:
                print(a)
    # Connecting to the database
    cnx = mysql.connector.connect(host='dsci-project.ceznqfofymma.us-west-1.rds.amazonaws.com', port='3306', user='root', password='root1234', database='DSCI')
    
    # cnx = mysql.connector.connect(user='root', password='Dsci-551',
    #                           host='127.0.0.1',
    #                           database='DSCI_project')
    
    cursor = cnx.cursor()

    command_name = command.split(" ")[0]
    # python3 edfs.py 2 "put cars.csv /Users 5"
    if command_name == 'put':
        return put(command)
    # python3 edfs.py 2 "mkdir /purvil/dir4" 
    elif command_name == 'mkdir':
        return mkdir(command, cnx)
    # python3 edfs.py 2 "ls /purvil" 
    elif command_name == 'ls':
        return ls(command, cnx)
    # python3 edfs.py 2 "getPartitionLocations /Users/test/patients_data.csv"
    elif command_name == 'getPartitionLocations':
        return  getPartitionLocations(command, cnx)
    # python3 edfs.py 2 "readPartition /Users/test/patients_data.csv 2"
    elif command_name == 'readPartition':
        return  readPartition(command, cnx)
    elif command_name == 'rm': 
        return  rm(command, cnx)
    elif command_name == 'cat':
        return  cat(command, cnx)
    cursor.close()
    cnx.close()
    
def MongoDB(command):

    def put(command):
        command = command.split(" ")[1:]
        if len(command) < 3:
            print("Enter correct arguments")
            exit(1)

        filename = command[0]
        location = command[1]
        no_of_partations = int(command[2])

        only_filename = filename.split('.')[0]

        # url = NameNode + location + "/" + only_filename
        print(filename, location, no_of_partations, only_filename)

        # Populating data in Namenode
        obj = {}
        for i in range(1,no_of_partations+1):
            obj["p"+str(i)] = "DataNode/p" + str(i)  + "/" + only_filename

        
        loc = NameNode
        if location!='/':
            for i in location.split('/')[1:]:
                loc = loc[i]
        
        # ADDED BY SANKET
        loc.insert_one({only_filename: 'file'})
        
        loc[only_filename].delete_one({})
        loc[only_filename].insert_one(obj)

        # Inserting data into partitions
        df = pd.read_csv(filename)
      
        final_data = {}
        for i in range(1, no_of_partations+1):
            final_data["p"+str(i)] = []
        
        for i,row in df[:10].iterrows():
            row = row.to_dict()
            partition_no = int(row['subject_id']) % no_of_partations + 1
            final_data["p" + str(partition_no)].append(row)
        # print(final_data)

        for i in range(1,no_of_partations+1):
            # url = DATA_NODE + "/" + "p" + str(i)  + "/" + only_filename
            loc = DataNode["p" + str(i)]
            # print(loc)
            final_obj = final_data["p" + str(i)]
            # print(len(final_data["p" + str(i)]))
            loc.delete_many({ only_filename : { "$exists" : "true" } })
            loc.insert_one({only_filename:final_obj})

        return 'File loaded successfully'
            
    def mkdir(command):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to mkdir")
            return
        
        path = commands[1]
        dirs = path.split('/')
        # remove empty directories.
        dirs[:] = [x for x in dirs if x]
        if len(dirs)<1:
            print("Invalid arguments to mkdir")
            return
        
        loc = NameNode
        for i in range(len(dirs)-1):
            if loc.find({dirs[i] : { "$exists" : "true" }}):
                loc = loc[dirs[i]]
            else:
                print("Invalid path")
                return
        # print(list(loc.find().keys()))
        # for x in loc.find({}):
        #     print(x)
        if loc.find_one({dirs[-1] : { "$exists" : "true" }}):
            print("Directory already exists")
            return
        loc.insert_one({dirs[-1]: 'dir'})
        return "Directory created"
        
    def ls(command):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Invalid arguments to mkdir")
            return

        path = commands[1]
        dirs = path.split('/')
        # remove empty directories.
        dirs[:] = [x for x in dirs if x]
        
        loc = NameNode
        for i in dirs:
            if loc.find({i : { "$exists" : "true" }}):
                loc = loc[i]
            else:
                print("Invalid path")
                return
        ans = []
        for i in loc.find({}, {'_id': 0}):
            for k,v in i.items():
                if v=='file':
                    ans.append(k+'.csv')
                    print(k+'.csv')
                else:
                    ans.append(k)
                    print(k)
        return ans
    
    def getPartitionLocations(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return
        location = ''.join(path.split('.')[:-1])
        loc = NameNode
        for i in location.split('/')[1:]:
            loc = loc[i]
        data = {}
        for i in loc.find():
            print(i)
            for x,y in dict(i).items():
                if x!="_id":
                    data[x] = y
        return data
    
    def readPartition(command):
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return
        partition_number = command.split(" ")[-1]
        print("partition_number", partition_number)

        location = ''.join(path.split('.')[:-1])
        loc = NameNode
        for i in location.split('/')[1:]:
            loc = loc[i]

        for i in loc.find():
            for x in i:
                if x[1:]==partition_number:
                    loc = DataNode
                    for p in i[x].split('/')[1:]:
                        loc = loc[p]
                    print("loc", loc)
                    data = loc.find({})
                    # print(data)
                    ans = {}
                    for i in data:
                        print("--", i)
                        # for x,y in dict(i).items():
                        #     if x!="_id":
                        #         ans[x] = y
                    return ans

    def rm(command):
        commands = command.split(" ")
        if len(commands) != 2:
            print("Enter correct arguments")
            return "Enter correct arguments"
            exit(1)
    
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"
    
        location = ''.join(path.split('.')[:-1])
        loc = NameNode
        only_filename = location.split('/')[-1]
        loc[only_filename].delete_one({})
        return "File Deleted Successfully"

    def cat(command):
        a_ls = []
        path = command.split(" ")[1]
        if '.csv' not in path:
            print("Please mention a path to csv file!")
            return "Please mention a path to csv file!"

        location = ''.join(path.split('.')[:-1])
        loc = NameNode
        for i in location.split('/')[1:]:
            loc = loc[i]

        for i in loc.find():
            for x in i:
                loc = DataNode
            for p in i[x].split('/')[1:]:
                    loc = loc[p]
                    data = loc.find({})
                    for i in data:
                        a_ls.append(i)
                    return a_ls

    client = MongoClient("mongodb+srv://root:root1234@dsci-project.d65u3ld.mongodb.net/?retryWrites=true&w=majority")
    db = client["dsci"]
    NameNode = db["NameNode"]
    DataNode = db["DataNode"]

    command_name = command.split(" ")[0]
    if command_name == 'put':
        return put(command)
     # python3 edfs.py 3 "mkdir /Users" 
    elif command_name == 'mkdir':
        return mkdir(command)
    # python3 edfs.py 3 "ls /Users" 
    elif command_name == 'ls':
        return ls(command)
    # python3 edfs.py 3 "getPartitionLocations /Users/patients_data.csv"
    elif command_name == 'getPartitionLocations':
        return getPartitionLocations(command)
    # python3 edfs.py 3 "readPartition /Users/patients_data.csv 2"
    elif command_name == 'readPartition':
        return readPartition(command)
    elif command_name == 'rm': 
        return rm(command)
    elif command_name == 'cat':
        return cat(command)
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Enter correct arguments")
        exit(1)
    else:
        server = sys.argv[1]
        command = sys.argv[2]

    if server == "1":
        Firebase(command)
    elif server == "2":
        MySQL(command)
    elif server == "3":
        MongoDB(command)