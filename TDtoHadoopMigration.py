#!/usr/bin/env python
#title           :TDtoHadoopMigration.py
#description     :This script will convert the Teradata queries to Hive queries.
#author          :Satish Gampala
#date            :2018-04-13
#version         :1.0
#usage           :python TDtoHadoopMigration.py
#python_version  :3.4.3 
#================================================================================

# Importing the modules needed to run the script.
import re
import sys
import os
import time
import datetime
import contextlib

def getErrorInfo():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    errorString = "FileName : " + fname + ", LineNumber : " + str(exc_tb.tb_lineno) + ", ExceptionType : "+ str(exc_type) +", Exception : "+ str(exc_obj)
    return (errorString)
def getCurrentTime():
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
def getMappingDictionary(filePath):
    with open(filePath,'r') as fileObject:
        try:
            lines = fileObject.readlines()
            mapDictionary = {}
            for i in range(9,len(lines)):
                line = lines[i]
                splitWords = line.split('=')
                if(len(splitWords)==2):
                    mapDictionary[splitWords[0].strip()]=splitWords[1].strip()
            return mapDictionary
        except Exception as e:
            fileObject.write(getCurrentTime() + " :: ERROR :: " +"Caught an exception while getting the Mapping Dictionary : ",getErrorInfo() + '\n\n')
def getTdQueryFilePaths(filePath):
    with open(logFilePath,"a") as logFileObject:
        try:
            with open(filePath,'r') as pathsFileObect:
                filePathsList = [i.strip() for i in pathsFileObect.readlines()]
            return filePathsList
        except Exception as e:
            logFileObject.write(getCurrentTime() + " :: ERROR :: " +"Getting List of query file paths from the given file is FAILED : ",getErrorInfo() + '\n\n')
def convertTDtoHive(mapDictionary,
                    outputFilePath,
                    queryFilePath
                    ):
    with open(logFilePath,"a") as logFileObject:
        try:
            with open(outputFilePath,"w+") as f:
                defaultLines = '''
------Set the Hive tuning parameters-------
-------------------------------------------------

set hive.execution.engine=tez ;
set mapred.compress.map.output=true ;
set mapred.output.compress=true ;
set hive.auto.convert.join=true;
set hive.exec.parallel=true;
set hive.vectorized.execution.enabled=true;

-------------------------------------------------
'''
                with open(queryFilePath, "r") as queryFile:
                    queryFileContent = queryFile.read()
                    pattern = re.compile("CREATE['\s']*VOLATILE['\s']*TABLE[^;]+;")
                    iterator = pattern.finditer(str(queryFileContent))
                    for match in iterator:
                        start,end = match.span()
                        # Excluding Comment lines
                        if not (queryFileContent[start:end].split('\n')[-1].startswith('--')):
                            pattern2 = re.compile("\)['\s\n\t']*"+'WITH["\s"]*DATA["\s"]*UNIQUE["\s"]*PRIMARY["\s"]*INDEX(.*)["\s"]*ON["\s"]*COMMIT["\s"]*PRESERVE["\s"]*ROWS')
                            iterator2 = pattern2.finditer(queryFileContent[start:end])
                            for match2 in iterator2:
                                start2,end2 = match2.span()
                                queryFileContent = queryFileContent[:(start+start2)] + "STORED AS ORC" + queryFileContent[(start+end2):]
                    with open("tempContent.txt","w+") as tempFileObj:
                        tempFileObj.write(queryFileContent)
                    queryLines = []
                    with open("tempContent.txt","r") as tempFileObj2:
                        queryLines = tempFileObj2.readlines()
                    if os.path.exists("tempContent.txt"):
                        os.remove("tempContent.txt")
                    # with contextlib.suppress(FileNotFoundError):
                    #     os.remove("tempContent.txt")
                    printHeaders = False
                    if not (queryLines[0].startswith("-- BEGIN HEADER")):
                        f.write(defaultLines)
                    for queryLine in queryLines:
                        if not queryLine.startswith("--"):
                            for tdString,hiveString in mapDictionary.items():
                                if(tdString == "SEL"):
                                    queryLine = re.sub(r'[\s\t]+SEL[\n\s\t]+', hiveString, queryLine)
                                elif(tdString == "WITH DATA UNIQUE PRIMARY INDEX() ON COMMIT PRESERVE ROWS"):
                                    queryLine = re.sub(r'\)["\s\n\t"]*WITH["\s"]*DATA["\s"]*UNIQUE["\s"]*PRIMARY["\s"]*INDEX(.*)["\s"]*ON["\s"]*COMMIT["\s"]*PRESERVE["\s"]*ROWS', hiveString, queryLine)
                                else:
                                    #queryLine = re.sub(r'[\s\t]+'+re.escape(tdString)+r'[\n\s\t]+', hiveString, queryLine)
                                    queryLine = queryLine.replace(tdString,hiveString)
                            f.write(queryLine)
                        # Printing headers and Default lines
                        elif queryLine.startswith("--"):
                            if queryLine.startswith("-- BEGIN HEADER"):
                                printHeaders = True
                            elif queryLine.startswith("-- END HEADER"):
                                f.write(queryLine + "\n")
                                f.write(defaultLines)
                                printHeaders = False
                        if(printHeaders):
                            f.write(queryLine)
                    logFileObject.write(getCurrentTime() + " :: " + queryFilePath.split("/")[-1] + " :: SUCCESS"+ '\n\n')
            
        except Exception as e:
            logFileObject.write(getCurrentTime() + " :: " + queryFilePath.split("/")[-1] + " :: FAILED : "+ getErrorInfo() + '\n\n')

def doTDtoHiveMigration(mappingFilePath = "/home/dwauser/TdToHiveMigration/lookup_tbl_hive.txt",
                        listOfFilesPath = "/home/dwauser/TdToHiveMigration/filePaths.txt",
                        outputLogFilePath = "/home/dwauser/TdToHiveMigration/MigrationOutput.log"
                        ):
    qeriesDestinationPath = "/".join(outputLogFilePath.split('/')[:-1]) + "/HiveQueries"
    if not os.path.isdir(qeriesDestinationPath):
         os.mkdir(qeriesDestinationPath)
    global logFilePath
    logFilePath = outputLogFilePath
    try:
        with open(logFilePath,"w+") as logFileObject:
            try:
                mapDictionary = getMappingDictionary(mappingFilePath)
                if (len(mapDictionary) > 0):
                    queryFilePaths = getTdQueryFilePaths(filePath = listOfFilesPath)
                    if len(queryFilePaths) > 0:
                        for filepath in queryFilePaths:
                            outputFilePath = qeriesDestinationPath + '/' + filepath.split("/")[-1].split(".")[0] + ".hql"
                            convertTDtoHive(mapDictionary = mapDictionary, outputFilePath=outputFilePath, queryFilePath=filepath)
                    else:
                        logFileObject.write(getCurrentTime() + " :: ERROR :: " +"Conversion FAILED :: Mininum one query file needed to proceed with the conversion. No Query file found to convert from TD to Hive" + '\n\n')
                else :
                    logFileObject.write(getCurrentTime() + " :: ERROR :: " +"TD to Hadoop Mapping strings NOT FOUND." + '\n\n')
            except Exception as e_sub:
                logFileObject.write(getCurrentTime() + " :: ERROR :: " + getErrorInfo() + '\n\n')
    except IOError:
        print (getCurrentTime() + " :: ERROR :: " +"Caught an exception while creating a Log file : " + logFilePath +  '\n\n')
        sys.exit()
    except Exception as e:
        logFileObject.write(getCurrentTime() + " :: ERROR :: " + getErrorInfo() + '\n\n')


if __name__ == "__main__":
    doTDtoHiveMigration()
