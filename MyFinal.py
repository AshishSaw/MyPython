#Reading Data from Azure Blob Storge, Script is written in Python Scriptng


def readfile(filepath, filename, filetype):
    storage_account_name = 'ashishsawblob'
    storage_account_access_key = 'mhW4Qbdtp+gsto/lFTypM0eqJTy5u/OtVuzVPQ23+fzkSPHN3KARvw+tSATvIETvAXmHhLobDoYbHVzMny3zFA=='
    spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)

  if fileType == "csv":
      return spark.read.csv(filepath + filename, header = "true", inferSchema = "true")
  elif filetype == "parquet":
      return spark.read.format("Parquet").load(filepath + filename)

#Write Parquet file to Raw Zone
def writefile(dataframe, filepath, filename, filetype):
    if filetype == "parquet":
        return dataFrame.write.mode("append").parquet(filepath + filename)


# DQ Check Start
# 1. Check Code should not be numm or blank, if yes change it to UN

# from pyspark.sql.functions import *

def dqqualitycheck(dataframe):
    return dataframe.fillna({'Code': '-1', 'SiteCode': 'UnKnown', 'OEECode': 'BADDATA', 'OEECodeECE': 'BADDATA'})

#Prepare BAD Data and Good Data Parquet File

def getgooddata(dataframe):
    dataframe = dataframe.filter(("OEECode != 'BADDATA'") )
    dataframe = dataframe.filter(("OEECodeECE != 'BADDATA'"))
    return dataframe.distinct()

# Get Bad Data from Data Frame

def getbaddata(rawdataframe, gooddataframe):
    rawdataframe = rawdataframe.subtract(gooddataframe)
    return rawdataframe.distinct()

#Function to Create/Append Data into Delta table
#Will confirm with Deepa, Where will be location of Delta table,it will be ADLS Mount or defult

def createorappendprocessedfileintodeltatable(dataframe, filepath):
    dataframe.write.format("delta").mode("append").saveAsTable(filepath)

