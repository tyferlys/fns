import logging

import psycopg

class DbRequests:
    def __init__(self):
        self.DBNAME = "reestrs"
        self.USER = "postgres"
        self.PASSWORD = "1234"
        self.HOST = "localhost"
        self.PORT = "5432"
        self.SEPARATOR = " ### "

        connectionString = f"dbname={self.DBNAME} user={self.USER} password={self.PASSWORD} host={self.HOST} port={self.PORT}"
        self.connection = psycopg.connect(connectionString)
        self.cursor = self.connection.cursor()

        self.insertQuery = "INSERT INTO tdata (data, metadata) VALUES (%s, %s);"

    def createRecord(self, objectData):
        if objectData is None:
            raise Exception("Object empty")

        metadata = self.SEPARATOR.join(list(objectData.keys()))
        data = self.SEPARATOR.join((str(x) for x in list(objectData.values())))

        self.cursor.execute(self.insertQuery, (data, metadata))
        self.connection.commit()

    def getRecord(self, requestGetRecord):
        stringRequest = "SELECT data, metadata FROM tdata WHERE"

        dataRequest = list(requestGetRecord.values())
        for i in range(len(dataRequest)):
            if dataRequest[i] is None:
                dataRequest[i] = ""
            stringRequest += f" data LIKE '%{dataRequest[i]}%'" if i == 0 else f" AND data LIKE '%{dataRequest[i]}%'"

        result = self.cursor.execute(stringRequest).fetchall()

        resultList = []
        for i in range(len(result)):
            resultObject = dict()

            data = str(result[i][0]).split(" ### ")
            metadata = str(result[i][1]).split(" ### ")

            for j in range(len(data)):
                resultObject[f"{metadata[j]}"] = data[j]

            resultList.append(resultObject)

        return resultList

    def clear_all_record(self):
        self.cursor.execute("DELETE FROM tdata;")

