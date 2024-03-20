import logging

import psycopg

# CREATE INDEX idx_data_search ON tdata USING gin(to_tsvector('russian', data));
# Это скрипт создания индекса, создавать, когда база забьется нужными данными
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
        data = data.replace("\"\'", "\"")

        try:
            self.cursor.execute(self.insertQuery, (data, metadata))
            self.connection.commit()
        except Exception as e:
            logging.warning(f"Ошибка при добавлении: {data} и {metadata}")

    def getRecord(self, requestGetRecord, isFast=False):
        dataRequest = list(requestGetRecord.values())
        stringRequest = ""

        if isFast:
            stringRequest = f"SELECT id, data, metadata FROM tdata WHERE to_tsvector('russian', data) @@ to_tsquery('russian', '{" | ".join(dataRequest)}')"
        else:
            stringRequest = f"SELECT id, data, metadata FROM tdata WHERE data LIKE {" AND data LIKE ".join([f"'%{item}%'" for item in dataRequest])}"

        result = self.cursor.execute(stringRequest).fetchall()

        resultList = []
        for i in range(len(result)):
            resultObject = dict()

            id = str(result[i][0])
            data = str(result[i][1]).split(" ### ")
            metadata = str(result[i][2]).split(" ### ")

            resultObject["id"] = id
            for j in range(len(data)):
                resultObject[f"{metadata[j]}"] = data[j]

            resultList.append(resultObject)

        return resultList

    def updateRecord(self, requestUpdateRecord):
        idElement = requestUpdateRecord["id"]
        requestUpdateRecord.pop("id")

        newData = self.SEPARATOR.join((str(x) for x in list(requestUpdateRecord.values())))
        newData = newData.replace("\"\'", "\"")

        stringRequest = f"UPDATE tdata SET data = '{newData}' WHERE id = {idElement}"

        try:
            self.cursor.execute(stringRequest)
            self.connection.commit()
        except Exception as e:
            logging.warning(f"Ошибка при обновлении: {newData}")

    def clear_all_record(self):
        self.cursor.execute("DELETE FROM tdata;")


