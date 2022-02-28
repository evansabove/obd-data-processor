import logging
import azure.functions as func
import pyodbc
import os

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    raw_lines = [x.decode('UTF-8').rstrip() for x in myblob.readlines()[1:]]
    drive_id = myblob.name.split('/')[1].split('.')[0]
    
    if len(raw_lines) == 0:
        return

    connection = pyodbc.connect(os.environ["DatabaseConnectionString"])
    cursor = connection.cursor()

    cursor.execute(f"INSERT INTO Drive(Id, Imported) VALUES (?, GETUTCDATE())", [drive_id])

    for i in raw_lines:
        line = i.split(',')
        line.append(drive_id)
        cursor.execute(f"INSERT INTO DriveData(SPEED, RPM, COOLANT_TEMP, INTAKE_TEMP, FUEL_LEVEL, ENGINE_LOAD, TIMESTAMP, DriveId) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", line)

    connection.commit()
    cursor.close()