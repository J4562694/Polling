from app.module.modbus import ReadModbusPoints
from app.module.sqlQuery.polling import PollingQuery
from app.module.sqlQuery.conn import CMS_POOL
from app.module.jsonEditor import updateJsonFile
from app.module.readFloat import readFloat
from app.module.timeout import timeout
import threading
import datetime

stopTask = False

mysqlSearchData = {}

skipDevices = []

deviceIdList = []

#  將當前mysql資料抓出來
def mysqlData():
    
    devices = PollingQuery.deviceDataSearch()

    dataBlock = PollingQuery.dataBlocksSearch()

    Point = PollingQuery.pointsDataSearch()
    
    # 製作device的結構
    for device in devices:
        mysqlSearchData[device['ID']] = {
            'IP': device['IP'],
            'DataBlocks':[]
        }
        # for _, deviceContent in mysqlSearchData.items():
        #     for dataBlock in deviceContent['DataBlocks']:
        #         print(mysqlSearchData)
        # keyExists = any(dataBlock['DataBlocks'] in d.values() for d in device['ID'])


    # 製作dataBlock的結構
    for dataBlocks in dataBlock:
        if dataBlocks['DeviceID'] in mysqlSearchData:
            mysqlSearchData[dataBlocks['DeviceID']]['DataBlocks'].append({
                'DeviceID': dataBlocks['DeviceID'],
                'Slave': dataBlocks['Slave'],
                'DataBlockID': dataBlocks['DataBlockID'],
                'PointTypeID': dataBlocks['PointTypeID'],
                'StartPoint': dataBlocks['StartPoint'],
                'endPoint': dataBlocks['endPoint'],
                'Points': []
            })

    # 製作Points的結構
    for _, deviceContent in mysqlSearchData.items():
        for dataBlock in deviceContent['DataBlocks']:
            for Points in Point:
                if dataBlock['DataBlockID'] == Points['DataBlockID']:
                    keyExists = any(Points['ID'] in d.values() for d in dataBlock['Points'])
                    if not keyExists:
                        dataBlock['Points'].append({
                            'ID': Points['ID'],
                            'DataBlockID': Points['DataBlockID'],
                            'DecimalValue': Points['DecimalValue'],
                            'PointAddr': Points['PointAddr'],
                            'Val': Points['Val'],
                            'AlarmCheck': Points['AlarmCheck']
                        })
            
    # updateJsonFile("./mysqlSearchData", mysqlSearchData)

# 將PLC資料讀出來
def modbusThread(deviceContent):

    global stopTask
            
    while not stopTask:
        print(deviceContent['IP'], "程式運行中", datetime.datetime.now())
        dataBlocks = deviceContent['DataBlocks']

        # 使用DataBlocks的資料去讀取modbus
        for dataBlockContent in dataBlocks:
            points = dataBlockContent['Points']
            modbus = ReadModbusPoints(
                plcIp=deviceContent['IP'],
                slave=dataBlockContent['Slave'],
                functionCode=dataBlockContent['PointTypeID'],
                startAddr=dataBlockContent['StartPoint'],
                endAddr=dataBlockContent['endPoint'],
                timeout=1
            )

            if modbus.plcIp in skipDevices:
                continue

            try:
                plcData = modbus.getData()
                # 讀出DataBlocks跟Points區間的資料
                for pointsContent in points:
                    pointID: str = pointsContent['ID']
                    DecimaValue = pointsContent['DecimalValue']  #縮放值
                    PointAddr: str = pointsContent['PointAddr']
                    oldPointVal = pointsContent['Val']
                    AlarmCheck = pointsContent['AlarmCheck']

                    newValue: float = 0
                
                    # 判斷是否為32位元
                    if ',' in PointAddr:
                        floatPoints = PointAddr.split(',')
                        firstPoint = int(floatPoints[0])
                        secondPoint = int(floatPoints[1])
                        newValue = round(readFloat((plcData[firstPoint], plcData[secondPoint])), 3)

                    else:
                        PointAddr = int(PointAddr)
                        newValue = plcData[PointAddr]

                    # 數值縮放以及確認是否為DIO
                    if modbus.functionCode != 0 and modbus.functionCode != 1:
                        newValue = round(newValue * DecimaValue, 3)
                    else:
                        newValue = int(newValue)

                    newValue = str(newValue)

                    # 確認是否更新點位
                    if oldPointVal != newValue:
                        pointsContent['Val'] = newValue
                        PollingQuery.pointDataUpdate(pointID, newValue)
                        print(f"PointID: {pointID}, New Value: {newValue} update successful!")

            except TimeoutError:
                skipDevices.append(deviceContent['IP'])

            except Exception as e:
                print(f"Error: {e}")

        timeout(1.5, 1)
                
            
def main():

    global stopTask

    mysqlData()

    threads = [] 

    for _, deviceContent in mysqlSearchData.items():
        threads.append(threading.Thread(target=modbusThread, args=(deviceContent, )))


    for thread in threads:
        thread.start()

    while True:
        try:
            timeout(2, 1)

        except KeyboardInterrupt:
            stopTask = True

            for thread in threads:
                thread.join()

            quit()

        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()