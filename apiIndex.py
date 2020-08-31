from scadaApiFetcher import ScadaApiFetcher
import datetime as dt

tokenUrl: str = 'http://portal.wrldc.in/idserver/connect/token'
apiBaseUrl: str = 'http://portal.wrldc.in/dashboard'
clientId = 'wrldc_electron'
clientSecret = 'wrldc@123'
startDate = dt.datetime.strptime("2020-08-25", '%Y-%m-%d')
endDate = dt.datetime.strptime("2020-08-29", '%Y-%m-%d')

#data not present for 23,24,26,27 aug 2020

fetcher = ScadaApiFetcher(tokenUrl, apiBaseUrl, clientId, clientSecret)

listOfEntity =['WRLDCMP.SCADA1.A0046945','WRLDCMP.SCADA1.A0046948','WRLDCMP.SCADA1.A0046953','WRLDCMP.SCADA1.A0046957','WRLDCMP.SCADA1.A0046962','WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980','WRLDCMP.SCADA1.A0047000']

for entity in listOfEntity:
    resData = fetcher.fetchData(entity, startDate, endDate)
    print(len(resData))
print('execution complete...')