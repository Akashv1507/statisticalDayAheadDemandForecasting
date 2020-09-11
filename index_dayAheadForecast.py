import argparse
from datetime import datetime as dt
from datetime import timedelta
from src.appConfig import getAppConfigDict
from src.derivedDataCreator.dayAheadForecastCreator import createDayAheadForecast

configDict=getAppConfigDict()

endDate = dt.now()
# startDate = endDate - timedelta(days=2)
startDate = endDate


# get start and end dates from command line
parser = argparse.ArgumentParser()
parser.add_argument('--start_date', help="Enter Start date in yyyy-mm-dd format",
                    default=dt.strftime(startDate, '%Y-%m-%d'))
parser.add_argument('--end_date', help="Enter end date in yyyy-mm-dd format",
                    default=dt.strftime(endDate, '%Y-%m-%d'))

                    
args = parser.parse_args()
startDate = dt.strptime(args.start_date, '%Y-%m-%d')
endDate = dt.strptime(args.end_date, '%Y-%m-%d')

startDate = startDate.replace(hour=0, minute=0, second=0, microsecond=0)
endDate = endDate.replace(hour=0, minute=0, second=0, microsecond=0)

print('startDate = {0}, endDate = {1}'.format(dt.strftime(
    startDate, '%Y-%m-%d'), dt.strftime(endDate, '%Y-%m-%d')))

# do adjustment between start and end dates
isRawDataCreationSuccess = createDayAheadForecast(startDate,endDate,configDict)
if isRawDataCreationSuccess:
    print(' day ahead forecast creation success...')
else:
    print(' day ahead forecast creation failure...')