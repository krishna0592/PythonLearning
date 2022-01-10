import calendar
from datetime import datetime, timedelta

class CalendarUtil():

    def __init__(self):
        pass

    def getAllMonthOfYear(self):
        print(calendar.calendar(2020))


    def getWeekHeader(self):
        print(calendar.weekheader(3))


    def getMonthAsList(self):
        print(calendar.monthcalendar(2020,6))


    def getDayOfWeekAsInt(self):
        print(calendar.weekday(2020,6,10))


    def getCurrentDate(self, format):
        print(datetime.today().strftime(format)) #cal.getCurrentDate('%Y-%m-%d %H:%M:%S')


    def stringToDateObject(self, date, format):
        print(datetime.strptime(date, format).date())


    def convertDateFormat(self, date, ipformat, opformat):
        print(datetime.strptime(date, ipformat).strftime(opformat))


    def getDateDiff(self, startDate, endDate):
        ed_date = datetime.strptime(endDate,'%Y-%m-%d')
        st_date = datetime.strptime(startDate,'%Y-%m-%d')
        diff = ed_date - st_date
        print(diff.days)


    def getTimeDiffInSec(self, startTime, endTime):
        ed_time = datetime.strptime(endTime,'%Y-%m-%d %H:%M:%S')
        st_time = datetime.strptime(startTime,'%Y-%m-%d %H:%M:%S')
        diff = ed_time - st_time
        print(diff.total_seconds())         # time diff in seconds
        print(diff.total_seconds() / 60)    # time diff in minutes


    def dateAddition(self, date, format, day):
        print(datetime.strptime(date, format) + timedelta(days=day))


    def dateSubtraction(self, date, format, day):
        print(datetime.strptime(date, format) - timedelta(days=day))


    def addHours(self, date, format, hour):
        print(datetime.strptime(date, format) + timedelta(hours=hour))


    def addMinutes(self, date, format, min):
        print(datetime.strptime(date, format) + timedelta(minutes=min))


    def dateExtract(self, date, format):
        date = datetime.strptime(date, format)

        print(repr('# day related extraction'))
        print("  day --> " + str(date.day))
        print("  day as string full form --> " + str(date.strftime('%A')))
        print("  day as string short form --> " + str(date.strftime('%a')))
        print("  day of week --> " + str(date.isoweekday()))
        print("  day of year --> " + str(date.timetuple().tm_yday))
        print()
        print(repr('# month related extraction'))
        print("  month --> " + str(date.month))
        print("  month as string full form --> " + str(date.strftime('%B')))
        print("  month as string short form --> " + str(date.strftime('%b')))
        print()
        print(repr('# year related extraction'))
        print("  year --> " + str(date.year))
        print("  week of year --> " + str(date.isocalendar().week))
        print("  week of month --> " + str(date.isocalendar()[1] - date.replace(day=1).isocalendar()[1] + 1))
        print()
        print(repr('# time realted extraction'))
        print("  hour --> " + str(date.hour))
        print("  minute --> " + str(date.minute))
        print("  seconds --> " + str(date.second))
        print()
        print("calendar --> " + str(date.isocalendar()))



# if __name__ == '__main__':
#     cal = CalendarUtil()
#     cal.dateExtract('2020-06-8 12:04:00', '%Y-%m-%d %H:%M:%S')
#
