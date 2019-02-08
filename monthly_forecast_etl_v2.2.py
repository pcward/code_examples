## Created by:  Chris Ward
##				chris.ward@clearlink.com
##
## Last edit:	2013-03-14
##
## Version:		2.2

## Imports forecast data to the Forecasts SQL table from a CSV file

## CSV file format: 
##		columns: yr, wk, brand, channel, forecast (must be WEEKLY values)

## To invoke the script, open a command line and issue the following:
##		python monthly_etl_forecast.py
##
## 		Ex: python monthly_etl_forecast.py


import glob
import string
import os
import MySQLdb
import sys
import time
import argparse

from sys import argv
from datetime import date, datetime
from warnings import filterwarnings

filterwarnings('ignore', category = MySQLdb.Warning)


def dedup_list(seq):  
   collection = set(seq)
   return list(collection)

def trunc(f, n):
    #Truncates/pads a float f to n decimal places without rounding
    return ('%.*f' % (n + 1, f))[:-1]


parser = argparse.ArgumentParser(description='Imports forecast data to the Forecasts SQL table from a CSV file.')
parser.add_argument('--version', action='version', version='%(prog)s v. 2.2')
args = parser.parse_args()

db_host = '127.0.0.1'
db_user = 'user'
db_pwd = 'pwd'
db_db = 'db'

weekend_proportion = 0.0833
weekday_proportion = 0.166


files = glob.glob('*orecast*.csv')

sql = MySQLdb.connect(host=db_host, user=db_user, passwd=db_pwd, db=db_db)
cursor = sql.cursor()


if len(files) > 0:

	print "Parsing new data. . ."

	new_forecast = []

	for f in files:
		fp = open(f,'r')

		line_num = 1

		for line in iter(fp):

			if line_num == 1:
				line_num += 1
				continue

			else:
				ln = [x.strip() for x in line.split(',')]

				if not ln[4]:
					ln[4] = '0'

				ln[0] = int(ln[0])
				ln[1] = int(ln[1])
				ln[2] = ln[2].strip('"')
				ln[3] = ln[3].strip('"')				
				ln[4] = float(trunc(float(ln[4].strip()),2))

				new_forecast.append(ln)
				line_num += 1

		fp.close()
		os.unlink(f)

	daily_qopps = []

	print "Calculating daily forecast values. . ."
	print "Inserting forecast in database. . ."

	today = datetime.today().strftime("%Y-%m-%d")

	for week in new_forecast:
		weekly_forecast = week[4]

		for i in range(7):
			if i == 0 or i == 6:
				week[4] = weekend_proportion * weekly_forecast
			elif i < 6 and i > 0:
				week[4] = weekday_proportion * weekly_forecast

			dt_str = '%d %d %d' % (week[0], week[1], i)
			dt = datetime.strptime(dt_str, "%Y %W %w").strftime("%Y-%m-%d")		# convert yr week day-of-week to date yyyy-mm-dd

			daily = []

			daily.extend([dt, week[2], week[3], week[4], today])
			oline = '\'' + '\',\''.join(map(str, daily)) + '\''

			cursor.execute("insert into db (`Date`,`Brand`,`Channel`,`Forecast`, `date_updated`) values (%s);" % oline)

	
# Drop the old stuff from marketing_performance
print "Dropping old data from rdb. . ."

cursor.execute("truncate db1;")


# Inject the fresh forecast
print "Injecting new data into db. . ."
cursor.execute("""insert into db1 (`Date`, Brand, Channel,`Forecast`) 
	SELECT f.`Date`, f.Brand , f.Channel , f.`Forecast` FROM db as f
	INNER JOIN (SELECT `Date`, Brand, Channel, MAX(date_updated) as date_updated
		FROM db
		GROUP BY `Date`, Brand	, Channel) as n 
		on f.`Date` = n.`Date` AND f.Brand = n.Brand AND f.Channel = n.Channel
	WHERE f.date_updated = n.date_updated
	GROUP BY f.`Date`
		, f.Brand
		, f.Channel
;""")

print "Cleaning up. . ."

# General housekeeping

cursor.execute("update db1 set Channel = '' where Channel is null and `Queue opp forecast` is not null;")
cursor.execute("update db1 set `Brand` = trim(`Brand`);")
cursor.execute("update db1 set `Channel` = trim(`Channel`);")



# Make sure data is committed to the database
sql.commit()
cursor.close()
sql.close()

