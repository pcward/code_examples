import glob
import datetime
import string
import os
import MySQLdb
from sys import argv

# Create a list of brands and keywords

keywords = { 'a': 'a', 'b': 'b' }

def find_key(dic, val):
    # return the key of dictionary dic given the value
    return [k for k, v in dic.iteritems() if v == val][0]


if len(argv) > 1:
	output_opt = argv[1]
else:
	output_opt = 'file'


if output_opt == 'file':
	#output file
	ofile = 'output_' + datetime.date.today().strftime("%Y-%m-%d") + '.csv'
	csv = open(ofile,'a')
	header = 'date,brand,keyword,gt_index,date_insert\n'

	csv.write(header)

elif output_opt == 'db':
	sql = MySQLdb.connect(host="127.0.0.1", user="user", passwd="pwd", db="db")
	cursor = sql.cursor()


date_insert = datetime.date.today().strftime("%Y-%m-%d")

# Open all the csvs in the directory, one by one. For each file:
files = glob.glob('*.csv')


for f in files:
	if output_opt == 'file':
		if f == ofile:
			continue

	fp = open(f,'r')

	line_num = 1

	# Discard lines 1-4
	# For line 5: split the line into parts. Match the second part to brand in the brand list above

	for line in iter(fp):
		if line_num == 5:
			# Set brand and keyword vars
			stuff = [x.strip() for x in line.split(',')]
			keyword = stuff[1]
			try:
				brand = find_key(brands,stuff[1])
			except IndexError:
				break

			line_num += 1
		elif line_num > 5:
			stuff = [x.strip() for x in line.split(',')]

			if len(stuff)<2:
				break
			elif stuff[1] == '':
				break

			if output_opt == 'file':
				# Write date, brand, keyword, and google trends index to file
				oline = stuff[0] + ',' + brand + ',' + keyword + ',' + stuff[1] + date_insert + '\n'
				csv.write(oline)

			elif output_opt == 'db':
				# Write date, brand, keyword, and google trends index to file
				oline = '\'' + stuff[0] + '\',\'' + brand + '\',\'' + keyword + '\',' + stuff[1] + ',\'' + date_insert + '\''
				cursor.execute("INSERT INTO db_table (`date`,`brand`,`keyword`,`gt_index`,`date_insert`) VALUES (%s);" % oline)

		else:
			line_num += 1

	fp.close()
	os.unlink(f)

if output_opt == 'file':
	csv.close()

elif output_opt == 'db':
	# Make sure data is committed to the database
	sql.commit()
	cursor.close()
	sql.close()
