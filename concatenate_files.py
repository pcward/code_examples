import glob

filenames = glob.glob('*.csv')
print filenames
#filenames = [1,2,3,4,5]

file_num = 0

with open('data.csv', 'w') as outfile:
    for fname in filenames:
    	
    	line_num = 1
    	file_num += 1

        name = '2015_0' + str(fname) + '.csv'
        with open(name) as infile:

        	for line in infile:
        		if file_num > 1:
        			if line_num == 1:
        				line_num += 1
        				continue
        			else:
        				outfile.write(line)
        				line_num += 1
        		else:
        			outfile.write(line)
        			line_num += 1