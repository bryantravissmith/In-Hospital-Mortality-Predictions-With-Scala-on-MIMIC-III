read_file = open('NOTEEVENTS.csv')
write_file = open('NOTEEVENTS_PROCESSED.csv','w')

header = read_file.readline()
write_file.write(header)

quotes = 0
count = 0
while True:
	line = read_file.readline()
	if not line:
		break
	new_line = ' '.join(line.replace("\n","").split())
	quotes += line.count("\"")
	while quotes % 2 == 1:
		line = read_file.readline()
		quotes += line.count("\"")
		new_line += ' '
		new_line += ' '.join(line.replace("\n","").split())
	new_line += "\n"
	count+=1
	write_file.write(new_line)
