#/usr/bin/python
import re
s = re.compile('^\*\*.*$')
last_line = ''
with open('/usr2/baf/bis/bismark/mailserv.txt') as file:
	last_line = file.readlines()[-1]
if s.match(last_line):
	print (last_line)
