# from collections import Counter
# import bisect
# import sys

input_file = "../log_input/small.txt"
# wc_output = sys.argv[2]
# median_output = sys.argv[3]

# Initialize data structures
hostnames = {}
resources = {}

# Define functions
def log_parse(line):

	# todo: handle bad lines
	# todo: is it better to split the whole line each time and extract data (like this), or save the array and split strings from that? (time each method)

	ip_string = line.split(" -")[0]
	time_string = line.split("[")[1][:26] # assume timestamp is always 26 chars long
	request = line.split("\"")[1] # returns full request, including HTTP verb
	code = line.split()[-2]
	bytes_sent = int(line.split()[-1])

	# print code, bytes_sent 
	return ip_string, time_string, request, code, bytes_sent

# Process file
with open(input_file, 'r', -1) as f0: # open in read mode with default buffer
	for line in f0:

		# Parse input log line
		ip_string, time_string, request, code, bytes_sent = log_parse(line)

		# Add count to hostnames
		hostnames[ip_string] = hostnames.get(ip_string, 0) + 1 # look up the ip key, and initialize to 0 if it does not exist

		# Tally bandwidth for the resource
		resource = request.split()[1]
		resources[resource] = resources.get(resource, 0) + bytes_sent # Add bytes to the tally for that resource



##############
# F1 output: 10 most common hosts

# option 1 (11.6s)
import heapq
print "sorting by heap"
for host in heapq.nlargest(10, hostnames, key=hostnames.get):
	print host, hostnames.get(host)

# option 2 (11.8s)
print "\nsorting by sorted()"
from operator import itemgetter
for host in sorted(hostnames, key=hostnames.get, reverse=True)[:10]:
	print host, hostnames.get(host)

##############
# F2 output: 10 highest-bandwidth resources

print "\nTop 10 Resources:"
for top_resource in heapq.nlargest(10, resources, key=resources.get):
	print top_resource, resources.get(top_resource)

