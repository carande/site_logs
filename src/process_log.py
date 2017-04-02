# from collections import Counter
# import sys

input_file = "../log_input/small.txt"
# wc_output = sys.argv[2]
# median_output = sys.argv[3]

# Initialize data structures
hostnames = {}
resources = {}
warning = {}
blocked = {}
current_tick = "" # timestamp of previous log line 

# Define functions
def log_parse(line):

	# todo: handle bad lines
	# todo: is it better to split the whole line each time and extract data (like this), or save the array and split strings from that? (time each method)

	ip_string = line.split(" -")[0]
	time_string = line.split("[")[1][:26] # assume timestamp is always 26 chars long
	request = line.split("\"")[1] # returns full request, including HTTP verb
	status = int(line.split()[-2])
	bytes_sent = int(line.split()[-1])

	# print status, bytes_sent 
	return ip_string, time_string, request, status, bytes_sent

# Process file
# todo: check that logs are sorted by time!
with open(input_file, 'r', -1) as f0: # open in read mode with default buffer
	for line in f0:

		# Parse input log line
		ip_string, time_string, request, status, bytes_sent = log_parse(line)

		# Add count to hostnames
		hostnames[ip_string] = hostnames.get(ip_string, 0) + 1 # look up the ip key, and initialize to 0 if it does not exist

		# Tally bandwidth for the resource
		resource = request.split()[1]
		resources[resource] = resources.get(resource, 0) + bytes_sent # Add bytes to the tally for that resource

		# check blocked and warning lists
		if ip_string in blocked:
			print "BLOCKED", line
		elif status == 304:
			if ip_string in warning:
				warning[ip_string][0] += 1 # increment
				# print warning
				# check if we should be blocked
				if warning[ip_string][0] >= 3:
					print "three strikes! Got Blocked "+line
					blocked[ip_string] = time_string # this timestamp indicates start of blocking period

			else: 
				warning[ip_string] = [1, time_string] # initialize
			



		# update blocked and warning lists unless we are still on the same second
		if time_string != current_tick:
			pass
			# todo: update lists


		current_tick = time_string

# ##############
# # F1 output: 10 most common hosts

# # option 1 (11.6s)
# import heapq
# print "sorting by heap"
# for host in heapq.nlargest(10, hostnames, key=hostnames.get):
# 	print host, hostnames.get(host)

# # option 2 (11.8s)
# print "\nsorting by sorted()"
# from operator import itemgetter
# for host in sorted(hostnames, key=hostnames.get, reverse=True)[:10]:
# 	print host, hostnames.get(host)

# ##############
# # F2 output: 10 highest-bandwidth resources

# print "\nTop 10 Resources:"
# for top_resource in heapq.nlargest(10, resources, key=resources.get):
# 	print top_resource, resources.get(top_resource)

