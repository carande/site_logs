import sys
from collections import Counter
import time
import numpy as np
import heapq

############ 
# I/O Setup

# default file locations
input_file = 		"../log_input/med.txt"

hosts_output = 		"../log_output/hosts.txt"
hours_output = 		"../log_output/hours.txt"
resources_output = 	"../log_output/resources.txt"
blocked_output = 	"../log_output/blocked.txt"
overlap = 3600

# Overwrite if given command line parameters
try:
	input_file = 		sys.argv[1]
	hosts_output = 		sys.argv[2]
	hours_output = 		sys.argv[3]
	resources_output = 	sys.argv[4]
	blocked_output = 	sys.argv[5]
	overlap = 			sys.argv[6]
except Exception as e:
	print "some command line arguments missing, using defaults for some values:"
	print input_file, hosts_output, hours_output, resources_output, blocked_output, overlap
	pass


################# 
# Setup Variables

# Assuming time zone is always -0400
TIME_FORMAT = "%d/%b/%Y:%H:%M:%S -0400" # '01/Jul/1995:00:00:01 -0400'

# Initialize data structures
hostnames = {}
resources = {}

warning = {}
blocked = {}

previous_time = "" 	# timestamp of previous log line (string)
current_time = None # time in seconds since epoch
t0 = None 			# time offset of first point in seconds

activity = [0] # list of activity counts each second

################# 
# Setup Functions

def logParse(line):
	try:
		ip_string = line.split(" -")[0]
		time_string = line.split("[")[1][:26] # assume timestamp is always 26 chars long
		request = line.split("\"")[1].split()[1] # returns only resource
		status = int(line.split()[-2])
		try:
			bytes_sent = int(line.split()[-1])
		except ValueError as v: # catch the case where bytes are given as "-"
			bytes_sent = 0
	except Exception as e:
		print "cannot process line: "+line, e # could print this to a file instead
		raise e

	return ip_string, time_string, request, status, bytes_sent

def warningUpdate(current_time):
	for ip, warn_tally in warning.items():
		elapsed = current_time - warn_tally[1]
		if elapsed > 20.0:
			del warning[ip]

def blockedUpdate(current_time):
	for ip, block_time in blocked.items():
		elapsed = current_time - block_time
		if elapsed > 5*60.0: # 5-min block period
			del blocked[ip]

def getTime(time_string):
	return int(time.mktime(time.strptime(time_string, TIME_FORMAT)))


# Get initial time from first line of file
with open(input_file, 'r', -1) as f:
	previous_time = logParse(f.readline())[1] # initial time, as string
	t0 = getTime(previous_time) # initial time, in seconds
	current_time = getTime(previous_time) # set this to t0 for first iteration
print "first time is "+previous_time, str(t0)


################# 
# Process File

with open(input_file, 'r', -1) as f0: # open in read mode with default buffer
	with open(blocked_output, "w") as b0:

		for line in f0:

			# Parse input log line
			try: 
				ip_string, time_string, resource, status, bytes_sent = logParse(line)
			except Exception as e:
				continue # can't process this line, just go on to next one	

			# Add count to hostnames (F1)
			hostnames[ip_string] = hostnames.get(ip_string, 0) + 1 # look up the ip key, and initialize to 0 if it does not exist

			# Tally bandwidth for the resource (F2)
			resources[resource] = resources.get(resource, 0) + bytes_sent # Add bytes to the tally for that resource

			# time-dependent functions (F3 and F4)

			if time_string != previous_time: # only update if we are on a new second
				new_time = getTime(time_string)
				delta = new_time - current_time
				activity.extend([0 for i in range(delta)]) # add 0s to our activity list based on the number of skipped seconds

				current_time = new_time

				# update lists to see if we have waited long enough
				warningUpdate(current_time)
				blockedUpdate(current_time)

			activity[current_time-t0] += 1 # Increment activity for this second

			# check blocked and warning lists
			if ip_string in blocked:
				b0.write(line)
			elif ip_string in warning:
				if status < 300: # "good" request resets the warning timer
					del warning[ip_string]

				elif status == 401: # unauthorized request increments warning
					warning[ip_string][0] += 1
					# check if we should be blocked
					if warning[ip_string][0] >= 3:
						blocked[ip_string] = current_time # this timestamp indicates start of blocking period
						del warning[ip_string] # remove from warning because we know they are blocked

			elif status == 401: # first warning
				warning[ip_string] = [1, current_time] # initialize

			# update time string
			previous_time = time_string


##############
# F1 output: 10 most common hosts
print "\nsorting most common hosts"

with open(hosts_output, "w") as file:
	for host in heapq.nlargest(10, hostnames, key=hostnames.get):
		file.write( host +","+ str(hostnames.get(host))+"\n" )


##############
# F2 output: 10 highest-bandwidth resources
print "\nsorting top 10 Resources"

with open(resources_output, "w") as file:
	for top_resource in heapq.nlargest(10, resources, key=resources.get):
		file.write( top_resource+"\n")

###############
# F3 output: Busiest 60-min periods

summed_activity = Counter() # integrating the next hour of activity, for each second
print "integrating activity"

# seed first point
previous_point = sum(activity[:3600])

summed_activity[0] = previous_point
activity.extend(np.zeros(3600, dtype=int)) # pad end of list so we can integrate out to end

for i in range(len(activity)-3600): # increment window by one space each time
	previous_point = previous_point - activity[i] + activity[i+3600] # remove trailing point, and add one additional point
	summed_activity[i+1] = previous_point


# find 10 highest counts not lying in an existing window
print "finding unique highest activity periods"
highest = []
for point in summed_activity.most_common():
	if len(highest) < 10:
		deltas = [abs(point[0]-i[0]) for i in highest]
		if not any(d<overlap for d in deltas): # save highest points that don't overlap within an hour
			highest.append(point)

with open(hours_output, "w") as file:
	for i in highest:
		file.write(time.strftime(TIME_FORMAT, time.localtime(i[0]+t0)) +","+ str(i[1]) +"\n")

##############
# F4 output: Blocked access attempts (already written to file)


