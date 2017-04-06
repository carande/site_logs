# Site Log processor code description
I kept my code structure relatively simple, the directory structure is the same as the example repo and all of my processing logic is in the python script "process_log.py"

I found Feature 3 (Highest activity windows) to be a little vague: the hour-long windows can start at any time (according to the description), but no information is given on whether the hours can overlap.  Therefore, I added an extra input parameter that can be optionally passed into my script which determines the allowable overlap between windows (in seconds).

This value defaults to 3600, which means that "hours.txt" will be the 10 highest activity periods that do not overlap at all.
Setting this value to 1 allows windows to be 1 second apart.
  



