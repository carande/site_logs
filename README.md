# Site Log processor code description
I kept my code structure relatively simple, the directory structure is the same as the example repo and all of my processing logic is in the python script "process_log.py"

To invoke:
process_log.py {input_file} {hosts_output} {hours_output} {resources_output} {blocked_output} {overlap_window}


I found Feature 3 (Highest activity windows) to be a little vague: the hour-long windows can start at any time (according to the description), but no information is given on whether the hours can overlap.  Therefore, I added an extra input parameter that can be optionally passed into my script which determines the allowable overlap between windows (in seconds).

This value defaults to 3600, which means that "hours.txt" will be the 10 highest activity periods that do not overlap at all.
Setting this value to 1 allows windows to be 1 second apart.
  
Because of this default behavior, my script "as written" does not pass 1 of the unit tests, since the test expects multiple overlapping "busiest" windows to be given for this 15 second period, and my code gives only 1 window when using default settings.  

If windows are allowed to overlap, and can start at any time (independent of events), then I think the 10 busiest periods in the test data should actually be given by the first 10 seconds (00:00:01, 00:00:02, etc), since these windows contain all, or nearly all, of the test data events.  This is the result given by my script when setting overlap=1.
