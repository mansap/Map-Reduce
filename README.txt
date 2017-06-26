Title: README for running MapReduceClient.java 

We expect the log files to be stacked together in a folder and the path of the folder to be passed as command line argument to our Map Reduce client 'MapRedClient.java'. 

There are two files:  The provided 'MapReduce.class' file[1] and the solution we have implemented 'MapRedClient.java'. 
These two files should be in the same folder while running the code.

The code should be run as follows:
1. Compile 'MapredClient.java' file using the following command:
	javac MapRedClient.java
2. Run the solution as with the following command:
	java MapRedClient <folder_path>
	For example: java MapRedClient.java /usr/local/pub/large_log_files
	Here, large_log_files is the folder which contains the log files
3. <folder_path> is the string representing the path to the folder where the log files are present. The files need to be present in a directory.
