in order to get into file system need to note few things:


hadoop fs -ls /
will list all files in root directory.

---------------------------
hadoop fs -ls /output
hadoop fs -ls output
---------------------------
 is not the same! , without "/" it will go for local user/Administrator files.
the same way it goes for opening files in JAVA with the hadoop FileSystem.

if want files from the root directory, we must mention /- e.g.: new Path("/output3/blah.txt");
this will it will look from the root and not from the user directory,


hit build for building the database from the input files, in order to support the word complition.