# s3-versioning-nuke
A single python script that will calculate size and count total number of delete markers, current and non-current objects in bucket within your AWS account. This script is designed for people who need to empty/delete a version enabled S3 bucket

Pre-reqs:
  - Have any code editor that can execute python files installed
  - Python 3.X installed (file created with 3.11)
  - AWS CDK with boto3
  
  To run the script, simply connect to your AWS account with any editor like Visual Studio Code and run the python file.
  From there it will give you a series of inputs to select which bucket to analyze and empty.
  
  Deletions are restricted to 1000 items at a time, so be aware that any buckets with a significant amount of objects will take some time to do the inital analyze and the deletion process.
  
  Analyzing buckets with extremely high amounts of objects will take time - so dont worry, the script isnt hung up if you came here looking for an answer - it just takes a while.
  
 During the deletion process for every 1k objects that are deleted, you will recieve an output log entry, so as long as you are still receiving output log entries the deletion process in still in progress. 
