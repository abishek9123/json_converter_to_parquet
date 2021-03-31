import json
import os
import glob

from pyspark import SparkContext, SparkConf, SQLContext

appName = "JSON Parse to Parquet File"
master = "local[2]"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# function to create final output
def remove_duplicates(file_name):
	# Opening JSON file
	f = open(file_name, )
	
	# returns JSON object as a dictionary
	data = json.load(f)
	
	# creating spark dataframe through the dictionary
	input_data = data['records']
	input_df = sqlContext.read.json(sc.parallelize(input_data))
	
	# closing file
	f.close()
	
	# removing duplicates if any
	result_df = input_df.drop_duplicates(['id', 'ts'])
	return result_df
	

# Reading the files and then archiving the files
input_dir = '/input_folder/'
archive_folder = '/archive/'
final_output_folder='/parquet/'
json_pattern = os.path.join(input_dir, '*.json')
file_list = glob.glob(json_pattern)

for file in file_list:
	# Calling remove_Duplicates
	output_df = remove_duplicates(file)
	
	# Getting File Name
	file_path = file.split('/')
	file_name=file_path[-1].split('.')
	file_parquet=file_name[0]+'.parquet'
	file_json=file_name[0]+'.json'
	
	# writing the data into parquet
	output_df.repartition(1).write.mode('overwrite').parquet(final_output_folder+file_parquet)
	
	#archive the processed file into another path
	os.rename(file, archive_folder+file_json)