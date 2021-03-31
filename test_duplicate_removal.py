from duplicate_removal import remove_duplicates
import unittest
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("unittests for JSON Parse to Parquet File") \
		.config("spark.some.config.option", "some-value").getOrCreate()
		
class MyTestCase(unittest.TestCase):
	def test_duplicates_in_data(self):
		"""
		This TestCase will check, if file is having same id and time and if yes, it will remove duplicates
		"""
		
		expected_df = Row('data', 'id', 'ts')
		res_df1 = expected_df('ABC', '10101', '2019-04-23T17:25:43.1112')
		res_df2 = expected_df('XYZ', '10102', '2019-04-23T18:25:43.1112')
		seq3 = [res_df1, res_df2]
		expected_df = spark.createDataFrame(seq3)
		actual_df = remove_duplicates('test_duplicates_in_data.json')
		self.assertEqual(actual_df.collect(), expected_df.collect())
	
	def test_no_duplicates_in_data(self):
		"""
		This TestCase will check, if file is having same id only and if yes, it will not remove duplicates
		"""
		
		expected_df = Row('data', 'id', 'ts')
		res_df1 = expected_df('ABC', '10101', '2019-04-23T17:25:43.1112')
		res_df2 = expected_df('XYZ', '10102', '2019-04-23T18:25:43.1112')
		res_df3 = expected_df('XYZ1', '10102', '2019-04-23T19:25:43.1112')
		seq3 = [res_df1, res_df2, res_df3]
		expected_df = spark.createDataFrame(seq3)
		actual_df = remove_duplicates('test_no_duplicates_in_data.json')
		self.assertEqual(actual_df.collect(), expected_df.collect())
		
		
if __name__ == '__main__':
	unittest.main()