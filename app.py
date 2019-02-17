import pandas as pd
import numpy as np
import multiprocessing
import re
import os, sys, glob

from tkinter.filedialog import askopenfilenames, asksaveasfilename
from tkinter import messagebox
from bs4 import UnicodeDammit
from collections import Counter
from multiprocessing.pool import MaybeEncodingError



class AdvancedFilter:

	def __init__(self):
		self.dtypes, self.date_columns = get_dtypes_and_date_columns()
		self.columns = get_columns()
		self.filter_files = get_all_txt_files_in_input_exclude_dtypes_and_columns()
		self.filters = get_filters(self.filter_files)
		
	def querying(self, file) -> pd.DataFrame:
		unicode = get_unicode(file)
		file_name = os.path.basename(file)
		if file_name.split('.')[1] == 'csv':
			df = pd.read_csv(file, encoding=unicode, low_memory=True, sep=None, dtype=self.dtypes, engine='python')
		elif file_name.split('.')[1] == 'xlsx':
			df = pd.read_excel(file, encoding=unicode, low_memory=True, dtype=self.dtypes)
		else:
			print(f'{file_name} will not be processed because it isn\'t csv or xlsx file!')
			return None
		# check if user put columns which are not available in df
		columns = [column for column in self.columns if column in df.columns]
		if len(columns) != len(self.columns):
			print(f'{file_name} hasn\'t such columns {[column for column in self.columns if column not in columns]}!')   
		df = df[columns]

		# parse date columns
		if self.date_columns:
			for column in self.date_columns:
				if column in columns:
					try:
						df[column] = pd.to_datetime(df[column], dayfirst=False)
					except ValueError:  # happens when day comes first in date
						df[column] = pd.to_datetime(df[column], dayfirst=True)
		
		# patterns for looking greater/less numeric and str filters
		pattern_for_num = r'([<>=]+)([0-9]+)'
		pattern_for_str = r'(contains|startswith|endswith) (.+)'
		pattern_for_date = r'([<>=]+)(\d{2}[.,/\\-]\d{2}[.,/\\-]\d{4}|\d{4}[.,/\\-]\d{2}[.,/\\-]\d{2})$'
		
		# iterate through filters to apply filters to df
		for column, values in self.filters.items():
			# check if filter for column has values
			if not values:
				continue

			# convert dtypes from df property to pythonic types
			original_column_type = type(df.iat[0, df.columns.tolist().index(column)])
			column_type = int if original_column_type is np.int64 else float if original_column_type is np.float64 else original_column_type
			if column_type is int or column_type is float:
				# looking for greater/less filters
				matcher_num = [re.match(pattern_for_num, value) for value in values]
				if any(matcher_num):
					for condition in [match for match in matcher_num if match]:
						df = df[eval(f'df[column] {condition.group(1)} {column_type.__name__}(condition.group(2))')]
					continue
			elif column_type is str:
				# looking for str filters
				matcher_str = [re.match(pattern_for_str, value) for value in values]
				if any(matcher_str):
					for condition in [match for match in matcher_str if match]:
						df = df[eval(f'df[column].str.lower().str.{condition.group(1)}(condition.group(2).lower())')]
					continue
			elif column_type is pd.Timestamp:
				matcher_date = [re.match(pattern_for_date, value) for value in values]
				if any(matcher_date):
					for condition in [match for match in matcher_date if match]:
						df = df[eval(f'df[column].dt.date{condition.group(1)}pd.Timestamp(condition.group(2)).date()')]
					continue
					
			# convert numeric values for filter into type which has same column in df
			if column_type is int or column_type is float:
				values = [eval(f'{column_type.__name__}({x})') for x in values]
				
			# apply to df equality filters  
			df = df[df[column].isin(values)]
			
			# check if df is empty because current filter isn't maching any rows
			if df.empty:
				break
		print(f'{file_name} is done!')
		return df
				

def get_dtypes_and_date_columns() -> tuple:
	with open('input/dtypes.txt') as f:
		types_split = [value.split(':') for value in f.readlines()]
	dtypes = {value[0]: eval(value[1].strip()) for value in types_split if value[1].strip() != 'date'}
	date_columns = [value[0] for value in types_split if value[1].strip() == 'date']
	return dtypes, date_columns

		
def get_columns() -> list:
	with open('input/columns.txt') as f:
		columns = [column.strip() for column in f.readlines()]
	return columns


def get_filters(filter_files) -> dict:
	filters = dict()
	for file in filter_files:
		with open(file, 'r') as f:
			filter_list = [value.strip() for value in f.readlines()]
		filters[file.split('\\')[1].split('.txt')[0]] = filter_list
	return filters


def get_all_txt_files_in_input_exclude_dtypes_and_columns() -> list:
	return [file for file in glob.glob('input/*.txt') if file.split('\\')[1] not in ['dtypes.txt', 'columns.txt']]
	

def get_unicode(file):
	with open(file, 'rb') as f:
		lines = b' '.join(f.readlines())
	encoding = UnicodeDammit(lines).original_encoding
	return encoding


if __name__ == '__main__':
	if sys.platform.startswith('win'):
		multiprocessing.freeze_support()
	print('Please select files which will be processed.')
	array = askopenfilenames(defaultextension="csv", filetypes=
							 (("CSV file", "*.csv"), ("Excel Workbook", "*.xlsx"),("All files", "*"),))
	advanced_filter = AdvancedFilter()
	p = multiprocessing.Pool()
	# don't forget to use pool !
	try:
		overall = p.map(advanced_filter.querying, array)
	except MaybeEncodingError:
		messagebox.showerror("Memory Error", "It seems that Memory Error has occured. " \
								"It's because using your filter retrieved too much data. " \
								"Try to use another filter or open less number of files.")
		sys.exit(0)
	p.close()
	p.join()
	result = pd.concat(overall)
	
	# check if filter result is empty
	if result.shape[0] == 0:
		messagebox.showinfo("Empty result", "Your result is empty. No data in your files for used filters")
		sys.exit(0)
		
	print(f'Your result file has: {result.shape[0]} rows')
	messagebox.showinfo("Completed", "Please select file where to save filtering result.")
	
	# choosing file for saving result
	while True:
		result_file = asksaveasfilename(defaultextension='xlsx', filetypes=
									(("Excel Workbook", "*.xlsx"),("CSV file (; for delimiter)", "*.csv"),))
		if not result_file:  # check if file was selected and prompt wasn't closed by incident
			answer_if_quit = messagebox.askquestion("Not saving",
													"So do you really not want to save the result of this query???")
			if answer_if_quit == 'yes':
				messagebox.showinfo("Sayonara", "See you space cowboy....")
				sys.exit(0)
		else:
			try:
				f = open(result_file, 'w')
			except IOError:
				messagebox.showerror("Permission Error", "Please close file for saving or choose another file!")
				continue
			else:
				f.close()
				break
	
	# saving result to excel or csv file
	result_file_name = os.path.basename(result_file)
	if result_file_name.split('.')[1] == 'xlsx':  # for xlsx result file
		try:
			result.to_excel(result_file, index=False)
			messagebox.showinfo("Completed", "Saving is completed! Enjoy your data!")
		except (ValueError, MemoryError):
			messagebox.showerror("Memory Error", "Wow! It seems that Memory Error has occured. " \
								"Let's try to save your result to csv as this file format is using less memory."
								)
			result_file_csv = asksaveasfilename(defaultextension="csv", filetypes=
												(("CSV file (; for delimiter)", "*.csv"),))
			result.to_csv(result_file_csv, sep=';', encoding='utf-16', index=False)
			messagebox.showinfo("Congratulations!", "It has worked! Enjoy your data!")
	else:  # for csv result file 
		result.ro_csv(result_file, sep=';', encoding='utf-16', index=False)
		messagebox.showinfo("Completed", "Saving is completed! Enjoy your data!")
