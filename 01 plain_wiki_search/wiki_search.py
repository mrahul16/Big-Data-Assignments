#!/usr/bin/env python3

import sys
import data_utils


def get_search_results(data_dir, key, n):

	# temp_n = None
	# if n[0] == ['data']:
	# 	temp_n = list(range(3))
	# else:
	# 	temp_n = n
	for i in n:
		wum, urls = data_utils.prepare_data(data_dir, i)

		results_indices = []

		if key in wum:
			results_indices = list(map(int, wum[key].split()))

		results = [urls[result] for result in results_indices]
		
		print()
		[print(result) for result in results]

		if len(results) == 0:
			print("%s not found in given file" % key)
		print()
	
	print("I/O: %.3f ms" % data_utils._IO_TIME)
	if data_utils._CPU_TIME == 0:
		print("CPU ~ %d ms" % data_utils._CPU_TIME)
	else: 
		print("CPU: %.3f ms" % data_utils._CPU_TIME)


if __name__ == "__main__":

	l = len(sys.argv)

	if l >= 4:
		args = sys.argv

		if args[1] == "-k":
			key = args[2]
			if args[3] == 'data':
				get_search_results("data", key, range(3))
			else:
				try:
					# print(args)
					file_list = list(map(int, args[3:]))
				except Exception as e:
					print(e)
				else:
					get_search_results("data", key, file_list)
