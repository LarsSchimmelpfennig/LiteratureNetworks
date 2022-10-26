import json
import os
import sys
from article_collection import ArticleCollection
import time
import csv
from dask import dataframe as dd
import pandas as pd

"""This script is used to convert all the reference data from the WOS CORE files to 
   a set of citation json files. The keys map to a list of papers that cite them.
   Only the paper's id and year are used in these new files.
   The ArticleCollection class simplifies extracting the information for each paper from the json files.
"""

#creates a year citation file, this is used while reading the sorted edge list
def add_curr_d(parent, curr_d, idx, l_citations_children):
	if parent in curr_d.keys() and l_citations_children[idx] not in curr_d[parent]:
		curr_d[parent].append(l_citations_children[idx])
	#else we want to create a list with this child
	else:
		curr_d[parent] = [l_citations_children[idx]]

def citation_year_dicts(target_year):
	t1 = time.time()
	#first we write an edge list for each core file to a temp csv
	files = os.listdir('D:\citation networks')
	with open(os.path.join('D:\citation networks','{}_temp_edge_list.csv'.format(target_year)),'w') as temp_csv:
		writer=csv.writer(temp_csv, delimiter=',',lineterminator='\n')
		#header
		writer.writerow(['parent', 'cited_by'])
		for file in files:
			file_year = file[:4]
			if file[5:9] == 'CORE' and file_year == target_year:
				print(file)
				for article in ArticleCollection(os.path.join('D:\citation networks',file)):
					#curr_year is the same as target_year?
					curr_year = article['pub_year']
					curr_id = article['id']
					l_refs = list(article.reference_list())#.ids())
					for year, refs in l_refs:
						if year == '' or year == None or len(year) > 4 or int(year) < 1500:
							continue
						#these are sorted so I can stop here
						if int(year) > int(target_year):
							break
						#each row is a touple of a year - followed by the corresponing citation id
						refs = list(refs)
						#print(year,refs)
						for ref in refs:
							if ref == '' or ref == None:
								continue
							#print(ref)
							citation_edge = [year + ', '+ ref, curr_year + ', ' + curr_id]
							writer.writerow(citation_edge)
				#print('deleting core file')
				#os.remove(os.path.join('D:\citation networks', file))	

	print('done writing {} temp edge list'.format(target_year))
	#need to sort edge list here
	start = time.time()
	#print('sorting edge list')
	dask_df = dd.read_csv(os.path.join('D:\citation networks','{}_temp_edge_list.csv'.format(target_year)))
	dask_df =  dask_df.set_index(dask_df.columns[0]).persist()
	#print('writing to csv')
	dask_df.to_csv(os.path.join('D:\citation networks','sorted_{}_temp_edge_list.csv'.format(target_year)), single_file = True)
	del dask_df
	print('completed sorting: ', (time.time()-start)/60, 'mins')
	os.remove(os.path.join('D:\citation networks','{}_temp_edge_list.csv'.format(target_year)))
	
	curr_d = {}
	num_chunks = len(list(pd.read_csv('D:\citation networks/sorted_{}_temp_edge_list.csv'.format(target_year), chunksize=10000)))
	increment = int(num_chunks / 4)
	print('number of chunks to scan: ',num_chunks)
	for num_chunk, chunk in enumerate(pd.read_csv('D:\citation networks/sorted_{}_temp_edge_list.csv'.format(target_year), chunksize=10000)):
		if increment > 0 and (num_chunk+1) % increment == 0:
			print('progress: ',((num_chunk+1) / increment) * 25,'%')
		l_citations_parents = chunk["parent"].values.tolist()
		l_citations_children = chunk["cited_by"].values.tolist()
		year = l_citations_parents[0].split(',')[0]
		curr_file_year = year
		curr_file_name = '{}_citations_years.json'.format(curr_file_year)

		if curr_d == {} and curr_file_name in os.listdir('D:\citation networks'):
			#print('at start, loading curr_d from prev file')
			f = open(os.path.join('D:\citation networks', curr_file_name))
			curr_d = json.load(f)
			f.close()

		#if this file year doesnt exist yet it will be created later and curr_d is {}
		for idx, parent in enumerate(l_citations_parents):
			year = parent[:4]

			#if we have reached the end of the last chunk I want to write this to a file
			if num_chunk+1 == num_chunks and idx+1 == len(l_citations_parents):
				add_curr_d(parent, curr_d, idx, l_citations_children)
				with open(os.path.join('D:\citation networks',curr_file_name), 'w', encoding='utf-8') as outfile:
					json.dump(curr_d, outfile, ensure_ascii=False, indent=4)
				#print('reached end of sorted edge list')
				break

			#if the next id year is a repeat
			if year == curr_file_year: #and (len(curr_d.keys()) < max_len or l_citations_parents[idx + 1] == l_citations_parents[idx]):
				add_curr_d(parent, curr_d, idx, l_citations_children)
				continue

			#if we are at a new year then we want to add this dict to a file
			if year != curr_file_year:
				with open(os.path.join('D:\citation networks',curr_file_name), 'w', encoding='utf-8') as outfile:
					json.dump(curr_d, outfile, ensure_ascii=False, indent=4)
				
				curr_file_year = year
				curr_file_name = os.path.join('{}_citations_years.json'.format(curr_file_year))

				#start filling up new dict depending if already exists or not
				if curr_file_name in os.listdir('D:\citation networks'):
					f = open(os.path.join('D:\citation networks', curr_file_name))
					curr_d = json.load(f)
					f.close()
				else:
					curr_d = {}
				add_curr_d(parent, curr_d, idx, l_citations_children)
	print('completed, ', (time.time()-t1)/60, 'mins')
	#print('deleting sorted temp edge list')
	print('-------------------------------------------------------------------------------')
	os.remove(os.path.join('D:\citation networks','sorted_{}_temp_edge_list.csv'.format(target_year)))

#citation_year_dicts('1999')
#whichever year is the highest citations_years file is the current year
for year in range(1900, 2022):
	files = os.listdir('D:\citation networks')
	#example CORE file name: 1999_CORE_002.json
	for file in files:
		if file[:4] == str(year) and file[5:9] == 'CORE' and '{}_citations_years.json'.format(year) not in files:
			print('starting year: ',year)
			citation_year_dicts(str(year))
			break
