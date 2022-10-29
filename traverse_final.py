import os
import csv
import time
import orjson as json

#for this version I will not use a Graph object as this is extra memory usage - I will write each edge directly to a csv file and nothing more

#TODO use sys.getsizeof() to track witch objects are using the most memory. 


def traverse_final(root_ids, root_year, end_year):
	with open(os.path.join('citation_network_images','manifesto_edge_list_{}-{}.csv'.format(root_year, end_year)),'w') as temp_csv:
		writer=csv.writer(temp_csv, delimiter=',',lineterminator='\n')
		writer.writerow(['parent', 'cited_by'])
		years_d = {}
		set_parents_added = set()
		total_num_edges = 0

		#testing variables here
		time_to_remove_keys = 0
		time_writing_rows = 0


		##########################################
		#ids are in the form 'year, num_citations, WOS_id' ex. '1998, 2229, WOS:000073512800005'
		#below sets up years_d, this dictionary/hash table uses years as keys to store lists of all of the ids that need to be searched
		#after the cited papers are found each id is removed from this dictionary
		##########################################
		
		for id in root_ids:
			year = id.split(', ')[0]
			if year not in years_d.keys():
				years_d[year] = [id]
			else:
				years_d[year].append(id)
		print('done setting up years_d')
		
		l = list(years_d.keys())
		l.sort()
		curr_year = l[0]
		file = '{}_num_citations_yearsv2.json'.format(curr_year)
		t1 = time.time()

		##########################################
		#d is a dictionary from each file year. The keys are ids and each of these contain a list of citing ids
		##########################################

		f = open(os.path.join('citation network WOS dataset',file))
		d = json.loads(f.read())
		del f
		print('done loading file: ',file,' ', (time.time()-t1)/60, ' mins')
		d_keys_set = set(d.keys())
		t0 = time.time()

		#This while loop is the main thing to be executed until the edge list is completed. Above this is just setup.

		while len(years_d.keys()) > 0:

			##########################################
			#with this dict I want to find the children of each id, create their edges, remove parents from this part of years_d, 
			#then add these new children to years_d for them to be checked in the future
			##########################################

			for parent in years_d[curr_year]:

				##########################################
				#if I have already added this parent to years_d at some point for this year I dont want to add it again.
				#this is necessary as sometimes there will be a loop 3 or more citations that would create an infinite loop.
				##########################################

				if parent in set_parents_added:
					temp_time_remove = time.time()
					years_d[curr_year].remove(parent)
					time_to_remove_keys += (time.time() - temp_time_remove) #in seconds
					continue
				set_parents_added.add(parent)

				if parent in d_keys_set:
					#I dont need to sort by num citations I am adding all children
					children = d[parent]
					#children.sort(key=lambda x:x.split(', ')[1], reverse=True)
				else:
					temp_time_remove = time.time()
					years_d[curr_year].remove(parent)
					time_to_remove_keys += (time.time() - temp_time_remove) #in seconds
					continue
				for child in children:
					temp_time_writing = time.time()
					writer.writerow([parent, child])
					time_writing_rows += (time.time() - temp_time_writing)

					total_num_edges+=1
					year = child.split(', ')[0]
					if year not in years_d.keys():
						years_d[year] = [child]
					elif child not in years_d[year]:
						years_d[year].append(child)
						#if year == curr_year:
							#this print statement helps show the speed that the list of ids to check for the current year is growing/shrinking - just for testing
							#print('adding child to same year, num parents to check: ', len(years_d[curr_year]))
				temp_time_remove = time.time()
				years_d[curr_year].remove(parent)
				time_to_remove_keys += (time.time() - temp_time_remove) #in secondsd
			
			#If I have reached the end for this year change file	
			if len(years_d[curr_year]) == 0:
				print('number of edges: ',total_num_edges, 'time to traverse file: ',(time.time()-t0)/60, ' mins')
				print('total time spent removing ids from years_d: ', time_to_remove_keys, ' seconds')
				print('total time writing rows to edge list: ', time_writing_rows,' seconds')
				print('------------------------------------------------------------------')
				print(' ')

				del years_d[curr_year]
				l = list(years_d.keys())
				l.sort()
				curr_year = l[0]
				file = '{}_num_citations_yearsv2.json'.format(curr_year)
				if file in os.listdir('citation network WOS dataset'):
					#print('loading ', file)
					t1 = time.time()
					f = open(os.path.join('citation network WOS dataset',file))
					del d
					d = json.loads(f.read())
					del f
					print('done loading file: ',file,' ', (time.time()-t1)/60, ' mins')
					d_keys_set = set(d.keys())
					set_parents_added = set()
					t0 = time.time()
	
	
foldamers_origin = ['1996, 617, WOS:A1996WA82700027',
 '1997, 545, WOS:A1997XA49600054',
 '1997, 206, WOS:A1997YK50200033',
 '1998, 2229, WOS:000073512800005',
 '2001, 2083, WOS:000172874900010',
 '1999, 1241, WOS:000078230400061']

#I have an incorrect value here - likely from the few missing files I missed when I first created it and these dont include the 31 citations in 2022
#I should redo the creation of these files
manifesto = ['1998, 2229, WOS:000073512800005']

traverse_final(manifesto, 1998, 2021)