import orjson as json
import os
import time
import re
import progressbar

"""add_num_citations reads through all previously created citation json files and reformats them to 
   include the number of citations at each parent and child reference. Updating the parent reference 
   uses the length of the list it references. In order to update each child reference, I construct a 
   dictionary with years as keys and a list of children from this year. Next, I open the corresponding 
   citation year file where I can find these children as parents and record the length of each of the lists 
   they reference. 
"""

def add_num_citations():
	for year in range(1995, 2022):
		if '{}_num_citations_yearsv2.json'.format(year) in os.listdir('D:\citation networks'):
			continue
		t1 = time.time()
		#this will give me a list of the files that fall under year
		files = os.listdir('D:\citation networks')
		#2021_citations_01.json
		l_names = []
		for file in files:
			name = re.findall(r'[0-9]{4}_citations_[0-9]{2}.json',file)
			if name != [] and name[0][:4] == str(year):
				l_names.append(name[0])
		print(l_names)
		print(year)
		#here I have a list of starting files of one year - I want to create a lookup dict from all of these
		#file = '{}_citations_years.json'.format(year)
		years_d = {}
		for file in l_names:
			f = open(os.path.join('D:\citation networks',file))
			d = json.loads(f.read())
			#this is the starting dict that will contain parents with their num citations but not for children
			#replace '1998, id' with '1998, 2, id' to add num citations
			for parent in d.keys():
				num_citations = len(d[parent])
				updated_parent = parent.replace(',', ', '+str(num_citations)+',')
				#for each child I want to add them to respective year of new dict including dict based on parent
				for child in d[parent]:
					child_year = child.split(',')[0]
					if int(child_year) < 1900 or int(child_year) > 2021:
						continue
					edge = updated_parent+'->'+child
					if child_year not in years_d.keys():
						years_d[child_year] = [edge]
					else:
						years_d[child_year].append(edge)
		print('finished creating starting dictionary of years containing edge lists')
		del d
		del f
		final_edge_list = []
		l_years = list(years_d.keys())
		l_years.sort()
		#years_d.keys().sort()
		#after going through the entire starting year file and creating a complete years_d I will now go through each year file
		#this program will take a very long time as I need to open so many future year files
		timer = time.time()
		for curr_year in l_years:
			start_time = time.time()
			edge_list = years_d[curr_year]
			l_names = []
			for file in os.listdir('D:\citation networks'):
				name = re.findall(r'[0-9]{4}_citations_[0-9]{2}.json',file)
				if name != [] and name[0][:4] == str(curr_year):
					l_names.append(name[0])
			#bar = progressbar.ProgressBar(maxval=20, widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
			#bar.start()
			#with this list of files I want to go through each file then iterate through all keys - if I find I remove and add to final edge list
			for idx, file in enumerate(l_names):
				#bar.update(idx+1)
				#print(file)
				t2 = time.time()
				f = open(os.path.join('D:\citation networks',file))
				d = json.loads(f.read())
				#print(file, "loaded in: ", (time.time()-t2)/60, ' mins')
				d_keys_set = set(d.keys())
				
				#found will be based on the previous not_found after each iteration
				if idx == 0:
					#this is the first iteration/file
					found = [item for item in edge_list if item.split('->')[1] in d_keys_set]
					not_found = [item for item in edge_list if item.split('->')[1] not in d_keys_set]
					#print('first file: ',len(found), len(not_found))
				else:
					#after the first iteration found will be based on prev 
					found = [item for item in not_found if item.split('->')[1] in d_keys_set]
					not_found = [item for item in not_found if item.split('->')[1] not in d_keys_set]
					#print('file ',idx+1, ': ',len(found),' ', len(not_found))
				#print(file, "loaded in: ", (time.time()-t2)/60, ' mins')
				#for edge in edge_list[::-1]:
				#t2 = time.time()
				for edge in found:
					parent,child = edge.split('->')
					if child in d_keys_set:
						child_num_citations = len(d[child])
						updated_child = child.replace(',', ', '+str(child_num_citations)+',')
						final_edge_list.append(parent+'->'+updated_child)
			print('len of edge list for year: ',curr_year, " ", len(edge_list), 'time: ', (time.time()-start_time)/60, ' mins')
			#bar.finish()
				#print('time to look through' ,file,  ' with edge_list: ', (time.time()-t2), ' secs')
				
		del d
		to_return_d = {}
		print('finished creating updated edge list for given year in ', (time.time()-timer)/60, ' mins')

		#now I should have a completed edge list and can reform the large dict and write it to the current year file
		for edge in final_edge_list:
			parent, child = edge.split('->')
			if parent in to_return_d.keys():
				to_return_d[parent].append(child)
			else:
				to_return_d[parent] = [child]
		del final_edge_list

		curr_file_name = '{}_num_citations_yearsv2.json'.format(year)
		with open(os.path.join('D:\citation networks',curr_file_name), 'wb') as outfile:#, encoding='utf-8') as outfile:
			outfile.write(json.dumps(to_return_d))
			#json.dump(to_return_d, outfile, ensure_ascii=False, indent=4)
		del to_return_d
		print('completed: ',(time.time()-t1)/60, ' mins')
		print('-------------------------------------------------------------------------------------')
		

add_num_citations()
