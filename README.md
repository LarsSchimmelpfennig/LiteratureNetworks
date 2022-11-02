# Literature Networks - UW-Madison Gellman Group

Data source - Web of Science CORE dataset provided by UW-Madison Libraries

article_collection module source: https://github.com/UW-Madison-Library/wos-explorer

Foldamers are among the main areas of research of the Gellman Group. As a member of the group, I am interested in the development of this field.

**Complete citation network for the field of Foldamers (2021)**
 - Node and edge color is assigned by modularity class (clustering).
 - Node size is assigned by in-degree. The larger nodes represent the more frequently cited papers in the network.

![complete network](foldamers_citation_network_2021.png)

**Citation network spanning 1998-2003 starting from 'Foldamers: a Manifesto' by Samuel H Gellman (1998)**
![network](Manifesto_1998-2003_network.png)

Traversing this network was accomplished by traverse_final.py. The literature dataset I had access to exclusively contained reference information. The script I wrote to convert to citation data is detailed in converting_reference_files_to_citation_dict.py. This process was complicated significantly by limited memory. Each CORE file was  converted to a large edge list of citations by reversing the direction of each pointer. I used dask dataframes to make sorting these large edge lists more efficent. Lastly, I loaded chunks of the edge list into memory from a csv and wrote the contents to the corresponding citation json file by year. 

With these citation json files I was able to traverse the citation network successfully. However, the size of the complete network grows so quickly that I wanted to include only a subset of the most cited papers for each paper. I was able to update my citation json files to include this number of citations with add_num_citations.py.

**Coauthorship Network for Authors of Foldamer Papers**
![coauthor](foldamer_coauthor_network_green.png)

**Visualizing the change in topics studied in the Foldamer literature**
<p align="center"> <img src="topic_heatmap.gif" width="700" height="600" /> </p>
Using a Word2Vec model I am able to visualize the change in topics for a given field of literature. The model was trained with the entire Web of Science dataset containing papers up to 2021. The title, headers, subjects, keywords, and abstract for each paper were used for training. 

