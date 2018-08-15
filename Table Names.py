from tableau_tools.tableau_documents import *
from tableau_tools import *
import urllib2
import csv

##########################################
files = [u'DB One Table.twb', 
		 u'DB One Table Extract.twb', 
		 u'DB Join.twb', 
		 u'DB Join Extract.twb', 
		 u'DB Join Custom SQL.twb', 
		 u'DB Join Custom SQL Extract.twb', 
		 u'DB Join Custom SQL Multiple.twb',
		 u'DB Join Custom SQL Multiple Extract.twb', 
		 u'DB Join Normal and Custom SQL.twb', 
		 u'DB Join Normal and Custom SQL Extract.twb', 
		 u'DB Multiple Tables Custom SQL.twb',
		 u'Two Datasources DB Join Normal and Custom SQL.twb']

#Recursive function to retrieve table names
def query_search(query, substring):
	tables = []
	def search (query, substring):
		if substring in query:
			start_pos = query.find(substring)+len(substring)+1
			
			#Finding the correct delimiter
			if query[start_pos:].find (' ') == -1:
				end_pos = query[start_pos:].find ('\n')
			elif query[start_pos:].find ('\n') == -1:
				end_pos = query[start_pos:].find (' ')
			else:
				end_pos = min(query[start_pos:].find (' '), query[start_pos:].find ('\n'))
			
			#Returning the table name
			if end_pos == -1:
				tables.append(query [start_pos:])
			else:	
				tables.append(query [start_pos:(start_pos + end_pos)])
			
			#Search again with the reduced string
			search ((query[start_pos:]), substring)
	search(query, substring)
	return tables

with open('tables.csv', 'wb') as csvfile:
    table_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    table_writer.writerow(['Workbook Name', 'Datasource Name', 'Live/ Extract', 'Table Name', 'Custom SQL?'])				

for file in files:
	T_file = TableauFile(file)
	t_doc =T_file.tableau_document
	dses = t_doc.datasources

	for ds in dses:
		relation = ds.xml.findall (u'.//relation')
		
		tables = []
		queries = []

		for i in relation:
			#Getting all table names
			table_name = i.get (u'table')
			if table_name is not None:
				tables.append (table_name)
				queries.append ("N/A")
				
			#Searching for Custom SQL
			if i.get (u'name') is not None:
				if "Custom SQL Query" in i.get (u'name'):
					
					for table in query_search (i.text.lower(), 'FROM'):
						tables.append (table)
						queries.append (i.text)
				
					for table in query_search (i.text.lower(), 'JOIN'):
						tables.append (table)
						queries.append (i.text)

		#Tables will be printed if they exist
		if tables:
			#Is datasource an extract or live?
			connection_type = ""
			if tables[-1] == "[Extract].[Extract]":
				connection_type = "Extract"
				del tables [-1]
				del queries [-1]
			else:
				connection_type =  "Live"
			
			#Writing to file
			for counter in range (len(tables)):
				with open('tables.csv', 'ab') as csvfile:
					table_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
					table_writer.writerow([file, ds.ds_name, connection_type, tables[counter].strip("[]`"), queries[counter]])        

	#Newline for each new workbook
	with open('tables.csv', 'ab') as csvfile:
		table_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
		table_writer.writerow("")
