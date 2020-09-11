import tableauserverclient as TSC
import csv

#Server Details#
SERVER_NAME = "http://"
SITE_NAME = ""
USERNAME = ""
PASSWORD = ""

#Providing column headers for CSV file#
csvfile  = open("tables.csv", "w+")
table_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
table_writer.writerow(['Workbook Name', 'Datasource Name', 'Published Datasource?', 'Extract?',
                       'Table Name', 'Database Name', 'Connection Type', 'Custom SQL?'])	

#Metadata API query#
query = '''
{
    workbooks {
        name
        embeddedDatasources {
            name
            hasExtracts
            upstreamTables { 
                name
                connectionType
                isEmbedded
                database {
                    name
                }
                referencedByQueries {
                    query
                }
            }
        }
        upstreamDatasources {
            name
            hasExtracts
            upstreamTables { 
                name
                connectionType
                isEmbedded
                database {
                    name
                }
                referencedByQueries {
                    query
                }
            }
        }
    }
}
'''
#Creating a TSC object#
tableau_auth = TSC.TableauAuth(USERNAME, PASSWORD, SITE_NAME)
server = TSC.Server(SERVER_NAME)
server.version = '3.5'

#Signing into REST API and executing the metadata query#
with server.auth.sign_in(tableau_auth):
    resp = server.metadata.query(query)
    workbooks = resp['data']
    #Traversing through each workbook#
    for workbook in workbooks['workbooks']:
        #Published data-sources get double counted as embedded data-sources#
        embedded_datasources = workbook['embeddedDatasources']
        published_datasources = workbook['upstreamDatasources']
        datasources = published_datasources.copy()
        #Removing duplications#
        for i in range (0, len(embedded_datasources)):
            if embedded_datasources[i]['name'] in [d['name'] for d in published_datasources]:
                pass
            else:
                datasources.append (embedded_datasources[i])
        #Grabbing information at the granularity of each table within each data-source#
        for datasource in datasources:
            if datasource['upstreamTables']:
                for counter in range (0, len(datasource['upstreamTables'])):
                    is_published = 'False'
                    sql_query = ''
                    if datasource in published_datasources:
                        is_published = 'True'
                    workbook_name = workbook['name']
                    datasource_name = datasource['name']
                    has_extract = datasource['hasExtracts']
                    table_name = datasource['upstreamTables'][counter]['name']
                    db_name = datasource['upstreamTables'][counter]['database']['name']
                    connection_type = datasource['upstreamTables'][counter]['connectionType']
                    if datasource['upstreamTables'][counter]['referencedByQueries']:
                        sql_query = datasource['upstreamTables'][counter]['referencedByQueries'][0]['query']
                    #Writing to CSV#
                    table_writer.writerow([workbook_name, datasource_name, 
                                is_published, has_extract, table_name, 
                                db_name, connection_type, sql_query])
