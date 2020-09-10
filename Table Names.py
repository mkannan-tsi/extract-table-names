import tableauserverclient as TSC
import csv

#Server Details#
SERVER_NAME = ""
SITE_NAME = ""
USERNAME = ""
PASSWORD = ""
#Creating a TSC object#
tableau_auth = TSC.TableauAuth(USERNAME, PASSWORD, SITE_NAME)
server = TSC.Server(SERVER_NAME)
server.version = '3.5'
#Initializing list elements for writing into CSV later#
datasource_list, workbook_list, tableName_list, connectionType_list = ([] for i in range(4))
sqlQuery_list, extract_list, publishedDatasource_list, dbName_list = ([] for i in range(4))

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
                    if datasource in published_datasources:
                        publishedDatasource_list.append ('True')
                    else:
                        publishedDatasource_list.append ('False')
                    workbook_list.append (workbook['name'])
                    datasource_list.append (datasource['name'])
                    extract_list.append (datasource['hasExtracts'])
                    tableName_list.append (datasource['upstreamTables'][counter]['name'])
                    dbName_list.append (datasource['upstreamTables'][counter]['database']['name'])
                    connectionType_list.append (datasource['upstreamTables'][counter]['connectionType'])
                    if datasource['upstreamTables'][counter]['referencedByQueries']:
                        sqlQuery_list.append (datasource['upstreamTables'][counter]['referencedByQueries'][0]['query'])
                    else:
                        sqlQuery_list.append ('')

#Writing all the information into a CSV file#
with open("tables.csv", "w+") as csvfile:
    table_writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    table_writer.writerow(['Workbook Name', 'Datasource Name', 'Published Datasource?', 'Extract?',
                           'Table Name', 'Database Name', 'Connection Type', 'Custom SQL?'])	
    for counter in range (0, len(datasource_list)):
        table_writer.writerow([workbook_list[counter], datasource_list[counter], 
                               publishedDatasource_list[counter], extract_list[counter], 
                               tableName_list[counter], dbName_list[counter],
                               connectionType_list[counter], sqlQuery_list[counter]])
