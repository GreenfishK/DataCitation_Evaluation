from rdf_data_citation import query_store as qs
import configparser
import logging

# Read config parameters such as query checksums of the evaluation queries and rdf store endpoints
config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Reset query story
query_store = qs.QueryStore()
query_store._remove(config.get("QUERY", "simple_query_fhir_checksum"))
query_store._remove(config.get("QUERY", "complex_query_fhir_checksum"))
query_store._remove(config.get("QUERY", "simple_query_wiki_checksum"))
query_store._remove(config.get("QUERY", "complex_query_wiki_checksum"))

# Single evaluations
#evaluate('timestamped_insert', 'small', 'mem_sav', 'simple_query', 'cite_query')
#evaluate('timestamped_insert', 'small', 'mem_sav', 'complex_query', 'cite_query')
#evaluate('timestamped_insert', 'big', 'mem_sav', 'simple_query', 'cite_query')
#evaluate('timestamped_insert', 'big', 'mem_sav', 'complex_query', 'cite_query')