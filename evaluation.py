from rdf_data_citation import citation as ct
from rdf_data_citation import citation_utils as ct_ut
from rdf_data_citation import rdf_star as rdf
from rdf_data_citation.rdf_star import VersioningMode
from rdf_data_citation import query_store as qs
import configparser
import logging
import timeit

config = configparser.ConfigParser()
config.read('config.ini')
logging.getLogger().setLevel(int(config.get('LOGGING', 'level')))

# Init versioning
rdf_engine = rdf.TripleStoreEngine(config.get('RDFSTORE_FHIR', 'get'), config.get('RDFSTORE_FHIR', 'post'))
rdf_engine.version_all_rows(versioning_mode=VersioningMode.SAVE_MEM)

# FHIR
simple_query_fhir = open("FHIR/simple_query.txt", "r").read()
complex_query_fhir = open("FHIR/simple_query.txt", "r").read()

citation = ct.Citation(config.get('RDFSTORE_FHIR', 'get'), config.get('RDFSTORE_FHIR', 'post'))
metadata = ct_ut.MetaData(identifier="simple_query_fhir_eval_20210602143900",
                          publisher="Filip Kovacevic",
                          resource_type="RDF",
                          publication_year="2021",
                          creator="Filip Kovacevic",
                          title="Simple FHIR query")


time = timeit.timeit(lambda: citation.cite(simple_query_fhir, metadata), number=1)
print("{0} loops, best of {1}: {2} sec per loop".format(1, 1, time))
query_store = qs.QueryStore()
query_store._remove(config.get("QUERY", 'simple_query_fhir_checksum'))
rdf_engine.reset_all_versions()

