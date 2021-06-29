# Introduction
# Evaluate RDF Data Citation
# Evaluate RDF Stores
Here, we want to test the rdf_star module of the rdf_data_citation package with different RDF stores. We thereby want 
to find out whether it suffices to use the SPARQL endpoints of each RDF store and eventually credentials to be able to 
perform read and write operations. Each RDF Store vendor offers its own interface, either CLI or GUI, to perform these 
operations. However, we opt for a generic solution by using just one module to connect to different RDF stores. Our 
assumption is that every RDF store should have a get and a post endpoint where queries and write statements 
can be sent via http.

First, download the FHIR RDF data we will be working with from: https://www.hl7.org/fhir/downloads.html --> RDF --> Definitions  
* fhir.ttl (classes and predicates)
* rim.ttl (Resource information model)
* w5.ttl (classes and predicates)
## Apache Jena
Download Apache Jena with fuseki as SPARQL server  from https://jena.apache.org/download/: apache-jena-fuseki-4.1.0.tar.gz (SHA512, PGP)

Extract Apache Jena Fuseki to /opt/apache_jena_fuseki  

Create directory /opt/apache_jena_fuseki/DataCitation_FHIR

Login as superuser:

    sudo -i
Set FUSEKI_HOME and PATH  
    <!-- binaries to start server are in apache_jena_fuseki and the ones to load and query datasets in apache_jena_fuseki/bin -->
    
    export FUSEKI_HOME=/opt/apache_jena_fuseki
    export PATH=$PATH:/opt/apache_jena_fuseki 

Start fuseki server
    
    fuseki-server

Start a second terminal and load files into db with. You do not need to create any database before. 
It will be created by issuing this command:
<!-- export PATH_TO_DATA=/home/filip/Dokumente/Uni/Master/8._Semester/Master_thesis/Research/Evaluation/orig_data/fhir.rdf.ttl -->
    export PATH=$PATH:/opt/apache_jena_fuseki/bin
    export PATH_TO_DATA=/path/to/data
    s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/fhir.ttl 
    s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/rim.ttl 
    s-put http://localhost:3030/DataCitation_FHIR default $PATH_TO_DATA/w5.ttl  

Alternatively, use fusaki's GUI by calling http://localhost:3030 in the browser and create a 
dataset "DataCitation_FHIR". Then upload fhir.ttl, rim.ttl and w5.ttl
Execute simple_query.txt to check whether data has been loaded, either with fusaki's GUI or via CLI:

    s-query --service=http://localhost:3030/DataCitation_FHIR/sparq --query=Experiment/FHIR/simple_query.txt

Service endpoints:
* read: http://localhost:3030/DataCitation_FHIR/sparql
* write: http://localhost:3030/DataCitation_FHIR/update
https://jena.apache.org/documentation/fuseki2/soh.html
  
### Conclusion
No authentication is required and both - read and write - statements are possible using the configured service 
endpoints. 

## GraphDB

## Stardog
Sign up for stardog to get the license-key: https://www.stardog.com/get-started/  
To run stardog on linux follow section "1. Download and Install Stardog" on: https://www.stardog.com/blog/a-stardog-app-in-5-easy-steps  
Extract stardog-latest.zip to /opt/stardog instead of /data/stardog  

    export STARDOG_HOME=/opt/stardog  
Copy stardog-license-bin.bin into /opt/stardog/bin  
Execute following command from opt/stardog/bin to start the server:  

     sudo ./stardog-admin server start

Sign up for https://www.stardog.com/studio/ with a business or university e-mail address  
Access stardog studio via https://stardog.studio/#/  
Create a database "DataCitation_FHIR" and load the FHIR turtle files (fhir.ttl, rim.ttl, w5.ttl) into DataCitation_FHIR 
using Stardog Studio.
To check whether it works, try executing "simple_query.txt" by opening the "Workspace" in Stardog Studio, which 
is accessible through the left vertical menu bar.
Now execute rdfstores.py


Service endpoints:
* read: http://localhost:5820/DataCitation_FHIR/query
* update: http://localhost:5820/DataCitation_FHIR/update

### Conclusion
The endpoints seem to be the right ones as there is a response when accessing these endpoints via browser. 
Authentication does not seem to work with SPARQL Wrapper and Stardog
We get following message: Unauthorized: access is denied due to invalid credentials (unauthorized). 
Check the credentials.
