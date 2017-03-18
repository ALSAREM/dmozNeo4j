# dmozNeo4j

## Death of Dmoz
After announcing the death (officialy) of Dmoz, I created this repositery to share Dmoz Data in CSV format importable directly to Neo4j graph database.

## Data
- The data came from the Dmoz website as RDF (not standard). 
- So I transformed them into CSV files, as next:
  - Split the structure file (900M) to smaller files (parts 1..9)
  - Transform structure files (parts) from rdf to csv using Dmoz module [dmoz.structure2CSV]
  - Transform content file (1.9G) from rdf to csv using Dmoz module [dmoz.contentPages2CSV]
  - Put CSV file in neo4j-data/import
  - 

- These CSV files are importable by Neo4j (see next).

## Neo4j Import
- You can import CSV files into neo4j using "Load CSV" [see http://neo4j.com/docs/developer-manual/current/cypher/clauses/load-csv/] 
- The used queries can be found in *dmozImportQueries*
- You can run the imort query using functions in [importDmozCSVinKG]



## Install
- There are no real install, all you need is to copy the csv files and run the Node.Js function (which is optional) to import them.
- Don't forget to Create indexes:
  - 
     `ON :DmozEditor(id)         ONLINE     (for uniqueness constraint)`
     
     `ON :DmozLang(id)           ONLINE     (for uniqueness constraint) `
     
      `ON :DmozPage(title)        ONLINE `                             
         
      `ON :DmozPage(description)  ONLINE  `  
                                         
      ` ON :DmozPage(url)          ONLINE  ` 
                                    
      ` ON :DmozPage(topic)        ONLINE  ` 
                                    
      ` ON :DmozPage(stemd)        ONLINE  `
                                     
      ` ON :DmozPage(topTerms)     ONLINE  `                                     
                                 
      ` ON :DmozPage(topStems)     ONLINE  `  
                                   
      ` ON :DmozPage(id)           ONLINE     (for uniqueness constraint) `
      
      ` ON :DmozTopic(name)        ONLINE       `     
                           
      ` ON :DmozTopic(fullName)    ONLINE   `        
                            
      ` ON :DmozTopic(description) ONLINE  `          
                           
      ` ON :DmozTopic(title)       ONLINE  `     
                                
      ` ON :DmozTopic(stemd)       ONLINE      `      
                           
      ` ON :DmozTopic(topTerms)    ONLINE   `        
                            
      ` ON :DmozTopic(topStems)    POPULATING       `       
                     
      ` ON :DmozTopic(id)          ONLINE     (for uniqueness constraint) `
      
- 
