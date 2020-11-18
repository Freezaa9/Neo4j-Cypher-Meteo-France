


CREATE CONSTRAINT ON (v:Ville) ASSERT v.NUM_POSTE IS UNIQUE
CREATE CONSTRAINT ON (m:Mois) ASSERT m.MOIS IS UNIQUE
CREATE CONSTRAINT ON (a:Annee) ASSERT a.ANNEE IS UNIQUE


LOAD CSV WITH HEADERS FROM 'file:///climat2019TO2020.csv' AS line FIELDTERMINATOR ','
MERGE  (v:Ville { NUM_POSTE: toString(line.NUM_POSTE)})
CREATE  (r:Releve { PSTATM: toInteger(line.PSTATM), PMERM: toInteger(line.PMERM)})
MERGE  (m:Mois { MOIS: line.MOIS})
MERGE  (a:Annee { ANNEE: line.ANNEE})
CREATE (r)-[w:depuis]->(v)
CREATE (r)-[x:de_lannee]->(a)
CREATE (r)-[c:du_mois]->(m)

LOAD CSV WITH HEADERS FROM 'file:///postesSynop.csv' AS line FIELDTERMINATOR ';'
MERGE (v:Ville {NUM_POSTE: line.ID})
ON MATCH SET v.Nom= line.Nom


MERGE (v:Ville {Nom: line.Nom, Latitude: toInteger(line.Latitude), Longitude: toInteger(line.Longitude), Altitude: toInteger(line.Altitude)})
ON CREATE SET v.NUM_POSTE = toString(line.ID) 
ON MATCH SET v.NUM_POSTE = toString(line.ID) 


MATCH (n)
DETACH DELETE n


