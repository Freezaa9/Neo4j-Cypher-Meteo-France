


CREATE CONSTRAINT ON (v:Ville) ASSERT v.numer_sta IS UNIQUE
CREATE CONSTRAINT ON (m:Mois) ASSERT m.numero IS UNIQUE
CREATE CONSTRAINT ON (a:Annee) ASSERT a.numero IS UNIQUE
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE


LOAD CSV WITH HEADERS FROM 'file:///202011.csv' AS line FIELDTERMINATOR ';'
MERGE  (v:Ville { numer_sta: toString(line.numer_sta)})
CREATE  (r:Releve { t: toInteger(line.t), h: toInteger(line.h)})
MERGE  (m:Mois { numero: substring(line.date, 4, 5)})
MERGE  (a:Annee { numero: left(line.date,3)})
MERGE  (j:Jour { numero: substring(line.date, 6, 7)})
MERGE  (h:Heure { numero: substring(line.date, 8, 9)})
CREATE (r)-[t:depuis]->(v)
CREATE (r)-[w:de_lannee]->(a)
CREATE (r)-[x:du_mois]->(m)
CREATE (r)-[y:le_jour]->(j)
CREATE (r)-[z:a_lheure]->(h)



LOAD CSV WITH HEADERS FROM 'file:///postesSynop.csv' AS line FIELDTERMINATOR ';'
MERGE (v:Ville {NUM_POSTE: line.ID})
ON MATCH SET v.Nom= line.Nom


MERGE (v:Ville {Nom: line.Nom, Latitude: toInteger(line.Latitude), Longitude: toInteger(line.Longitude), Altitude: toInteger(line.Altitude)})
ON CREATE SET v.NUM_POSTE = toString(line.ID) 
ON MATCH SET v.NUM_POSTE = toString(line.ID) 


MATCH (n)
DETACH DELETE n



