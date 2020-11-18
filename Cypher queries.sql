


CREATE CONSTRAINT ON (v:Ville) ASSERT v.numer_sta IS UNIQUE
CREATE CONSTRAINT ON (m:Mois) ASSERT m.numero IS UNIQUE
CREATE CONSTRAINT ON (a:Annee) ASSERT a.numero IS UNIQUE
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE


LOAD CSV WITH HEADERS FROM 'file:///202011.csv' AS line FIELDTERMINATOR ';'
MERGE  (v:Ville { numer_sta: toString(line.numer_sta)})
MERGE  (r:Releve { t: toInteger(line.t), u: toInteger(line.u)})
MERGE  (m:Mois { numero: substring(toString(line.date), 4, 2)})
MERGE  (a:Annee { numero: substring(toString(line.date), 0, 4)})
MERGE  (j:Jour { numero: substring(toString(line.date), 6, 2)})
MERGE  (h:Heure { numero: substring(toString(line.date), 8, 2)})
CREATE (r)-[t:depuis]->(v)
CREATE (r)-[w:de_lannee]->(a)
CREATE (r)-[x:du_mois]->(m)
CREATE (r)-[y:le_jour]->(j)
CREATE (r)-[z:a_lheure]->(h)



LOAD CSV WITH HEADERS FROM 'file:///postesSynop.csv' AS line FIELDTERMINATOR ';'
MERGE (v:Ville {numer_sta: line.ID})
SET v.Nom = line.Nom
SET v.Latitude = toInteger(line.Latitude)
SET v.Longitude = toInteger(line.Longitude)
SET v.Altitude = toInteger(line.Altitude)




MATCH (n)
DETACH DELETE n



