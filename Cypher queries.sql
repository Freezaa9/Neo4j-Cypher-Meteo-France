


CREATE CONSTRAINT ON (s:Station) ASSERT s.numer_sta IS UNIQUE;
CREATE CONSTRAINT ON (m:Mois) ASSERT m.numero IS UNIQUE;
CREATE CONSTRAINT ON (a:Annee) ASSERT a.numero IS UNIQUE;
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE;
CREATE CONSTRAINT ON (h:Heure) ASSERT h.numero IS UNIQUE;


LOAD CSV WITH HEADERS FROM 'file:///202011.csv' AS line FIELDTERMINATOR ';'
MERGE  (v:Ville { numer_sta: toString(line.numer_sta)})
CREATE  (r:Releve { t: toInteger(line.t), u: toInteger(line.u)})
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
SET v.Latitude = toString(line.Latitude)
SET v.Longitude = toString(line.Longitude)
SET v.Altitude = toString(line.Altitude)




MATCH (n)
DETACH DELETE n


Insertion v2

:auto USING PERIODIC COMMIT 2000
LOAD CSV WITH HEADERS FROM 'file:///merged_data.csv' 
AS ligne FIELDTERMINATOR ';'

WITH left(ligne.date, 4) AS annee, 
right(left(ligne.date, 10), 2) AS heure, 
right(left(ligne.date, 8), 2) AS jour,
right(left(ligne.date, 6), 2) AS mois,
ligne.numer_sta AS numer_sta,
toFloat(ligne.u) AS humidite,
toFloat(ligne.t) AS temperature

FOREACH(ignoreMe IN CASE WHEN humidite IS NOT NULL
AND temperature IS NOT NULL
THEN [1] ELSE [] END |

MERGE (a:Annee {numero: toInteger(annee)})
MERGE (m:Mois {numero: toInteger(mois)})
MERGE (j:Jour {numero : toInteger(jour)})
MERGE (h:Heure {numero: toInteger(heure)})

MERGE (s:Station {numer_sta: numer_sta})

CREATE (r:Releve {temperature: temperature, 
humidite: humidite})

CREATE (r)-[:A_ETE_RELEVE_A_ANNEE]->(a)
CREATE (r)-[:A_ETE_RELEVE_AU_MOIS]->(m)
CREATE (r)-[:A_ETE_RELEVE_AU_JOUR]->(j)
CREATE (r)-[:A_ETE_RELEVE_A_HEURE]->(h)
CREATE (r)-[:A_ETE_RELEVE_A_STATION]->(s)
)


LOAD CSV WITH HEADERS FROM 'file:///postesSynop.csv' AS line FIELDTERMINATOR ';'
MERGE (s:Station {numer_sta: line.ID})
SET s.ville = line.Nom
SET s.latitude = toFloat(line.Latitude)
SET s.longitude = toFloat(line.Longitude)
SET s.altitude = toInteger(line.Altitude)