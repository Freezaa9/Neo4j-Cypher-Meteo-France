
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

-------------
INSERTION V2
-------------

CREATE CONSTRAINT ON (s:Station) ASSERT s.numer_sta IS UNIQUE;
CREATE CONSTRAINT ON (m:Mois) ASSERT m.numero IS UNIQUE;
CREATE CONSTRAINT ON (a:Annee) ASSERT a.numero IS UNIQUE;
CREATE CONSTRAINT ON (j:Jour) ASSERT j.numero IS UNIQUE;
CREATE CONSTRAINT ON (h:Heure) ASSERT h.numero IS UNIQUE;


:auto USING PERIODIC COMMIT 5000
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
);


LOAD CSV WITH HEADERS FROM 'file:///postesSynop.csv' AS line FIELDTERMINATOR ';'
MERGE (s:Station {numer_sta: line.ID})
SET s.nom = line.Nom
SET s.latitude = toFloat(line.Latitude)
SET s.longitude = toFloat(line.Longitude)
SET s.altitude = toInteger(line.Altitude);

MATCH (n:Station) SET n.habitants = 0;

MATCH (n:Station) SET n.codesInsee = [];

LOAD CSV WITH HEADERS FROM 'file:///communes2020.csv' AS line FIELDTERMINATOR ','
MATCH (s:Station)
WHERE line.ncc IN split(s.nom, '-')  
SET s.codesInsee = s.codesInsee + line.com;

Corrections (import des codes insee plutôt inneficace, mais à défaut d avoir mieux) :

MATCH (s:Station {nom: "PTE DE LA HAGUE"}) SET s.codesInsee = ["50041"];

MATCH (s:Station {nom: "ROUEN-BOOS"}) SET s.codesInsee = ["76116", "76540"];

MATCH (s:Station {nom: "TROYES-BARBEREY"}) SET s.codesInsee = ["10030", "10387"];

MATCH (s:Station {nom: "BELLE ILE-LE TALUT"}) SET s.codesInsee = ["56009"];

MATCH (s:Station {nom: "PTE DE CHASSIRON"}) SET s.codesInsee = ["17323"];

MATCH (s:Station {nom: "LIMOGES-BELLEGARDE"}) SET s.codesInsee = ["87085"];

MATCH (s:Station {nom: "CLERMONT-FD"}) SET s.codesInsee = ["63113"];

MATCH (s:Station {nom: "LE PUY-LOUDES"}) SET s.codesInsee = ["43124", "43157"];

MATCH (s:Station {nom: "BORDEAUX-MERIGNAC"}) SET s.codesInsee = ["33063", "33281"];

MATCH (s:Station {nom: "GOURDON"}) SET s.codesInsee = ["46127"];

MATCH (s:Station {nom: "MONT-DE-MARSAN"}) SET s.codesInsee = ["40192"];

MATCH (s:Station {nom: "ST GIRONS"}) SET s.codesInsee = ["09261"];

MATCH (s:Station {nom: "CAP CEPET"}) SET s.codesInsee = ["83153"];

MATCH (s:Station {nom: "PLOUMANAC'H"}) SET s.codesInsee = ["22168"];



Get toutes les stations qui sont en France Métropolitaine : 

MATCH (s:Station) 
WHERE s.latitude >= 41 AND s.latitude <= 52
AND s.longitude >= -5 AND s.longitude <= 10
RETURN s


Get toutes les stations qui ne sont pas en France Métropolitaine : 

MATCH (s:Station) 
WHERE s.latitude <= 41 OR s.latitude >= 52
OR s.longitude <= -5 OR s.longitude >= 10
RETURN s


Stations qui se trouvent dans la moitié Nord de la France : 

MATCH (s:Station) 
WHERE s.latitude >= 47 AND s.latitude <= 52
AND s.longitude >= -5 AND s.longitude <= 10
RETURN s


Stations qui se trouvent dans la moitié Sud de la France : 

MATCH (s:Station) 
WHERE s.latitude >= 41 AND s.latitude <= 52
AND s.longitude >= -5 AND s.longitude <= 10
RETURN s


Stations qui se trouvent dans la moitié Est de la France : 

MATCH (s:Station) 
WHERE s.latitude >= 41 AND s.latitude <= 52
AND s.longitude >= 2.337 AND s.longitude <= 10
RETURN s


Stations qui se trouvent dans la moitié Ouest de la France : 

MATCH (s:Station) 
WHERE s.latitude >= 41 AND s.latitude <= 52
AND s.longitude >= -5 AND s.longitude <= 2.337
RETURN s


Stations hors France Métropolitaine qui se trouvent dans l hémisphère Sud : 

MATCH (s:Station) 
WHERE (s.latitude <= 41 OR s.latitude >= 52
OR s.longitude <= -5 OR s.longitude >= 10)
AND s.latitude <= 0
RETURN s.nom, s.latitude, s.longitude


Stations hors France Métropolitaine qui se trouvent dans l hémisphère Nord : 

MATCH (s:Station) 
WHERE (s.latitude <= 41 OR s.latitude >= 52
OR s.longitude <= -5 OR s.longitude >= 10)
AND s.latitude >= 0
RETURN s.nom, s.latitude, s.longitude


Pourcentage d habitants par station :

MATCH (s:Station)
MATCH (s2:Station)
RETURN s.nom AS Station, s.habitants AS `Nombre d'habitants`, 100.0*s.habitants/(sum(s2.habitants)) AS `Pourcentage d'habitants de toutes les stations`
ORDER BY `Nombre d'habitants` DESC


Température moyenne par saison : 

MATCH (r:Releve)-[:A_ETE_RELEVE_AU_MOIS]->(m:Mois)
RETURN 
CASE
WHEN 1 <= m.numero <= 3 THEN 'Hiver'
WHEN 4 <= m.numero <= 6 THEN 'Printemps'
WHEN 7 <= m.numero <= 9 THEN 'Eté'
WHEN 10 <= m.numero <= 12 THEN 'Automne'
END AS Saison, 
avg(r.temperature)-273.15 AS `Température moyenne`


Température moyenne par mois : 

MATCH (r:Releve)-[:A_ETE_RELEVE_AU_MOIS]->(m:Mois)
RETURN m.numero AS Mois, avg(r.temperature)-273.15 AS `Température moyenne`
ORDER BY Mois


Température moyenne par année : 

MATCH (r:Releve)-[:A_ETE_RELEVE_A_ANNEE]->(a:Annee)
RETURN a.numero AS Année, avg(r.temperature)-273.15 AS `Température moyenne`
ORDER BY Année


Température moyenne par heure de relevé :

MATCH (r:Releve)-[:A_ETE_RELEVE_A_HEURE]->(h:Heure)
RETURN h.numero AS Heure, avg(r.temperature)-273.15 AS `Température moyenne`
ORDER BY Heure