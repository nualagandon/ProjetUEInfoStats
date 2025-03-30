import os
import sqlite3
import csv

def convert_float(value):
    try:
        return(float(value))
    except (ValueError,TypeError):
        return None
    
def connect(nom_base) :
    if os.path.exists("BDprojet.db"):
        os.remove("BDprojet.db")
        
    bd = sqlite3.connect(nom_base)
    curs = bd.cursor()
 
    curs.executescript("""
        CREATE TABLE Communes (
            code_INSEE CHAR(5),
            altitude_med INT,
            latitude FLOAT,
            longitude FLOAT,
            PRIMARY KEY(code_INSEE)
        );

        CREATE TABLE Incendies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commune VARCHAR(40),
            code_INSEE CHAR(5),
            surface_parcouru_m2 INT,
            annee INT,
            mois CHAR(3),
            jour INT,
            heure INT,
            nature_primaire VARCHAR(20),
            nature_secondaire VARCHAR(20) DEFAULT 'NA',
            FOREIGN KEY (code_INSEE) REFERENCES Communes(code_INSEE),
            CHECK(heure BETWEEN 0 AND 23),
            CHECK(jour BETWEEN 1 AND 31)
        );

        CREATE TABLE Donnees_Meteos (
            code_INSEE CHAR(5),
            RR_MED FLOAT,
            NBJRR1_MED FLOAT,
            NBJRR5_MED FLOAT,
            NBJRR10_MED FLOAT,
            Tmin_MED FLOAT,
            Tmax_MED FLOAT,
            tens_vap_MED FLOAT,
            force_vent_MED FLOAT,
            insolation_MED VARCHAR(10) DEFAULT 'NA',
            rayonnement_MED VARCHAR(10) DEFAULT 'NA',
            PRIMARY KEY(code_INSEE),
            FOREIGN KEY(code_INSEE) REFERENCES Communes(code_INSEE),
            CHECK(RR_MED >= 0),
            CHECK(NBJRR1_MED >= 0),
            CHECK(NBJRR5_MED >= 0),
            CHECK(NBJRR10_MED >= 0),
            CHECK(tens_vap_MED >= 0),
            CHECK(force_vent_MED >= 0)
        );
    """)
    bd.commit()
    return bd, curs



db, curs = connect("BDprojet.db")

with open("donnees_geo.csv") as file :
    reader_communes = csv.DictReader(file)
    for row in reader_communes:
        code_insee = row['code_INSEE']
        latitude = float(row['latitude'])
        longitude = float(row['longitude'])
        altitude = int(row['altitude_med'])

        curs.execute("""INSERT INTO Communes(code_INSEE, latitude, longitude, altitude_med) VALUES (?, ?, ?, ?)""",
             (code_insee, latitude, longitude, altitude))
        db.commit()

with open("donnees_incendies.csv") as file:
    reader_incendies = csv.DictReader(file)
    for row in reader_incendies:
        commune = row['commune']
        code_insee = row['code_INSEE']
        surface_parcourue = int(row['surface_parcourue_m2'])
        annee = int(row['annee'])
        mois = row['mois']
        jour = int(row['jour'])
        heure = int(row['heure'])
        nature_inc_prim = row['nature_inc_prim']
        nature_inc_sec = row['nature_inc_sec']

        curs.execute("""INSERT INTO Incendies(commune,code_INSEE,surface_parcouru_m2,annee,mois,jour,heure,nature_primaire,nature_secondaire) VALUES (?,?,?,?,?,?,?,?,?)""",
                     (commune,code_insee,surface_parcourue,annee,mois,jour,heure,nature_inc_prim,nature_inc_sec))
        db.commit()

with open("donnees_meteo.csv") as file:
    reader_meteo = csv.DictReader(file)
    for row in reader_meteo:
        code_insee = row['Code.INSEE']
        rr = convert_float(row['RR_med'])
        nbjrr1 = convert_float(row['NBJRR1_med'])
        nbjrr5 = convert_float(row['NBJRR5_med'])
        nbjrr10 = convert_float(row['NBJRR10_med'])
        tmin =convert_float(row['Tmin_med'])
        tmax = convert_float(row['Tmax_med'])
        tens_vap = convert_float(row['Tens_vap_med'])
        force_vent = convert_float(row['Force_vent_med'])
        insolation = row['Insolation_med']
        rayonnement = row['Rayonnement_med']

        curs.execute("""INSERT INTO Donnees_Meteos(code_INSEE,RR_MED,NBJRR1_MED,NBJRR5_MED,NBJRR10_MED,Tmin_MED,Tmax_MED,tens_vap_MED,force_vent_MED,insolation_MED,rayonnement_MED) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                     (code_insee,rr,nbjrr1,nbjrr5,nbjrr10,tmin,tmax,tens_vap,force_vent,insolation,rayonnement))
        db.commit()


with open('nature_saison.csv', mode='w',newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['id','code_INSEE','saison','nature_incendie'])
    curs.execute(""" SELECT 
                 id,
                 code_INSEE,
                 CASE
                    WHEN mois IN ('Mar', 'Apr', 'May') THEN 'Printemps'
                    WHEN mois IN ('Jun', 'Jul', 'Aug') THEN 'Ete'
                    WHEN mois IN ('Sep', 'Oct', 'Nov') THEN 'Automne'
                    ELSE 'Hiver'
                 END AS saison,
                 nature_primaire
                 FROM Incendies 
    """)
    tout = curs.fetchall()
    curs.close()
    writer.writerows(tout)