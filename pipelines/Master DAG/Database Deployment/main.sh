db=gtd_database
public_ip=34.122.46.36
gsutil cp gs://terrorism-csv-storage-bucket/region.csv .
gsutil cp gs://terrorism-csv-storage-bucket/country.csv .
gsutil cp gs://terrorism-csv-storage-bucket/target.csv .
gsutil cp gs://terrorism-csv-storage-bucket/group.csv .
gsutil cp gs://terrorism-csv-storage-bucket/weapon.csv .
gsutil cp gs://terrorism-csv-storage-bucket/attack.csv .
gsutil cp gs://terrorism-csv-storage-bucket/fatality.csv .
gsutil cp gs://terrorism-csv-storage-bucket/event.csv .


sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "CREATE DATABASE $db;"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "CREATE TABLE $db.region (
	regionid int NOT NULL UNIQUE,
	region VARCHAR(200),
    PRIMARY KEY (regionid));"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "CREATE TABLE $db.country (
countryid int Not null unique,
countrycode INT,
country VARCHAR(50),
provstate VARCHAR(50),
city VARCHAR(50),
PRIMARY KEY (countryid));"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "CREATE TABLE $db.target(
    targetid INT NOT NULL UNIQUE,
    targtype1 INT NOT NULL,
    targtype1_txt VARCHAR(100),
    targsubtype1 INT,	
    targsubtype1_txt VARCHAR(100),
    corp1 VARCHAR(100),
    target1 VARCHAR(100),
    natlty1 INT,
    natlty1_txt VARCHAR(100),
    targtype2 INT,
    targtype2_txt VARCHAR(100),
    targsubtype2 INT,
    targsubtype2_txt VARCHAR(100),
    corp2 TEXT,
    target2 TEXT,
    natlty2 INT,
    natlty2_txt TEXT,
    targtype3 INT,
    targtype3_txt TEXT,
    targsubtype3 INT,
    targsubtype3_txt TEXT,
    corp3 TEXT,
    target3 TEXT,
    natlty3 INT,
    natlty3_txt TEXT,
    countryid INT,
    regionid	INT,
    PRIMARY KEY (targetid),
    FOREIGN KEY (countryid) REFERENCES country(countryid),
    FOREIGN KEY (regionid) REFERENCES region(regionid)
);"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "create table $db.group(
gid INT NOT NULL UNIQUE,
gname VARCHAR(100) DEFAULT 'Unknown',	
gsubname VARCHAR(100) DEFAULT 'Unknown',	
gname2	VARCHAR(100) DEFAULT 'Unknown',
gsubname2 VARCHAR(100) DEFAULT 'Unknown',	
gname3	VARCHAR(100) DEFAULT 'Unknown',
gsubname3 VARCHAR(100) DEFAULT 'Unknown',	
guncertain1	INT,
guncertain2 INT,
guncertain3	INT,
individual	INT,
nperps	INT,
nperpcap INT,	
claimed	INT,
claimmode INT,	
claimmode_txt VARCHAR(100),	
claim2	INT,
claimmode2	INT,
claimmode2_txt	VARCHAR(100),
claim3	INT,
claimmode3	INT,
claimmode3_txt	VARCHAR(100),
compclaim VARCHAR(100),
Constraint g_pk Primary Key(gid)
);"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "Create Table $db.weapon(
weaponid INT NOT NULL UNIQUE,	
weaptype1 INT NOT NULL,	
weaptype1_txt VARCHAR(100) DEFAULT  'Unknown',	
weapsubtype1 INT DEFAULT NULL,	 	
weapsubtype1_txt VARCHAR(100) DEFAULT 'Unknown',		
weaptype2 INT DEFAULT NULL,
weaptype2_txt VARCHAR(100) DEFAULT 'Unknown',	
weapsubtype2 INT DEFAULT NULL,
weapsubtype2_txt VARCHAR(100) DEFAULT 'Unknown',		
weaptype3 	INT DEFAULT NULL,
weaptype3_txt	VARCHAR(100) DEFAULT 'Unknown',	
weapsubtype3	INT DEFAULT NULL,
weapsubtype3_txt VARCHAR(100) DEFAULT 'Unknown',		
weaptype4	INT DEFAULT NULL,
weaptype4_txt	VARCHAR(100) DEFAULT 'Unknown',	
weapsubtype4	INT DEFAULT NULL,
weapsubtype4_txt VARCHAR(100) DEFAULT 'Unknown',		
weapdetail VARCHAR(100) ,	
CONSTRAINT W_PK PRIMARY KEY(weaponid));"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "CREATE TABLE $db.attack(attackid INT NOT NULL Unique,
attacktype VARCHAR(50),
PRIMARY KEY(attackid));"


sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e " CREATE TABLE $db.fatality(
    fatalityid BIGINT(15) NOT NULL UNIQUE,
    nkill INT,
    nkillus INT,
    nkillter INT,
    nwound INT,
    nwoundus INT,
    nwoundte INT,
    property INT,
    propextent INT,
    propextent_txt VARCHAR(20),
    propvalue INT,
    propcomment VARCHAR(15),
    ishostkid INT,
    nhostkid INT,
    nhostkidus INT,
    divert VARCHAR(15),
    kidhijcountry VARCHAR(20),
    ransom INT,
    ransomamt INT,
    ransomamtus INT,
    ransompaid INT,
    ransompaidus INT,
    ransomnote INT,
    hostkidoutcome INT,
    hostkidoutcome_txt VARCHAR(20),
    nreleased INT,
    PRIMARY KEY (fatalityid)
    ) ;"

sudo mysql --local-infile=1 --host=$public_ip --user=root --password=root -e "
CREATE TABLE $db.event (
    eventid BIGINT(12) NOT NULL UNIQUE,
    year INT(4),
    month INT,
    day INT,
    latitude FLOAT,
    longitude FLOAT,
    crit1 INT(1),
    crit2 INT(1),
    crit3 TEXT,
    dbsource TEXT,
    success INT,
    targetid INT,
    gid INT,
    fatalityid BIGINT,
    weaponid INT,
    attackid INT,
    PRIMARY KEY (eventid),
    FOREIGN KEY (targetid) REFERENCES target(targetid),
    FOREIGN KEY (gid) REFERENCES $db.group(gid),
    FOREIGN KEY (fatalityid) REFERENCES fatality(fatalityid),
    FOREIGN KEY (weaponid) REFERENCES weapon(weaponid),
    FOREIGN KEY (attackid) REFERENCES attack(attackid)
);"





sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db region.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db country.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db target.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db group.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db weapon.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db attack.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db fatality.csv
sudo mysqlimport --host=$public_ip --ignore-lines=1  --lines-terminated-by='\n' --fields-terminated-by=',' --default-character-set=utf8 --user=root --password=root --local $db event.csv
