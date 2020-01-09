use bd_gdelt;

LOAD DATA LOCAL INFILE '/home/arek/Documents/Mgr/big-data/gdelt-big-data/setup/docker/db/color_metadata.csv' INTO TABLE color_metadata
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n';

