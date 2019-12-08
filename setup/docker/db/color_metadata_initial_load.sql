use bd_gdelt;

/*DELIMITER $$
CREATE OR REPLACE PROCEDURE color_metadata_initial_load()
BEGIN
    DECLARE i int DEFAULT 0;
    DECLARE j int DEFAULT 0;
    DECLARE k int DEFAULT 0;

    WHILE i <= 255 DO
        SET j = 0;
        WHILE j <= 255 DO
            SET k = 0;
            WHILE k <= 255 DO
                INSERT INTO color_metadata (r, g, b) VALUES (i, j, k);
                SET k = k + 1;
            END WHILE;
            SET j = j + 1;
        END WHILE;
        SET i = i + 1;
    END WHILE;
END $$
DELIMITER ;

CALL color_metadata_initial_load();*/

LOAD DATA LOCAL INFILE '/home/arek/Documents/Mgr/big-data/gdelt-big-data/setup/docker/db/color_metadata.csv' INTO TABLE color_metadata
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n';

