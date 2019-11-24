-- country
CREATE OR REPLACE TABLE country (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);

-- article
CREATE OR REPLACE TABLE article (
    id INT NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    url VARCHAR(255) NOT NULL,
    date_published DATE,
    date_event DATE,
    topic VARCHAR(255),
    language VARCHAR(30),
    tone DECIMAL(9,5),
    load_date TIMESTAMP NOT NULL,
    country_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (country_id)
        REFERENCES country (id)
);

-- image
CREATE OR REPLACE TABLE image (
    id INT NOT NULL AUTO_INCREMENT,
    url VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    article_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (article_id)
        REFERENCES article (id)
);

-- color_metadata
CREATE OR REPLACE TABLE color_metadata (
    id INT NOT NULL AUTO_INCREMENT,
    r DECIMAL(3) NOT NULL,
    g DECIMAL(3) NOT NULL,
    b DECIMAL(3) NOT NULL,
    PRIMARY KEY (id)
);

-- image_metadata
CREATE OR REPLACE TABLE image_metadata (
    count DECIMAL(10) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    image_id INT NOT NULL,
    color_metadata_id INT NOT NULL,
    FOREIGN KEY (image_id)
        REFERENCES image (id),
    FOREIGN KEY (color_metadata_id)
        REFERENCES color_metadata (id)
);

-- article_keyword
CREATE OR REPLACE TABLE article_keyword(
    id INT NOT NULL AUTO_INCREMENT,
    keyword VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    article_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (article_id)
        REFERENCES article (id)
);

-- country_tone
CREATE OR REPLACE TABLE country_tone(
    id INT NOT NULL AUTO_INCREMENT,
    start_date DATE NOT NULL,
    end_date DATE,
    mood VARCHAR(255),
    load_date TIMESTAMP NOT NULL,
    country_id INT NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (country_id)
        REFERENCES country (id)
);
