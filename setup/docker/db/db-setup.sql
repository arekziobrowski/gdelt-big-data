
-- country
CREATE OR REPLACE TABLE country (
    id CHAR(2) NOT NULL,
    name VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

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
    country_id CHAR(2) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (country_id)
        REFERENCES country (id),
    INDEX (topic),
    INDEX (date_event)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- image
CREATE OR REPLACE TABLE image (
    id INT NOT NULL AUTO_INCREMENT,
    url VARCHAR(255) NOT NULL,
    article_id INT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (article_id)
        REFERENCES article (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- color_metadata
CREATE OR REPLACE TABLE color_metadata (
    id INT NOT NULL AUTO_INCREMENT,
    r DECIMAL(3) NOT NULL,
    g DECIMAL(3) NOT NULL,
    b DECIMAL(3) NOT NULL,
    PRIMARY KEY (id)
    #INDEX (r, g, b) -- should be added after the initial load of the color_metadata table
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- image_metadata
CREATE OR REPLACE TABLE image_metadata (
    count DECIMAL(10) NOT NULL,
    color_metadata_id INT NOT NULL,
    article_id INT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (article_id, color_metadata_id),
    FOREIGN KEY (article_id)
        REFERENCES article (id),
    FOREIGN KEY (color_metadata_id)
        REFERENCES color_metadata (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- article_keyword
CREATE OR REPLACE TABLE article_keyword(
    id INT NOT NULL AUTO_INCREMENT,
    keyword VARCHAR(255) NOT NULL,
    score DECIMAL(9,2) NOT NULL,
    article_id INT NOT NULL,
    load_date TIMESTAMP NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (article_id)
        REFERENCES article (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- country_tone
CREATE OR REPLACE TABLE country_tone(
    id INT NOT NULL AUTO_INCREMENT,
    related_month DATE NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    articles_number INT NOT NULL,
    mood VARCHAR(255),
    last_published_date DATE NOT NULL,
    load_date TIMESTAMP NOT NULL,
    country_id CHAR(2) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (country_id)
        REFERENCES country (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
