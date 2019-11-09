CREATE TABLE IF NOT EXISTS user (id TEXT NOT NULL PRIMARY KEY,
                                 name TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS item (id TEXT NOT NULL PRIMARY KEY,
                                 title TEXT,
                                 description TEXT,
                                 price REAL,
                                 imageUrl TEXT,
                                 salesCategory TEXT,
                                 salesRank INTEGER);

CREATE TABLE IF NOT EXISTS review (id INTEGER NOT NULL PRIMARY KEY,
                                   userId TEXT NOT NULL,
                                   itemId TEXT NOT NULL,
                                   rating INTEGER NOT NULL,
                                   reviewTime INTEGER,
                                   FOREIGN KEY (userId) REFERENCES user(id),
                                   FOREIGN KEY (itemId) REFERENCES item(id));

CREATE TABLE IF NOT EXISTS user_review_list (userId TEXT NOT NULL,
											                       reviewId TEXT NOT NULL,
											                       FOREIGN KEY (userId) REFERENCES user(id),
											                       FOREIGN KEY (reviewId) REFERENCES review(id),
											                       UNIQUE(userId, reviewId));

CREATE TABLE IF NOT EXISTS item_related_list (itemId TEXT NOT NULL,
										  	                      relatedItemId TEXT NOT NULL,
										  	                      relation TEXT,
										  	                      FOREIGN KEY (relatedItemId) REFERENCES item(id),
										  	                      FOREIGN KEY (itemId) REFERENCES item(id));

CREATE TABLE IF NOT EXISTS category (id INTEGER NOT NULL PRIMARY KEY,
                                        parentCategoryId INTEGER,
                                        namespace TEXT UNIQUE,
                                        name TEXT NOT NULL,
                                        FOREIGN KEY (parentCategoryId) REFERENCES category(id));

CREATE TABLE IF NOT EXISTS item_category_list (itemId TEXT NOT NULL,
									    categoryId INTEGER NOT NULL,
									    FOREIGN KEY (itemId) REFERENCES item(id),
									    FOREIGN KEY (categoryId) REFERENCES category(id));
