import argparse
import gzip
import os
import sqlite3
import subprocess
import sys


class DBSetup:
    sqlite_db = "data/amazon_product_data.db"

    def __init__(self):
        self.args = self.parse_commandline()

    @staticmethod
    def parse_commandline():
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("--review_file", type=str, help="This is a path to an input file (.json.gz) with reviews.")
        parser.add_argument("--meta_file", type=str, help="This is a path to an input file (.json.gz) with metadata.")

        return parser.parse_args()

    def run(self):
        print("Setting up a database for data ...")

        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()

        with open('db_tables.sql') as f:
            for sql_command in f.read().split(';'):
                connection.execute(sql_command)

        i = 1
        if self.args.review_file:
            print("Parsing a file at ", self.args.review_file, "...")

            for review in self.parse(self.args.review_file):
                try:
                    user_id = review.get("reviewerID")
                    user_name = review.get("reviewerName")
                    item_id = review.get("asin")
                    item_rating = review.get("overall")

                    review_time = review.get("unixReviewTime", None)

                except NameError:
                    print("ERROR: Following review is missing at least one of the required attributes"
                          "(reviewerID, reviewerName, asin, overall, unixReviewTime):")
                    print(review)
                    raise

                cursor.execute("INSERT OR IGNORE INTO user(id, name) VALUES (?, ?)",
                               [user_id, user_name])
                cursor.execute("INSERT INTO review(userId, itemId, rating, reviewTime) VALUES (?, ?, ?, ?)",
                               [user_id, item_id, item_rating, review_time])
                review_id = cursor.lastrowid
                cursor.execute("INSERT INTO user_review_list(userId, reviewId) VALUES (?, ?)",
                               [user_id, review_id])

                print("Review n.", i, " parsed: reviewId, userId, itemId - ",
                      ", ".join([str(review_id), user_id, item_id]))
                i += 1

            print(i, " reviews parsed.")

        if self.args.meta_file:
            print("Parsing a file at ", self.args.meta_file, "...")

            # inserted_categories - dictionary in format {namespace: id}
            inserted_categories = {}
            for item in self.parse(self.args.meta_file):
                try:
                    item_id = item.get("asin")

                    item_title = item.get("title", None)
                    item_description = item.get("description", None)
                    item_price = item.get("price", None)
                    item_image_url = item.get("imUrl", None)

                    sales = item.get("salesRank", None)
                    sales_category = None
                    sales_rank = None

                    if sales is not None:
                        for key, value in sales.items():
                            sales_category, sales_rank = key, value

                    categories = item.get("categories", None)
                    related = item.get("related", None)

                except NameError:
                    print("ERROR: Following item is missing at least one of the required attributes"
                          "(asin):")
                    print(item)
                    raise

                cursor.execute("INSERT INTO item(id, title, description, price, imageUrl, salesCategory, salesRank) "
                               "VALUES (?, ?, ?, ?, ?, ?, ?)",
                               [item_id, item_title, item_description, item_price, item_image_url,
                                sales_category, sales_rank])

                if related is not None:
                    for key in related.keys():
                        for related_item_id in related.get(key):
                            cursor.execute(
                                "INSERT INTO item_related_list(itemId, relatedItemId, relation) VALUES (?, ?, ?)",
                                [item_id, related_item_id, key])

                if categories is not None:
                    for category_hierarchy_array in categories:
                        namespace = ''
                        previous_category_id = None
                        for category in category_hierarchy_array:
                            namespace += '.' + category
                            if namespace in inserted_categories:
                                previous_category_id = inserted_categories[namespace]
                            else:
                                cursor.execute("INSERT INTO category(parentCategoryId, namespace, name) "
                                               "VALUES(?, ?, ?)", [previous_category_id, namespace, category])
                                inserted_id = cursor.lastrowid
                                previous_category_id = inserted_id
                                inserted_categories[namespace] = inserted_id

                        cursor.execute("INSERT INTO item_category_list(itemId, categoryId) VALUES (?, ?)",
                                       [item_id, previous_category_id])

                print("Item n.", i, " parsed: itemId - ", item_id)
                i += 1

            print(i, " items parsed.")

        connection.commit()
        connection.close()

    def parse(self, file_path):
        g = gzip.open(file_path, 'r')

        for l in g:
            yield eval(l)


if __name__ == "__main__":
    db_setup = DBSetup()
    db_setup.run()
