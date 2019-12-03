import argparse
import gzip
import sys
import time

from scripts.filter_items import filter_items
from scripts.utils import create_connection


class DBSetup:

    def __init__(self):
        self.args = self.parse_commandline()

    @staticmethod
    def parse_commandline():
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

        parser.add_argument("--review_file", type=str, help="This is a path to an input file (.json.gz) with reviews.")
        parser.add_argument("--meta_file", type=str, help="This is a path to an input file (.json.gz) with metadata.")
        return parser.parse_args()

    def run(self):
        print("Setting up a database for data...")
        time.sleep(2)

        connection = create_connection()

        # prepare tables for DB
        with open('db_tables.sql') as f:
            for sql_command in f.read().split(';'):
                connection.execute(sql_command)

        count = 0

        # if review file was input, parse review into DB
        if self.args.review_file:
            print("Starting to parse a file at '{}'...".format(self.args.review_file))
            time.sleep(2)

            # for each review in the file, get data
            for review in self.parse(self.args.review_file):
                count += 1

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

                print("parsing review for an item '{}'".format(item_id))

                # insert parsed data into DB tables
                connection.execute("INSERT OR IGNORE INTO user(id, name) "
                                   "VALUES (?, ?)", [user_id, user_name])

                cursor = connection.execute("INSERT INTO review(userId, itemId, rating, reviewTime) "
                                            "VALUES (?, ?, ?, ?)", [user_id, item_id, item_rating, review_time])
                review_id = cursor.lastrowid
                cursor.close()

                connection.execute("INSERT INTO user_review_list(userId, reviewId) "
                                   "VALUES (?, ?)", [user_id, review_id])

            print("--------------------------------------")
            print("Parsing of reviews was SUCCESSFUL!")
            print("Reviews parsed: {}".format(count))
            print("--------------------------------------")
            time.sleep(2)

        # if meta file was input, parse items' metadata into DB
        if self.args.meta_file:
            print("Starting to parse a file at '{}'...".format(self.args.meta_file))
            time.sleep(2)

            # dictionary for inserted_categories in format {namespace: id}
            inserted_categories = {}

            # for each item in the file, get data
            for item in self.parse(self.args.meta_file):
                count += 1

                # let every 1000th item user know that the script is running
                if count % 1000 == 0:
                    if count % 10000 == 0:
                        print(count)
                    else:
                        print(".", end="")
                    sys.stdout.flush()

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

                except NameError:
                    print("ERROR: Following item is missing at least one of the required attributes"
                          "(asin):")
                    print(item)
                    raise

                # insert parsed data into DB
                connection.execute("INSERT INTO item(id, title, description, price, imageUrl, "
                                   "salesCategory, salesRank) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                   [item_id, item_title, item_description, item_price, item_image_url,
                                    sales_category, sales_rank])

            connection.commit()

            print("")
            print("--------------------------------------")
            print("Parsing of all products was SUCCESSFUL!")
            print("Products parsed: {}".format(count))
            print("--------------------------------------")
            time.sleep(2)

            # filter out items without reviews
            filter_items()

            print("Starting to parse 'related' and 'categories' for filtered products...")
            time.sleep(2)

            # get filtered out items
            cursor = connection.execute("SELECT id FROM item")
            filtered_items = [item[0] for item in cursor.fetchall()]
            cursor.close()

            j = 0

            # loop the file again and for each filtered item parse rest of metadata
            for item in self.parse(self.args.meta_file):

                item_id = item.get("asin")

                # continue if item was filtered out
                if item_id not in filtered_items:
                    continue
                else:
                    j += 1

                categories = item.get("categories", None)
                related = item.get("related", None)

                print("parsing data for an item '{}'".format(item_id))

                if related is not None:
                    for key in related.keys():
                        for related_item_id in related.get(key):
                            connection.execute("INSERT INTO item_related_list(itemId, relatedItemId, relation) "
                                               "VALUES (?, ?, ?)", [item_id, related_item_id, key])

                if categories is not None:
                    for category_hierarchy_array in categories:
                        namespace = ''
                        previous_category_id = None
                        for category in category_hierarchy_array:
                            namespace += '.' + category
                            if namespace in inserted_categories:
                                previous_category_id = inserted_categories[namespace]
                            else:
                                cursor = connection.execute("INSERT INTO category(parentCategoryId, namespace, name) "
                                                            "VALUES(?, ?, ?)",
                                                            [previous_category_id, namespace, category])
                                inserted_id = cursor.lastrowid
                                previous_category_id = inserted_id
                                inserted_categories[namespace] = inserted_id
                                cursor.close()

                        connection.execute("INSERT INTO item_category_list(itemId, categoryId) "
                                           "VALUES (?, ?)", [item_id, previous_category_id])

            print("--------------------------------------")
            print("Parsing of extra data was SUCCESSFUL!")
            print("Products parsed: {}".format(j))
            print("--------------------------------------")
            time.sleep(2)

        connection.commit()
        connection.close()

    def parse(self, file_path):
        g = gzip.open(file_path, 'r')

        for l in g:
            yield eval(l)


if __name__ == "__main__":
    db_setup = DBSetup()
    db_setup.run()
