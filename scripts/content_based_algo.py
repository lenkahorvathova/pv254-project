from sqlite3 import Connection

from scripts.utils import create_connection


class Item:
    connection: Connection
    id: str
    title: str
    image_url: str
    overall_rating: float
    related: iter

    def set_properties(self, id: str, title: str, image_url: str, overall_rating: float):
        self.id = id
        self.title = title
        self.image_url = image_url
        self.overall_rating = overall_rating

    # items_set: set of <Item>s - Restriction of related items to the given set.
    def get_related(self, items_set: iter = None):
        if self.related is None:
            cursor = self.connection.cursor()
            if items_set is None:
                cursor.execute("SELECT relatedItemId FROM item_related_list WHERE itemId = ?;", [self.id])
            else:
                cursor.execute(
                    "SELECT relatedItemId FROM item_related_list WHERE itemId = ? AND relatedItemId IN ({});".format(
                        ",".join(['"{}"'.format(item.id) for item in items_set])), [self.id])
            self.related = {item[0] for item in cursor.fetchall()}

        return self.related


class SimilarityRecommender:

    connection: Connection
    item_id: str
    # {id -> Item}
    similar_items: dict
    # {id -> id -> float}
    similarity_matrix: dict

    def __init__(self, connection: Connection, item_id: str):
        self.connection = connection
        self.item_id = item_id

    # find_similar_items_callback - Take one parameter item_id: str. Return dict in format {id -> Item}.
    # count_similarities_callback - Take one parameter similar_items: dict - result of the function above. Return
    # Two-dimensional dictionary, keys on both first and second level are the ids of items from similar_items.
    # Value on index [x][y] and [y][x] is the same, it is float and corresponds to similarity of items x and y. Value
    # [x][x] is not defined.
    #
    # Based on given callbacks find similar items and count similarities between them, and then find the given number
    # of them so that they are as different from each other as possible(*) (* Not really as it is an NP-complete
    # problem, but approximately.)
    # Return ordered list of recommended <Item>s.
    def recommend_products(self, count: int, find_similar_items_callback: callable,
                           count_similarities_callback: callable) -> list:
        self.similar_items = find_similar_items_callback(self.item_id)
        if count > len(self.similar_items):
            raise Exception('There is only ' + str(
                len(self.similar_items)) + ' similar product(s) returned by algorithm, can not recommend ' + str(count))
        self.similarity_matrix = count_similarities_callback(self.similar_items)

        return self.get_diverse_recommenations(count)

    # Do not call directly
    def get_diverse_recommenations(self, count) -> list:
        # Find the best product as initial
        best_item_id = None
        best = -1
        for item_id, item in self.similar_items.items():
            if item.overall_rating > best:
                best_item_id = item.id
                best = item.overall_rating

        selected_ids = {best_item_id}
        self.similarity_matrix.pop(best_item_id)

        for i in range(0, count - 1):
            less_similar_item_id = None
            less_similarity = float('inf')
            for v in self.similarity_matrix:
                # # V1
                # similarity = 0
                # for sel in selected_ids:
                #     similarity += self.similarity_matrix[v][sel]
                # if similarity < less_similarity:
                #     less_similar_item_id = v
                #     less_similarity = similarity
                # # V2
                # max_similarity = 0
                # for sel in selected_ids:
                #     sim = self.similarity_matrix[v][sel]
                #     if sim > max_similarity:
                #         max_similarity = sim
                # if max_similarity < less_similarity:
                #     less_similar_item_id = v
                #     less_similarity = max_similarity
                # V3
                squared_similarity = 0
                for sel in selected_ids:
                    squared_similarity += (self.similarity_matrix[v][sel]) ** 2
                if squared_similarity < less_similarity:
                    less_similar_item_id = v
                    less_similarity = squared_similarity

            selected_ids.add(less_similar_item_id)
            # print(less_similar_item_id)
            self.similarity_matrix.pop(less_similar_item_id)

        recommended_items = [self.similar_items[item_id] for item_id in selected_ids]

        def sort_func(item: Item) -> float:
            return item.overall_rating
        recommended_items.sort(key=sort_func, reverse=True)

        return recommended_items


# def add_related_items(connection, item_id: str, items: set) -> set:
#     items_cur = connection.execute("SELECT relatedItemId FROM item_related_list WHERE itemId = ?;", [item_id])
#     for item in items_cur.fetchall():
#         items.add(item[0])
#
#     return items


def find_similar_items_test(item_id: str) -> dict:
    item1 = Item()
    item2 = Item()
    item3 = Item()
    item4 = Item()
    item5 = Item()
    item6 = Item()
    item7 = Item()
    item8 = Item()
    item9 = Item()
    item10 = Item()
    item1.set_properties('1', 'Item 1', '', 4)
    item2.set_properties('2', 'Item 2', '', 4.3)
    item3.set_properties('3', 'Item 3', '', 3)
    item4.set_properties('4', 'Item 4', '', 4.9)
    item5.set_properties('5', 'Item 5', '', 3.1)
    item6.set_properties('6', 'Item 6', '', 5)
    item7.set_properties('7', 'Item 7', '', 4.5)
    item8.set_properties('8', 'Item 8', '', 4)
    item9.set_properties('9', 'Item 9', '', 2.6)
    item10.set_properties('10', 'Item 10', '', 3.8)
    items = {'1': item1, '2': item2, '3': item3, '4': item4, '5': item5, '6': item6, '7': item7, '8': item8, '9': item9, '10': item10}
    return items


def count_similarities_test(similar_items: dict) -> dict:
    similarity_matrix = {
        '1': {
            '2': 3, '3': 4, '4': 0, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '2': {
            '1': 3, '3': 6, '4': 1, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '3': {
            '1': 4, '2': 6, '4': 1, '5': 0, '6': 0, '7': 0, '8': 0, '9': 0, '10': 0
        },
        '4': {
            '1': 0, '2': 1, '3': 1, '5': 10, '6': 10, '7': 2, '8': 3, '9': 2, '10': 3
        },
        '5': {
            '1': 0, '2': 0, '3': 0, '4': 10, '6': 10, '7': 7, '8': 3, '9': 1, '10': 3
        },
        '6': {
            '1': 0, '2': 0, '3': 0, '4': 10, '5': 10, '7': 7, '8': 6, '9': 3, '10': 4
        },
        '7': {
            '1': 0, '2': 0, '3': 0, '4': 2, '5': 7, '6': 7, '8': 1, '9': 0, '10': 1
        },
        '8': {
            '1': 0, '2': 0, '3': 0, '4': 3, '5': 3, '6': 6, '7': 1, '9': 9, '10': 9
        },
        '9': {
            '1': 0, '2': 0, '3': 0, '4': 2, '5': 1, '6': 3, '7': 0, '8': 9, '10': 7
        },
        '10': {
            '1': 0, '2': 0, '3': 0, '4': 3, '5': 3, '6': 4, '7': 1, '8': 9, '9': 7
        },
    }
    return similarity_matrix


if __name__ == "__main__":
    product_id = ''
    connection = create_connection()
    similarity_recommender = SimilarityRecommender(connection, product_id)
    recommended = similarity_recommender.recommend_products(10, find_similar_items_test, count_similarities_test)
    print()
    print("----result----")
    for product in recommended:
        print(product.id)
