import gzip
import operator
from collections import Counter

from scripts.utils import create_connection


def parse(path):
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)


# from the database
def categories_statistics():
    connection = create_connection()
    category_items_counts_cur = connection.execute("SELECT count(itemId) FROM item_category_list GROUP BY categoryId;")
    category_items_counts = [c[0] for c in category_items_counts_cur]
    category_items_counts_sorted = sorted(category_items_counts)
    category_items_counts_statistics = Counter(category_items_counts)
    category_items_counts_statistics_sorted = sorted(category_items_counts_statistics.items())

    with open('../data/statistics/category_products_counts.txt', 'w', encoding='utf-8') as outp:
        outp.write('Postupně pro jednotlivé kategorie počty produktů.\n')
        for c in category_items_counts_sorted:
            outp.write(str(c) + '\n')

    with open('../data/statistics/category_products_counts_statistics.txt', 'w', encoding='utf-8') as outp:
        outp.write('x\ty ... Je y kategorií, které mají právě x produktů.\n')
        for c in category_items_counts_statistics_sorted:
            outp.write(str(c[0]) + '\t' + str(c[1]) + '\n')


# From the file
# Must exist directory data/statistics
def categories_statistics_from_file(cat_file='Toys_and_Games'):
    categories = []
    counts_for_products = []

    for product in parse('../data/meta_' + cat_file + '.json.gz'):
        product_categories = []
        # json_product_categories is array of categories and each category is also array with categories hierarchy
        json_product_categories = product['categories']
        for category_hierarchy_array in json_product_categories:
            category_name = category_hierarchy_array[0]
            for category in category_hierarchy_array[1:]:
                category_name += '.' + category
            product_categories.append(category_name)
        categories += product_categories
        counts_for_products.append(len(product_categories))

    counts_for_categories = Counter(categories)

    counts_for_categories_sorted = sorted(counts_for_categories.items(), key=operator.itemgetter(1))
    counts_for_products_sorted = sorted(counts_for_products)

    counts_for_categories_statistics = Counter(counts_for_categories.values())
    counts_for_products_statistics = Counter(counts_for_products)
    counts_for_categories_statistics_sorted = sorted(counts_for_categories_statistics.items())
    counts_for_products_statistics_sorted = sorted(counts_for_products_statistics.items())

    with open('../data/statistics/' + cat_file + '_categories_counts.txt', 'w', encoding='utf-8') as outp:
        outp.write('Název kategorie a kolik produktů do ní patří.\n')
        for (cat, count) in counts_for_categories_sorted:
            outp.write(cat + '\t' + str(count) + '\n')

    with open('../data/statistics/' + cat_file + '_counts_for_products.txt', 'w', encoding='utf-8') as outp:
        outp.write('Postupně pro jednotlivé produkty počty kategorií, do kterých ten produkt patří.\n')
        for c in counts_for_products_sorted:
            outp.write(str(c) + '\n')

    with open('../data/statistics/' + cat_file + '_categories_counts_statistics.txt', 'w', encoding='utf-8') as outp:
        outp.write('x\ty ... Je y kategorií, do kterých patří právě x produktů.\n')
        for c in counts_for_categories_statistics_sorted:
            outp.write(str(c[0]) + '\t' + str(c[1]) + '\n')

    with open('../data/statistics/' + cat_file + '_counts_for_products_statistics.txt', 'w', encoding='utf-8') as outp:
        outp.write('x\ty ... Je y produktů, které patří právě do x kategorí.\n')
        for c in counts_for_products_statistics_sorted:
            outp.write(str(c[0]) + '\t' + str(c[1]) + '\n')


if __name__ == "__main__":
    categories_statistics()
