from collections import Counter
import operator
from sqlite3 import Connection

from scripts.utils import create_connection

class RelatedItemsExplorer:

    connection: Connection

    def __init__(self):
        self.connection = create_connection()

    def related_statistics(self):
        item_relations_counts_cur = self.connection.execute("SELECT count(item_related_list.relatedItemId) FROM item LEFT JOIN item_related_list ON item.id = item_related_list.itemId GROUP BY item_related_list.itemId;")
        item_relations_counts = [c[0] for c in item_relations_counts_cur]
        item_relations_counts_statistics = Counter(item_relations_counts)
        item_relations_counts_sorted = sorted(item_relations_counts)
        item_relations_counts_statistics_sorted = sorted(item_relations_counts_statistics.items())

        with open('../data/statistics/product_relations_counts.txt', 'w', encoding='utf-8') as outp:
            outp.write('Postupně pro jednotlivé produkty počty jejich related produktů.\n')
            for c in item_relations_counts_sorted:
                outp.write(str(c) + '\n')

        with open('../data/statistics/product_relations_counts_statistics.txt', 'w', encoding='utf-8') as outp:
            outp.write('x\ty ... Je y produktů, které mají právě x related produktů.\n')
            for c in item_relations_counts_statistics_sorted:
                outp.write(str(c[0]) + '\t' + str(c[1]) + '\n')

        relations_counts_cur = self.connection.execute("SELECT relation, count(itemId) FROM item_related_list GROUP BY relation;")
        relations_counts = [c_rel for c_rel in relations_counts_cur]
        relations_counts_sorted = sorted(relations_counts, key=operator.itemgetter(1))

        with open('../data/statistics/relations_counts.txt', 'w', encoding='utf-8') as outp:
            outp.write('Název relace a kolikrát se vyskytuje.\n')
            for (rel, count) in relations_counts_sorted:
                outp.write(rel + '\t' + str(count) + '\n')

    def check_symmetry(self, limit=20, offset=0):

        cursor = self.connection.cursor()

        sample_items_cur = self.connection.execute("SELECT id FROM item DESC LIMIT ? OFFSET ?;",
                                                   [limit, offset])
        sample_items = sample_items_cur.fetchall()

        also_bought_symmetric = 0
        also_bought_antisymmetric = 0
        also_viewed_symmetric = 0
        also_viewed_antisymmetric = 0
        bought_together_symmetric = 0
        bought_together_antisymmetric = 0
        buy_after_viewing_symmetric = 0
        buy_after_viewing_antisymmetric = 0

        rel_count = 0

        for sample_item in sample_items:
            item_id = sample_item[0]
            relations = self.connection.execute("SELECT relatedItemId, relation FROM item_related_list WHERE itemId = ?;",
                                                [item_id])

            for (related_item_id, relation) in relations:
                rel_count += 1

                if relation == 'also_bought':
                    cursor.execute(
                        "SELECT itemId FROM item_related_list WHERE itemId = ? AND relatedItemId = ? AND relation = 'also_bought';",
                        [related_item_id, item_id])
                    inverse = cursor.fetchone()
                    if inverse is None:
                        also_bought_antisymmetric += 1
                    else:
                        also_bought_symmetric += 1

                elif relation == 'also_viewed':
                    cursor.execute(
                        "SELECT itemId FROM item_related_list WHERE itemId = ? AND relatedItemId = ? AND relation = 'also_viewed';",
                        [related_item_id, item_id])
                    inverse = cursor.fetchone()
                    if inverse is None:
                        also_viewed_antisymmetric += 1
                    else:
                        also_viewed_symmetric += 1

                elif relation == 'bought_together':
                    cursor.execute(
                        "SELECT itemId FROM item_related_list WHERE itemId = ? AND relatedItemId = ? AND relation = 'bought_together';",
                        [related_item_id, item_id])
                    inverse = cursor.fetchone()
                    if inverse is None:
                        bought_together_antisymmetric += 1
                    else:
                        bought_together_symmetric += 1

                elif relation == 'buy_after_viewing':
                    cursor.execute(
                        "SELECT itemId FROM item_related_list WHERE itemId = ? AND relatedItemId = ? AND relation = 'buy_after_viewing';",
                        [related_item_id, item_id])
                    inverse = cursor.fetchone()
                    if inverse is None:
                        buy_after_viewing_antisymmetric += 1
                    else:
                        buy_after_viewing_symmetric += 1

        self.connection.close()

        print('Number of relations: ' + str(rel_count))
        print('Also bought:')
        print('\tsymmetric: ' + str(also_bought_symmetric))
        print('\tantisymmetric: ' + str(also_bought_antisymmetric))
        print('Also viewed:')
        print('\tsymmetric: ' + str(also_viewed_symmetric))
        print('\tantisymmetric: ' + str(also_viewed_antisymmetric))
        print('Bought together:')
        print('\tsymmetric: ' + str(bought_together_symmetric))
        print('\tantisymmetric: ' + str(bought_together_antisymmetric))
        print('Buy after viewing:')
        print('\tsymmetric: ' + str(buy_after_viewing_symmetric))
        print('\tantisymmetric: ' + str(buy_after_viewing_antisymmetric))

    def get_relations(self, item_id):

        aa = self.connection.execute("SELECT title FROM item WHERE id = ?;", [item_id])
        item_name_tuple = aa.fetchone()
        if (item_name_tuple is not None) and (item_name_tuple[0] is not None):
            item_name = item_name_tuple[0]
        else:
            item_name = 'No title'
        print('Item name: ' + item_name)
        print()

        relations = self.connection.execute("SELECT relatedItemId, relation FROM item_related_list WHERE itemId = ?;",
                                            [item_id])
        related_items = []

        for related_item_id, relation in relations:
            name = self.connection.execute('SELECT title FROM item WHERE id = ?', [related_item_id])
            related_items.append((relation, related_item_id, name.fetchone()))

        self.connection.close()

        for related_item in related_items:
            print(related_item)

    # http://www.webgraphviz.com/
    #
    # If run with argument item_id = "0641843208", add result to this:
    # digraph prof {
    #       ratio = fill;
    #       node [style=filled];
    #       "0641843208 McNeill Designs YBS Pop Culture Add-on Deck" [color="0.679 0.592 0.6"];
    #       ...(method result)
    # }
    #
    # colors: "0.679 0.592 0.6" "0.629 0.75 0.75" "0.571 0.896 0.85" "0.546 0.954 0.9"
    #
    # TODO varianta generovat vazby jen mezi existujicimi uzly (ze seznamu), nevytvaret nove
    def get_relations_for_graph(self, item_id, color):
        not_only_also_view = False

        relations = self.connection.execute("SELECT relatedItemId, relation FROM item_related_list WHERE itemId = ?;",
                                            [item_id])

        aa = self.connection.execute("SELECT title FROM item WHERE id = ?;", [item_id])
        item_name_tuple = aa.fetchone()
        if (item_name_tuple is not None) and (item_name_tuple[0] is not None):
            item_name = item_name_tuple[0]
        else:
            item_name = 'No title'
        print(item_name)
        print()
        print()
        print()

        item = '"' + item_id + ' ' + item_name[:50] + '"'
        rel_colors = {
            'also_viewed': '1,0,0',
            'also_bought': '0.283 0.671 0.308',
            'bought_together': '0.67 0.742 0.367',
            'buy_after_viewing': '0.992 0.854 0.521',
        }
        rel_labels = {
            'also_viewed': 'av',
            'also_bought': 'ab',
            'bought_together': 'bt',
            'buy_after_viewing': 'bav',
        }
        graph = []

        for (related_item_id, relation) in relations:
            name_cur = self.connection.execute("SELECT title FROM item WHERE id = ?", [related_item_id])
            name_tuple = name_cur.fetchone()
            if (name_tuple is not None) and (name_tuple[0] is not None):
                name = name_tuple[0]
            else:
                name = 'No title'
            rel_item = '"' + related_item_id + ' ' + name[:50] + '"'
            graph.append(item + ' -> ' + rel_item + ' [color="' + rel_colors[relation] + '", label="' + rel_labels[relation] + '"]')
            graph.append(rel_item + ' [color="' + color + '"]')

            if relation != 'also_viewed':
                not_only_also_view = True

        self.connection.close()

        sep = ';\n'
        graph_string = sep.join(graph)
        print(graph_string)
        print()
        print(not_only_also_view)

        return graph_string

    def get_name(self, item_id):
        aa = self.connection.execute("SELECT title FROM item WHERE id = ?;", [item_id])
        print(aa.fetchone())

        self.connection.close()


if __name__ == "__main__":
    related_items_explorer = RelatedItemsExplorer()
    # related_items_explorer.check_symmetry()
    # id = 'B00004NKJO'
    # related_items_explorer.get_relations(id)
    related_items_explorer.related_statistics()
