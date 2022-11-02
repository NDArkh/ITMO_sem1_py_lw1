import csv
from random import choice
from datetime import datetime as dt


class RowsCounter:
    def __init__(self):
        """
        This class implements a task of rows counting. New instance will be
        created with zero counters.
        """
        self.total_rows_cnt = 0
        self.ok_len_cnt = 0

    def __call__(self, name_to_score: str):
        """
        When the RowsCounter class's instance called as function, it needs to
        provide name_to_score arg of str type. It increases ok_len_cnt only if
        arg len is more than 30. total_rows_cnt will be increased anyway.
        """
        self.total_rows_cnt += 1
        if len(name_to_score) > 30:
            self.ok_len_cnt += 1


class DataWorker:
    def __init__(self):
        """ The base books.csv working class. """
        self.id_to_node = dict()
        self.author_to_ids = dict()
        self.tags_set = set()

    def _append(self, node: list[str], counter: RowsCounter):
        """
        Appends new book's node. Original ID will be changed on collision.
        We use str type, because there is no need of other types usage
        (by task).

        :param node: list of 13 str values with fixed index-sense relation
            [0: 'ID', 1: 'Название', 2: 'Тип', 3: 'Автор', 4: 'Автор (ФИО)',
            5: 'Возрастное ограничение на книгу', 6: 'Дата поступления',
            7: 'Цена поступления', 8: 'Кол-во выдач', 9: 'Дата списания книги',
            10: 'Инвентарный номер', 11: 'Выдана до', 12: 'Жанр книги']
        """
        def collision_avoid(old_id: int, target_dict: dict) -> int:
            """
            Silly algorithm that tries to find free ID by endless increment.

            :return: new key with no collision
            """
            new_id = old_id
            while new_id in target_dict:
                new_id += 1
            return new_id

        _id = int(node[0])
        _author = node[4]
        # add new book's node
        if _id in self.id_to_node:
            _id = collision_avoid(_id, self.id_to_node)
        self.id_to_node[_id] = node[1:]
        # we need in year's row as datetime object
        self.id_to_node[_id][5] = dt.strptime(self.id_to_node[_id][5],
                                              '%d.%m.%Y %H:%M')
        # we need in count of book readers as int object
        _readers_cnt = self.id_to_node[_id][6].split('.')[0]  # to avoid '223.3'
        # at ID 22766202 (22766201)
        self.id_to_node[_id][6] = int()
        # add new book to author's list
        if _author not in self.author_to_ids:
            self.author_to_ids[_author] = [_id]
        else:
            self.author_to_ids[_author].append(_id)
        # add new tags
        for tag in node[12].split('#'):
            _tag = tag.strip()
            if _tag == '':
                continue
            self.tags_set.add(_tag)
        # update counts
        counter(name_to_score=node[1])

    def _sort_books(self):
        """ Sorts books by ID at author_to_ids lists """
        for author in self.author_to_ids:
            self.author_to_ids[author].sort()

    def read_csv(self, fname: str = 'books.csv', encoding: str = 'ansi'):
        """
        Reads a csv file. It also performs other parts of the Task: prints
        file-reading process&results log to stdout; prints unique tags;
        prints TOP-20 of the most popular books.
        You MUST use the Task's file (RUS!) or provide full equality.

        :param fname: name/relative path of file
        :param encoding: file encoding
        """
        counter = RowsCounter()
        print('Чтение файла...', end=' ')
        with open(fname, 'r', encoding='ansi') as f_input:
            is_header = True
            for row in csv.reader(f_input, delimiter=';'):
                if is_header:  # skip the header row
                    is_header = False
                    continue
                self._append(node=row, counter=counter)
        self._sort_books()
        print('завершено.')
        print('-' * 79)

        print('Список уникальных тегов:\n# ' + '\n# '.join(sorted(self.tags_set)))
        print('-' * 79)
        print(f'| {"всего книг":<16} | {counter.total_rows_cnt:>5}{"":>52}|'
              f'\n| {"длинных названий":<16} | {counter.ok_len_cnt:>5}{"":>52}|'
              f'\n| {"уникальных тегов":<16} | {len(self.tags_set):>5}{"":>52}|'
              f'\n{"-" * 79}')

        print('Запись 20 случайных книг в файл...', end=' ')
        self._write_random_bibliographic_file()
        print('завершена.')
        print('-' * 79)

        print('20 самых популярных книг:')
        sorted_by_popularity = sorted(
            self.id_to_node,
            key=lambda _id: self.id_to_node[_id][6],
            reverse=True
        )
        print('-. ' + '\n-. '.join(
            map(self.get_bibliographic_str, sorted_by_popularity[:20])
        ))
        print('-' * 79)

    def _write_random_bibliographic_file(
            self, fname: str = 'generated_result.txt', encoding: str = 'utf-8'
    ):
        """
        Writes 20 random books as bibliographic strings to a text file.

        :param fname: name/relative path of file
        :param encoding: file encoding
        """
        picked_ids = set()
        # for random.choice usage - we need in subscriptable sequence that
        # provides subscript by index
        ids_list = list(self.id_to_node.keys())  # so we make list of keys (IDs)

        # we'll pick random ID until get 20 unique books
        while len(picked_ids) < 20:
            picked_ids.add(choice(ids_list))
        # so just write them
        with open(fname, 'w', encoding=encoding) as f_gen:
            for i, book_id in enumerate(picked_ids):
                f_gen.write(f'{i:>2}: {self.get_bibliographic_str(book_id)}\n')

    def get_bibliographic_str(self, book_id: int) -> str:
        """
        Returns book's data as fmt: "{AUTHOR_NAME}. {BOOK_TITLE} - {YEAR}"

        :param book_id: book's ID
        :return: book's data
        """
        data = self.id_to_node[book_id]
        return f'{data[3]}. {data[0]} - {data[5].year}'

    def _is_able(self, book_id: int) -> bool:
        """
        :param book_id: book's ID
        :return: True if {book's year} >= 2018
        """
        return self.id_to_node[book_id][5] >= dt.strptime('01.01.2018 00:00',
                                                          '%d.%m.%Y %H:%M')

    def find_books(self, search_query: str) -> dict:
        """
        Searches authors by a search query (not case-sensitive; partly match).
        This method can only peek an author with books older(/eq.) than 2018.
        Authors at the results will be sorted by grow of not matched chars count.
        Book list of each author is already sorted by grow of ID. Be sure that
        you called DataWorker.read_csv first.

        :param search_query: searching string (an author name)
        :return: dict; keys: sorted by relevant author names;
        values: list of IDs}
        """
        _search_query = search_query.lower()
        search_query_len = len(_search_query)
        search_results = dict()
        # results searching; rules:
        #   - NO Match Case
        #   - NO Words / Full Match
        #   - YES sort by relevant
        for author in self.author_to_ids:
            if _search_query in author.lower():
                search_results[author] = {
                    'relevant_score': len(author) - search_query_len,
                    'books': list()
                }
                # variable for better code readability;
                # will work because it points to list (mutable type)
                books_list = search_results[author]['books']
                for book in self.author_to_ids[author]:
                    if self._is_able(book):
                        books_list.append(book)

                if len(books_list) == 0:
                    # there are no books that pass the Tasks filter
                    # (year >= 2018) so we don't need to put this author
                    # at results list
                    del search_results[author]

        # makes result dict (look at method's docstring)
        sorted_results = dict()
        for author in sorted(
                search_results,
                key=lambda athr: search_results[athr]['relevant_score']
        ):
            # the search_results will be deleted without these lists because
            # of outer pointers that we make at next line, so we can ignore
            # usage of list.copy()
            sorted_results[author] = search_results[author]['books']
        del search_results  # I'm not 100%-sure what garbage collector will do,
        # so it's just paranoid action

        return sorted_results


if __name__ == '__main__':
    dw = DataWorker()
    dw.read_csv()
    sq = 'author'
    print('СПРАВКА\n-. выход: EOF (*nix: Ctrl-D, Windows: Ctrl-Z+Return) '
          'или Enter (без ввода).')
    print('-' * 79)
    try:
        while (sq := input('Поиск по авторам: ')) != '':
            results = dw.find_books(sq)
            if len(results) > 0:
                for res_author in results:
                    print('-. ', end='')
                    print('\n-. '.join(
                        map(dw.get_bibliographic_str, results[res_author]))
                    )
            else:
                print('!. Нет результатов')
            print('-' * 79)
    except EOFError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        print('Всего доброго!')
