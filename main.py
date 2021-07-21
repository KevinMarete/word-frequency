import luigi
import os
import pickle
import requests

from bs4 import BeautifulSoup
from collections import Counter


class GetTopBooks(luigi.Task):
    """
    Get list of the most popular books from Project Gutenberg
    """

    def output(self):
        return luigi.LocalTarget('data/books_list.txt')

    def run(self):
        resp = requests.get('https://www.gutenberg.org/browse/scores/top')
        soup = BeautifulSoup(resp.content, 'html.parser')

        page_header = soup.find_all('h2', string='Top 100 EBooks yesterday')[0]
        list_top = page_header.find_next_sibling('ol')

        with self.output().open('w') as f:
            for result in list_top.select('li>a'):
                if '/ebooks' in result['href']:
                    f.write(
                        'https://www.gutenberg.org{link}.txt.utf-8\n'.format(link=result['href'])
                    )


class DownloadBooks(luigi.Task):
    """
    Download a specified list of books
    """

    FileID = luigi.IntParameter()
    REPLACE_LIST = """.,"';_[]:*-"""

    def requires(self):
        return GetTopBooks()

    def output(self):
        return luigi.LocalTarget('data/downloads/{}.txt'.format(self.FileID))

    def run(self):
        with self.input().open('r') as i:
            url = i.read().splitlines()[self.FileID]

            with self.output().open('w') as output_file:
                book_downloads = requests.get(url)
                book_text = book_downloads.text

                for char in self.REPLACE_LIST:
                    book_text = book_text.replace(char, ' ')

                book_text = book_text.lower()
                output_file.write(book_text)


class CountWords(luigi.Task):
    """
    Count the frequency of the most common words from a file
    """

    FileID = luigi.IntParameter()

    def requires(self):
        return DownloadBooks(FileID=self.FileID)

    def output(self):
        return luigi.LocalTarget(
            'data/counts/count_{}.pickle'.format(self.FileID),
            format=luigi.format.Nop
        )

    def run(self):
        with self.input().open('r') as i:
            word_count = Counter(i.read().split())

            with self.output().open('w') as output_file:
                pickle.dump(word_count, output_file)


class GlobalParams(luigi.Config):

    NumberBooks = luigi.IntParameter(default=10)
    NumberTopWords = luigi.IntParameter(default=500)


class TopWords(luigi.Task):
    """
    Aggregate the count results from the different files
    """

    @staticmethod
    def clear_data_files():
        summary_file = 'data/summary.txt'
        book_list_file = 'data/books_list.txt'

        # Remove summary and book_list files from data directory
        if os.path.isfile(summary_file): os.unlink(summary_file)
        if os.path.isfile(book_list_file): os.unlink(book_list_file)

        # Remove counts and downloads directory files from data directory
        [os.unlink(f) for f in os.scandir("./data/counts/") if f.name.endswith(".pickle")]
        [os.unlink(f) for f in os.scandir("./data/downloads/") if f.name.endswith(".txt")]

    def requires(self):
        required_inputs = []
        for i in range(GlobalParams().NumberBooks):
            required_inputs.append(CountWords(FileID=i))
        return required_inputs

    def output(self):
        TopWords.clear_data_files()
        return luigi.LocalTarget('data/summary.txt')

    def run(self):
        total_count = Counter()
        for my_input in self.input():
            with my_input.open('rb') as infile:
                next_counter = pickle.load(infile)
                total_count += next_counter

        with self.output().open('w') as f:
            for item in total_count.most_common(GlobalParams().NumberTopWords):
                f.write('{0: <15}{1}\n'.format(*item))
