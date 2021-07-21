# WORD-FREQUENCY

Word Frequency Data Pipeline using Luigi. It selects Top 100 Ebooks from Project Gutenberg.

---

## Requirements

For development, you will only need Python 3.5+ and a python global package such as pip, installed in your environment. Follow this guide on [Digital Ocean](https://www.digitalocean.com/community/tutorials/how-to-build-a-data-processing-pipeline-using-luigi-in-python-on-ubuntu-20-04).

## Install

    $ git clone https://github.com/KevinMarete/word-frequency
    $ cd word-frequency
    $ python3 -m venv luigi-venv
    $ . luigi-venv/bin/activate
    $ pip install -r requirements.txt

## Running the Luigi Scheduler

    $ sudo sh -c ". luigi-venv/bin/activate ;luigid --background --port 8082"


## Running the Luigi Task

    $ python -m luigi --module main TopWords --GlobalParams-NumberBooks 15 --GlobalParams-NumberTopWords 750
