# -*- coding: utf-8 -*-

"""Mapreduce implementation of PySpark wordcount

The implementation is based on Google Cloud Platform, where the script will
run.

.. _Google Python Style Guide
    https://github.com/google/styleguide/blob/gh-pages/pyguide.md
"""

__copyright__ = 'Copyright 2020, Lorenzo Carnevale'
__author__ = 'Lorenzo Carnevale <lorenzocarnevale@gmail.com>'
__credits__ = ''
__description__ = 'Mapreduce implementation of PySpark wordcount'


# standard libraries
from datetime import datetime
# third parties libraries
from pyspark import SparkContext, SparkConf


DESCENDING = 0


def define_spark_context():
    """Define Spark context with necessary configuration
    """
    conf = SparkConf().setAppName("PySpark Wordcount Job").setMaster("local")
    return SparkContext(conf=conf)

def create_words_list_from_file(spark_context, input_file_endpoint):
    """Read data from text file and split each line into words
    """
    return spark_context.textFile(input_file_endpoint) \
        .flatMap(lambda line: preprocess(line))

def apply_wordcount(words):
    """Count the occurrence of each word
    """
    return words.map(lambda word: mapper(word)) \
        .reduceByKey(lambda a,b: reducer(a,b))

def sort_descending_order(word_counts):
    return word_counts.map(lambda (a,b): (b, a)) \
        .sortByKey(DESCENDING, 1) \
        .map(lambda (a,b): (b, a))

def preprocess(line):
    return line.split(" ")

def mapper(word):
    return (word, 1)

def reducer(a, b):
  return a+b


def main():
    timestamp = datetime.now()

    #TODO - make storage_base_url customizable
    storage_base_url = 'gs://lorenzo-dataproc-template'
    input_file_url = '/oliver-twist-dickens.txt'
    input_file_endpoint = '%s%s' % (storage_base_url, input_file_url)
    output_folder_url = '/oliver-twist-dickens-output-%s' % (timestamp)
    output_file_endpoint = '%s%s' % (storage_base_url, output_folder_url)

    spark_context = define_spark_context()
    words = create_words_list_from_file(spark_context, input_file_endpoint)
    word_counts = apply_wordcount(words)
    ascending_words_counts = sort_descending_order(word_counts)
    ascending_words_counts.saveAsTextFile(output_file_endpoint)


if __name__ == "__main__":
    main()
