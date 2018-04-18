import multiprocessing
from multiprocessing import Process, Pool, current_process
from map_functions import words_counter_map
from reduce_functions import words_counter_reduce
import os
import csv


def read_configuration(filename):
    """
    :param filename:name of configuration file to Map Reduce
    :return: dictionary of configuration items
    """
    configuration_dict = {}
    with open(filename) as f:
        for line in f:
            element = line.strip().split(' = ')
            configuration_dict[element[0]] = element[1]
    return configuration_dict


def data_reader(filename):
    """
    :param filename:name of data file: can be .txt or .csv
    :return: splitted data file
    """
    if filename.endswith(".txt"):
        with open(filename) as f:
            lines = f.read().split()
        return lines
    elif filename.endswith(".csv"):
        lines = []
        with open(filename, "r", encoding='utf-8', errors='ignore') as csvDataFile:
            csv_file = csv.reader(csvDataFile)
            for row in csv_file:
                for element in row:
                    splited_element = element.split(' ')
                    for i in splited_element:
                        lines.append(i)
        return lines
    else:
        print("Bad Format")
        return


def data_writer(map_data_result, filename, mode):
    """
    :param map_data_result:data which will be written in the file
    :param filename: name of the file where data will be written
    :param mode: descriptor of the file
    """
    file_to_write = open(filename, mode)
    for key, value in map_data_result:
        file_to_write.write(str(key) + " " + str(value) + '\n')


def data_partition(lines, number_of_mappers):
    """
    :param lines: data to partition
    :param number_of_mappers: number of equal partitions of the data
    :return:partitioned data
    """
    len_of_frame = int(len(lines) / number_of_mappers)
    mapper = 0
    iterated_index = 0
    list_divided_into_frames = []
    while mapper != number_of_mappers:
        if mapper == number_of_mappers - 1:
            list_divided_into_frames.append(lines[iterated_index:])
        else:
            list_divided_into_frames.append(lines[iterated_index:iterated_index + len_of_frame])
        mapper += 1
        iterated_index += len_of_frame

    return list_divided_into_frames


def map(data, cur_proc, combine_data):
    """
    :param data: data to map
    :param cur_proc: number of worked procvess
    :param combine_data: 0 or 1: 1 mean combine function will call 0-won't call
    """
    uncombined_map_data_result = words_counter_map(data)
    if combine_data == 1:
        combined_map_data_result = combine(uncombined_map_data_result)
        data_writer(combined_map_data_result, "Map/" + str(cur_proc) + ".txt", "w")
    elif combine_data == 0:
        data_writer(uncombined_map_data_result, "Map/" + str(cur_proc) + ".txt", "w")


def combine(uncombined_map_data_result):
    """
    :param uncombined_map_data_result: uncombined data
    :return combined data
    """
    combined_words = {}
    for element in uncombined_map_data_result:
        word = element[0]
        if word not in combined_words:
            combined_words[word] = 1
        else:
            combined_words[word] += 1
    return combined_words.items()


def shuffle(number_of_mappers):
    """
    :param number_of_mappers
    :return sorted list, where each element of the list is all ocurrences of element
    """
    map_number = 0
    sorted_tuple_list = []
    while map_number != number_of_mappers:
        with open("Map/" + str(map_number) + ".txt") as f:
            for line in f:
                # print(line)
                element = line.strip().split(" ")
                # print(element)
                sorted_tuple_list.append((element[0], element[-1]))
        map_number += 1

    sorted_tuple_list.sort(key=lambda tup: tup[0])

    curr_key = sorted_tuple_list[0][0]
    index = 0
    tmp_list = []
    final = []

    while index != len(sorted_tuple_list):
        if sorted_tuple_list[index][0] == curr_key:
            tmp_list.append((sorted_tuple_list[index][0], sorted_tuple_list[index][1]))
        else:
            curr_key = sorted_tuple_list[index][0]
            tmp_list = [(sorted_tuple_list[index][0], sorted_tuple_list[index][1])]
            final.append(tmp_list)
        index += 1

    return final


def reduce(data):
    """"
    reduces all elements of data and write it into different files
    """
    curr_proc = current_process()._identity[0]
    reduced_data = words_counter_reduce(data)
    data_writer(reduced_data, "Reduce/" + str(curr_proc) + ".txt", "a")


def reducer_result_merge(result_file):
    """"
    marge all reducers files in one file
    """
    if result_file.endswith(".txt"):
        filenames = []
        cwd = os.getcwd()
        for file in os.listdir(str(cwd) + "/Reduce"):
            filenames.append(file)
            # os.remove("Reduce/" + str(file))

        with open(result_file, 'w') as outfile:
            for filename in filenames:
                with open("Reduce/" + str(filename)) as infile:
                    for line in infile:
                        outfile.write(line)
                os.remove("Reduce/" + str(filename))
    else:
        return "Bad Format"


def parallel_mapper(divided_data, combine_data):
    processes = []
    for i in range(len(divided_data)):
        process = multiprocessing.Process(target=map, args=(divided_data[i], i, combine_data,))
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()


def parallel_reducer(data, number_of_reducers):
    pool = Pool(number_of_reducers)
    pool.map(reduce, data)


# text_csv = data_reader('test_csv.csv')

text_txt = data_reader('test_txt.txt')

config_data = read_configuration("configuration.txt")
parallel_mapper(data_partition(text_txt, int(config_data["NUMBER OF MAPPERS"])), config_data["COMBINE DATA"])
shuffled_data = shuffle(int(config_data["NUMBER OF MAPPERS"]))
parallel_reducer(shuffled_data, int(config_data["NUMBER OF REDUCERS"]))
reducer_result_merge(config_data["RESULT FILE"])
