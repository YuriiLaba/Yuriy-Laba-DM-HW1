import multiprocessing
from multiprocessing import Process, Lock, Pool

def readData(filename, number_of_mappers):

    # splited_list = []
    # with open(filename) as f:
    #     for line in f:
    #         splited_list.append(line.strip().split(' '))
    #
    # return splited_list

    with open(filename) as f:
        lines = f.read().split()
    # print(lines)
    len_of_frame = int(len(lines)/number_of_mappers)
    # print(len_of_frame)
    mapper = 0
    iterated_index = 0
    list_divided_into_frames = []
    while mapper != number_of_mappers:
        if mapper == number_of_mappers-1:
            list_divided_into_frames.append(lines[iterated_index:])
        else:
            list_divided_into_frames.append(lines[iterated_index:iterated_index + len_of_frame])
        mapper += 1
        iterated_index += len_of_frame

    return list_divided_into_frames

def map(frame):
    cur_proc = multiprocessing.current_process().name
    file = open("Map/" + cur_proc + '.txt', 'w')
    for element in frame:
        file.write(element + " 1" + '\n')




def combine(filename):
    number_of_words = {}
    with open("Map/"+filename) as f:
        for line in f:
            word = line.strip().split(' ')[0]
            if word not in number_of_words:
                number_of_words[word] = 1
            else:
                number_of_words[word] += 1

    file = open("Combine/" + filename, 'w')
    for key, value in number_of_words.items():
        file.write(str(key) + ' ' + str(value) + '\n')
    return number_of_words

def shuffle(filenames):
    sorted_tuple_list = []
    for file in filenames:
        with open("Combine/" + file) as f:
            for line in f:
                element = line.strip().split(' ')
                sorted_tuple_list.append((element[0], element[1]))

    sorted_tuple_list.sort(key=lambda tup: tup[0])

    return sorted_tuple_list

def reduce():
    return

def writeFile():
    return



def parallel_map(divided_data):
    processes = []
    # map_results = multiprocessing.Manager().list([None] * len(array))     #The list that we will be writing to
    # print("Number of mappers is", core_number)
    for i in range(len(divided_data)):
        process = multiprocessing.Process(target=map, args=(divided_data[i], ))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()





divided_text = readData('test.txt', 2)
parallel_map(divided_text)


# print(divided_text)
# pool = Pool(5)
# m = pool.map(map, divided_text)

# lst = ['Process-1.txt', 'Process-2.txt']

# v = pool.map(combine, lst)

# print(shuffle(lst))

# print(combine('ForkPoolWorker-1.txt'))


# print()

# manager = multiprocessing.Manager()
# final_test = manager.list()
# lock = Lock()


# processes=[]
# processes.append(multiprocessing.Process(target=map, args=(divided_text[0], )))
# processes.append(multiprocessing.Process(target=map, args=(divided_text[1], )))

# processes.append(multiprocessing.Process(target=combine, args=(lst[0], )))
# processes.append(multiprocessing.Process(target=combine, args=(lst[1], )))

# pool = Pool(5)
# m = pool.map(map, )

# for process in processes:
#     process.start()
#
# for process in processes:
#     process.join()

# print(final_test)