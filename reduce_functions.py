def words_counter_reduce(data):
    number_of_words = {}
    for element in data:
        word = element[0]
        if word not in number_of_words:
            number_of_words[word] = 1
        else:
            number_of_words[word] += 1
    return number_of_words.items()
