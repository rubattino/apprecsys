__author__ = 'mertergun'
def get_user_id(line):
    return line.split('\t')[0]
def get_item_id(line):
    return line.split('\t')[1]
def context_city(line):
    return line.split('\t')[5]
def context_dayOfWeek(line):
    return line.split('\t')[6]
def context_time_of_day(line):
    return line.split('\t')[7]

context = [context_city, context_dayOfWeek, context_time_of_day]




