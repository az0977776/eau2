from random import randint, random

rows = 250
num_spaces = 1
integer_range = 1000000
strings = ['Hello', 'world', 'foo', 'bar', 'baz', '"Hello World"', '1.333', '1', '1111', '"123 String"', '', '', '""', '"Really long string to add to the line to parse"']

def get_start():
    return " " * randint(0, num_spaces) + "<" + " " * randint(0, num_spaces)

def get_end():
    return " " * randint(0, num_spaces) + ">" + " " * randint(0, num_spaces)


def get_boolean():
    rv = str(randint(0, 1))
    if randint(0, 1) == 0:
        rv = ''
    return get_start() + rv + get_end()

def get_int():
    rv = str(randint(-1 * integer_range, integer_range))
    if randint(0, 1) == 0:
        rv = ''
    return get_start() + rv + get_end()

def get_float():
    rv = str((random() - 0.5) * integer_range)
    if randint(0, 1) == 0:
        rv = ''
    return get_start() + rv + get_end()

def get_string():
    return get_start() + strings[randint(0, len(strings) - 1)] + get_end()



def print_strings():
    for _ in range(rows):
        print(get_boolean() + get_int() + get_float() + get_string() + get_boolean() + get_int() + get_float() + get_string() + get_boolean() + get_int())


print_strings()


