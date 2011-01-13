import typedbytes
import random
import sys


def grabbag(a):
    a.write_bytes('abcdefg')
    a.write_bytes('01234')
    a.write_byte(83)
    a.write_byte(12)
    a.write_bool(True)
    a.write_bool(False)
    a.write_int(13413)
    a.write_int(164)
    a.write_long(8589934592L)
    a.write_long(17179869184L)
    a.write_float(12.5)
    a.write_float(15.25)
    a.write_double(134223.123)
    a.write_double(.1232233)
    a.write_string('abcdefg')
    a.write_string('01234')
    a.write_vector((1, 'a', True))
    a.write_vector((1, .25, ()))
    a.write_list([1, 'a', True])
    a.write_list([1, .25, []])
    a.write_map({'1': 3, 5: True})
    a.write_map({'1': 3, (3, 4): True})


def rand_bytes(sz):
    return ''.join(chr(random.randint(0, 255)) for x in range(sz))


def rand_float():
    return random.random()


def rand_int():
    return random.randint(-sys.maxint - 1, sys.maxint)


def rand_long():
    lower = -9223372036854775808L
    upper = 9223372036854775807L
    return random.randint(lower, upper)


def rand_bool():
    return bool(random.randint(0, 1))


def rand_list(sz, immutable=False):
    f = [lambda : rand_bytes(random.randint(0, 255)),
         rand_float, rand_int, rand_long, rand_bool,
         lambda : rand_tuple(sz / 2, immutable)]
    if not immutable:
        f.append(lambda : rand_list(sz / 2, immutable))
        f.append(lambda : rand_dict(sz / 2))
    return [random.choice(f)() for x in range(sz)]


def rand_tuple(sz, immutable=False):
    return tuple(rand_list(sz / 2, immutable))


def rand_dict(sz):
    zip(rand_list(sz / 2, True), rand_list(sz / 2))


with open('grabbag_single.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    grabbag(a)

with open('grabback_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        grabbag(a)

with open('bytes_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_bytes(rand_bytes(x))
        a.write_bytes(rand_bytes(x))

with open('bool_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_bool(rand_bool(x))
        a.write_bool(rand_bool(x))

with open('byte_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_byte(ord(rand_bytes(1)))
        a.write_byte(ord(rand_bytes(1)))

with open('string_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_string(rand_bytes(x))
        a.write_string(rand_bytes(x))

with open('float_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_float(rand_float())
        a.write_float(rand_float())

with open('double_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_double(rand_float())
        a.write_double(rand_float())

with open('int_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_int(rand_int())
        a.write_int(rand_int())

with open('long_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_long(rand_long())
        a.write_long(rand_long())

with open('list_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_list(rand_list(x))
        a.write_list(rand_list(x))

with open('tuple_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_vector(rand_tuple(x))
        a.write_vector(rand_tuple(x))

with open('dict_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_map(rand_dict(x))
        a.write_map(rand_dict(x))


