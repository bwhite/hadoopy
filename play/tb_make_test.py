# -*- coding: utf-8 -*-
import typedbytes
import random
import sys

uni_test = ('ᚠᛇᚻ᛫ᛒᛦᚦ᛫ᚠᚱᚩᚠᚢᚱ᛫ᚠᛁᚱᚪ᛫ᚷᛖᚻᚹᛦᛚᚳᚢᛗᛋᚳᛖᚪᛚ᛫ᚦᛖᚪᚻ᛫ᛗᚪᚾᚾᚪ᛫ᚷᛖᚻᚹᛦᛚᚳ᛫ᛗᛁᚳᛚᚢᚾ᛫ᚻᛦᛏ᛫ᛞ'
            'ᚫᛚᚪᚾᚷᛁᚠ᛫ᚻᛖ᛫ᚹᛁᛚᛖ᛫ᚠᚩᚱ᛫ᛞᚱᛁᚻᛏᚾᛖ᛫ᛞᚩᛗᛖᛋ᛫ᚻᛚᛇᛏᚪᚾ᛬An preost wes on leoden,'
            'Laȝamon was ihoten He wes Leovenaðes sone -- liðe him be Drihten. ').decode('utf-8')


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


def rand_string(sz):
    return ''.join(random.choice(uni_test) for x in range(sz))


def rand_byte():
    return random.randint(-128, 127)


def rand_float():
    return random.random()


def rand_int():
    return random.randint(-sys.maxint - 1, sys.maxint)


def rand_long():
    # [-2**128, 2**128 - 1]
    lower = -340282366920938463463374607431768211456L
    upper = 340282366920938463463374607431768211455L
    return random.randint(lower, upper)


def rand_bool():
    return bool(random.randint(0, 1))


def rand_list(sz, immutable=False):
    f = [lambda : rand_bytes(sz / 2),
         lambda : rand_string(sz / 2),
         rand_float, rand_int, rand_long, rand_bool,
         lambda : rand_tuple(sz / 2, immutable)]
    if not immutable:
        f.append(lambda : rand_list(sz / 2, immutable))
        f.append(lambda : rand_dict(sz / 2))
    return [random.choice(f)() for x in range(sz)]


def rand_tuple(sz, immutable=False):
    return tuple(rand_list(sz, immutable))


def rand_dict(sz):
    return dict(zip(rand_list(sz, True), rand_list(sz)))


with open('gb_single.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    grabbag(a)

with open('gb_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        grabbag(a)

with open('bytes_100_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_bytes(rand_bytes(100))
        a.write_bytes(rand_bytes(100))

with open('bytes_1k_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_bytes(rand_bytes(1000))
        a.write_bytes(rand_bytes(1000))

with open('bytes_10k_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_bytes(rand_bytes(10000))
        a.write_bytes(rand_bytes(10000))

with open('bool_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_bool(rand_bool())
        a.write_bool(rand_bool())

with open('byte_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_byte(rand_byte())
        a.write_byte(rand_byte())

with open('string_100_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_unicode(rand_string(100))
        a.write_unicode(rand_string(100))

with open('string_1k_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_unicode(rand_string(1000))
        a.write_unicode(rand_string(1000))

with open('string_10k_10k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
        a.write_unicode(rand_string(10000))
        a.write_unicode(rand_string(10000))


with open('float_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_float(rand_float())
        a.write_float(rand_float())

with open('double_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_double(rand_float())
        a.write_double(rand_float())

with open('int_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_int(rand_int())
        a.write_int(rand_int())

with open('long_100k.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(100000):
        a.write_long(rand_long())
        a.write_long(rand_long())

with open('list_50.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(50):
        a.write_list(rand_list(x))
        a.write_list(rand_list(x))

with open('tuple_50.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(50):
        a.write_vector(rand_tuple(x))
        a.write_vector(rand_tuple(x))

with open('dict_50.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(50):
        a.write_map(rand_dict(x))
        a.write_map(rand_dict(x))
