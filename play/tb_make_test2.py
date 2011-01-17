import typedbytes


with open('ex1.tb', 'wb') as fp:
    a = typedbytes.Output(fp)
    for x in range(10000):
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
