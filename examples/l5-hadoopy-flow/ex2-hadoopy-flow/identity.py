import hadoopy


def mapper(k, v):
    yield k, v

if __name__ == '__main__':
    hadoopy.run(mapper)
