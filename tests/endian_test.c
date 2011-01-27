/* Fuzz tester for the byteconversion.h code

Only 64bit little endian is tested as that is the only real code.  This file assumes you have endian.h
so that we can compare against it.

Test 0: Ensure that the tests fail when the functions just return 0.  This is a test of the test.
gcc -o endian_test endian_test.c -Wall -D FAIL_TEST

Test 1: Check the code itself, this will stress test the actual conversion code 
gcc -o endian_test endian_test.c -Wall

Test 3: Check that if we 
gcc -o endian_test endian_test.c -Wall -D BYTECONVERSION_HASENDIAN_H
*/
#include <endian.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#ifndef FAIL_TEST
#include "../hadoopy/byteconversion.h"
#else
#define _be64toh(x) (0)
#define _htobe64(x) (0)
#endif

inline void cmp(uint64_t vali) {
    assert(_be64toh(vali) == be64toh(vali));
    assert(_htobe64(vali) == htobe64(vali));
}

void random_cast_double() {
    int rounds;
    double rand_max = (double)RAND_MAX;
    for (rounds = 0; rounds < 1000; ++rounds) {
	double val = rand() / rand_max;
	uint64_t vali = *((uint64_t*)(&val));
	cmp(vali);
    }
}

void rand_int() {
    int rounds;
    uint64_t vali = 0;
    unsigned char *valc = (unsigned char *)&vali;
    int i;
    for (rounds = 0; rounds < 10000; ++rounds) {
	for (i = 0; i < 8; ++i)
	    valc[i] = rand() % 256;
	cmp(vali);
    }
}


void rand_int_add() {
    int rounds;
    uint64_t vali = 0;
    for (rounds = 0; rounds < 10000; ++rounds) {
	cmp(vali);
	vali += rand();
    }
}


void count_int() {
    int rounds;
    uint64_t vali = 0;
    for (rounds = 0; rounds < 10000; ++rounds) {
	cmp(vali);
	vali++;
    }
}


int main() {
    random_cast_double();
    rand_int();
    count_int();
    rand_int_add();
    return 0;
}
