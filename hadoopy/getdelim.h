#ifndef GETDELIM_H
#define GETDELIM_H
#include <sys/types.h>
#include <stdlib.h>
#include <stdio.h>
ssize_t getdelim (char **lineptr, size_t *n, int delimiter, FILE *fp);
#endif
