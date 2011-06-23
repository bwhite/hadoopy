#ifndef GETDELIM_H
#define GETDELIM_H
#include <stdlib.h>
#include <stdio.h>
ssize_t getdelim (char **lineptr, size_t *n, int delimiter, FILE *fp);
#endif
