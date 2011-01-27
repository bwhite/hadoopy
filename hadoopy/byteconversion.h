int bedd32toh(int val);

#ifndef BYTECONVERSION_H
#define BYTECONVERSION_H

#ifdef BYTECONVERSION_HASENDIAN_H
#include <endian.h>
#else
#include <stdint.h>

#ifdef BYTECONVERSION_ISBIGENDIAN
#ifndef be32toh
uint32_t be32toh(uint32_t val) {return val;}
#endif
#ifndef htobe32
uint32_t htobe32(uint32_t val) {return val;}
#endif
#ifndef be64toh
uint64_t be64toh(uint64_t val) {return val;}
#endif
#ifndef htobe64
uint64_t htobe64(uint64_t val) {return val;}
#endif
#else
#include <arpa/inet.h>

uint32_t be32toh(uint32_t val) {
    return ntohl(val);
}
#ifndef be32toh
#endif
#ifndef htobe32
uint32_t htobe32(uint32_t val) {
    return htonl(val);
}
#endif
#ifndef be64toh
uint64_t be64toh(uint64_t val) {
    return ((uint64_t)ntohl(val >> 32)) | ((uint64_t)ntohl(val) << 32);
}
#endif
#ifndef htobe64
uint64_t htobe64(uint64_t val) {
    return ((uint64_t)htonl(val >> 32)) | ((uint64_t)htonl(val) << 32);
}
#endif
#endif
#endif

#endif
