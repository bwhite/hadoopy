#ifndef BYTECONVERSION_H
#define BYTECONVERSION_H

#ifdef BYTECONVERSION_HASENDIAN_H
#include <endian.h>
#define _be32toh(x) (be32toh(x))
#define _htobe32(x) (htobe32(x))
#define _be64toh(x) (be64toh(x))
#define _htobe64(x) (htobe64(x))
#else
#include <stdint.h>
#define _be32toh(x) (ntohl(x))
#define _htobe32(x) (htonl(x))
#ifdef BYTECONVERSION_ISBIGENDIAN
#define _be64toh(x) (x)
#define _htobe64(x) (x)
#else
#include <arpa/inet.h>
inline uint64_t _be64toh(uint64_t val) {
    return ((uint64_t)ntohl(val >> 32)) | ((uint64_t)ntohl(val) << 32);
}
inline uint64_t _htobe64(uint64_t val) {
    return ((uint64_t)htonl(val >> 32)) | ((uint64_t)htonl(val) << 32);
}
#endif
#endif

#endif
