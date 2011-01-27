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
#include <arpa/inet.h>
#define _be32toh(x) (ntohl(x))
#define _htobe32(x) (htonl(x))
#ifdef BYTECONVERSION_ISBIGENDIAN
#define _be64toh(x) (x)
#define _htobe64(x) (x)
#else
inline uint64_t _byteswap64bit(uint64_t val) {
    return ((uint64_t)ntohl(val >> 32)) | ((uint64_t)ntohl(val) << 32);
}
#define _be64toh(x) (_byteswap64bit(x))
#define _htobe64(x) (_byteswap64bit(x))

#endif /* BYTECONVERSION_ISBIGENDIAN */
#endif /* BYTECONVERSION_HASENDIAN_H */

#endif /* BYTECONVERSION_H */
