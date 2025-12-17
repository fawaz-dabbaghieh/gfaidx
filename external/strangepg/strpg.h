
/*
 * Taken from strangepg https://github.com/qwx9/strangepg/
 * Created by Konstantinn Bonnet under MIT license
 * Only the parts needed for gfaidx were copied
 */

#include <stddef.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <math.h>


#define nil NULL

/* u.h */
//#define nil		((void*)0)
typedef	unsigned short	ushort;
typedef	unsigned char	uchar;
typedef unsigned int	uint;
typedef unsigned long	ulong;	/* FIXME */
typedef signed char	schar;
typedef	long long	vlong;
typedef	unsigned long long uvlong;
typedef long long	intptr;
typedef unsigned long long uintptr;
typedef	uint		Rune;

typedef size_t usize;
typedef ssize_t ssize;
typedef int8_t s8int;
typedef int16_t s16int;
typedef int32_t s32int;
typedef int64_t s64int;
typedef uint8_t u8int;
typedef uint16_t u16int;
typedef uint32_t u32int;
typedef uint64_t u64int;

/* libc.h */
#define	nelem(x)	(sizeof(x)/sizeof((x)[0]))
#define USED(x) (void)(x)
#define SET(x)	(x = *(&(x)))
#define PI	M_PI
#define	OREAD	O_RDONLY
#define	OWRITE	O_WRONLY
#define ORDWR	O_RDWR
#define AEXIST	F_OK
#define AREAD	R_OK
#define IOUNIT	(1<<16)