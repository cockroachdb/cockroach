typedef struct DriveStats DriveStats;
typedef struct CPUStats CPUStats;

enum {
	NDRIVE = 16,
	NAMELEN = 31
};

struct DriveStats {
	char name[NAMELEN+1];
	int64_t size;
	int64_t blocksize;

	int64_t read;
	int64_t written;
	int64_t nread;
	int64_t nwrite;
	int64_t readtime;
	int64_t writetime;
	int64_t readlat;
	int64_t writelat;
	int64_t readerrs;
	int64_t writeerrs;
	int64_t readretries;
	int64_t writeretries;
};

struct CPUStats {
	natural_t user;
	natural_t nice;
	natural_t sys;
	natural_t idle;
};

extern int lufia_iostat_v1_readdrivestat(DriveStats a[], int n);
extern int lufia_iostat_v1_readcpustat(CPUStats *cpu);

#if (MAC_OS_X_VERSION_MIN_REQUIRED < 120000)
	// If deployent target is before monterey, use the old name.
	#define IOMainPort IOMasterPort
#endif
