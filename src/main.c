/*
 * rdb parser for redis.
 * author: @hulk
 * transfered by wxf
 * date: 2017-5-14
 */
#include "server.h"

#include "slowlog.h"
#include "bio.h"
#include "latency.h"
#include "atomicvar.h"

#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

static void rdb2jsonUsage(void)
{
    fprintf(stderr, "USAGE: ./rdbtools [-f file] -V -h\n");
    fprintf(stderr, "\t-V --version \n");
    fprintf(stderr, "\t-h --help show usage \n");
    fprintf(stderr, "\t-f --file specify which rdb file would be parsed.\n");
    fprintf(stderr, "\t-d --dest specify which json file would be written.\n");
	fprintf(stderr, "\t-t --type specify which type  would be showen.\n");
    fprintf(stderr, "\t Notice: This tool only test on redis 2.2 and 2.4, 2.6, 2.8.\n\n");
}


/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* Server global state */
volatile unsigned long lru_clock; /* Server global current LRU time. */


/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLog(int level, const char *fmt, ...) {
   
}


/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}


void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
                          const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
                      const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

uint64_t dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

uint64_t dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
                         const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
        return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

uint64_t dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            uint64_t hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Generic hash table type where keys are Redis Objects, Values
 * dummy pointers. */
dictType objectKeyPointerValueDictType = {
        dictEncObjHash,            /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictEncObjKeyCompare,      /* key compare */
        dictObjectDestructor, /* key destructor */
        NULL                       /* val destructor */
};

/* Set dictionary type. Keys are SDS strings, values are ot used. */
dictType setDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        dictSdsDestructor,         /* key destructor */
        NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* Note: SDS string shared & freed by skiplist */
        NULL                       /* val destructor */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictObjectDestructor   /* val destructor */
};

/* server.lua_scripts sha (as sds string) -> scripts (as robj) cache. */
dictType shaScriptObjectDictType = {
        dictSdsCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictObjectDestructor        /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        NULL,                       /* key destructor */
        NULL                        /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
        dictSdsCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        dictSdsDestructor           /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
        dictObjHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictObjKeyCompare,          /* key compare */
        dictObjectDestructor,       /* key destructor */
        dictListDestructor          /* val destructor */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:6379 to
 * clusterNode structures. */
dictType clusterNodesDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType clusterNodesBlackListDictType = {
        dictSdsCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType modulesDictType = {
        dictSdsCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};

/* Migrate cache dict type. */
dictType migrateCacheDictType = {
        dictSdsHash,                /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCompare,          /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};

/* Replication cached script dict (server.repl_scriptcache_dict).
 * Keys are sds SHA1 strings, while values are not used at all in the current
 * implementation. */
dictType replScriptCacheDictType = {
        dictSdsCaseHash,            /* hash function */
        NULL,                       /* key dup */
        NULL,                       /* val dup */
        dictSdsKeyCaseCompare,      /* key compare */
        dictSdsDestructor,          /* key destructor */
        NULL                        /* val destructor */
};




void createSharedObjects(void) {
    int j;
    for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] =
                makeObjectShared(createObject(OBJ_STRING,(void*)(long)j));
        shared.integers[j]->encoding = OBJ_ENCODING_INT;
    }
}

void initServerConfig(void) {
    server.port = CONFIG_DEFAULT_SERVER_PORT;
    server.dbnum = CONFIG_DEFAULT_DBNUM;
    server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    
    server.maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    server.maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    server.maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
   
    server.hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    server.hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;
    server.list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
    server.list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
    server.set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;
    server.zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
    server.zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;
}

extern char **environ;


void initServer(void) {
    server.system_memory_size = zmalloc_get_memory_size();
    createSharedObjects();
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);
}

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
    long long start = ustime();
    if (server.aof_state == AOF_ON) {
       
    } else {
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        if (rdbLoad(server.rdb_filename,&rsi) == C_OK) {
            serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",
                      (float)(ustime()-start)/1000000);

            /* Restore the replication ID / offset from the RDB file. */
            if (rsi.repl_id_is_set && rsi.repl_offset != -1) {
                memcpy(server.replid,rsi.repl_id,sizeof(server.replid));
                server.master_repl_offset = rsi.repl_offset;
                /* If we are a slave, create a cached master from this
                 * information, in order to allow partial resynchronizations
                 * with masters. */
             
            }
        } else if (errno != ENOENT) {
            serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
            exit(1);
        }
    }
}

void redisOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
              allocation_size);
    serverPanic("Redis aborting for OUT OF MEMORY");
}

int main(int argc, char **argv) {
    int c;
    char *rdb_file = NULL;
    char *lua_file = NULL;
    char *output_file = NULL;
    char *log_file = NULL;
	char *exec_script = NULL;
	char* compose_condi = NULL;
	int type = 0;
    int is_show_help = 0, is_show_version = 0, is_exec_script=0;
    char short_options [] = { "ehVf:s:o:l:t:" };

    struct option long_options[] = {
            { "help", no_argument, NULL, 'h' }, /* help */
            { "version", no_argument, NULL, 'V' }, /* version */
            { "rdb-iile-path", required_argument,  NULL, 'f' }, /* rdb file path*/
            { "output_file-path", required_argument,  NULL, 'd' }, /* output file path*/
            { "log_file-path", optional_argument , NULL, 'l' }, /* log file path */
            { NULL, no_argument, NULL, 0 }
    };

    struct timeval tv;
    zmalloc_set_oom_handler(redisOutOfMemoryHandler);
    initServerConfig();
    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            break;
        }
        switch (c) {
            case 'h':
                is_show_help = 1;
                break;
            case 'V':
                is_show_version = 1;
                break;
            case 'f':
                rdb_file = optarg;
                break;
            case 'o':
                output_file = optarg;
                break;
            case 'l':
                log_file = optarg;
                break;
			case 't':
                compose_condi = optarg;
                break;
			case 'e':
                is_exec_script = 1;
                break;
            default:
                exit(0);
        }
    }

    if(is_show_version) {
        fprintf(stderr, "\nHELLO, THIS RDB PARSER VERSION 1.0\n\n");
    }
    if(is_show_help) {
        rdb2jsonUsage();
    }
    if(is_show_version || is_show_help) {
        exit(0);
    }
    if(!rdb_file) {
        serverLog(LL_WARNING, "You must specify rdb file by option -f filepath.\n");
    }
    
    if (!output_file && !is_exec_script) {
		rdb2jsonUsage();
        fprintf(stderr, "You must specify output file by option -o filepath.\n");
		exit(0);
    }
    if (!log_file) {
        serverLog(LL_WARNING, "Log file not specified, default path used: /tmp/rdbtools.log.\n");
        server.logfile = "/tmp/rdbtools.log";
    } else {
        server.logfile = log_file;
    }

	if(is_exec_script == 1) {
		exec_script = "parse_txt.py";
	}
	
	if(compose_condi == NULL) {
		compose_condi = "simple";
	}

	if(compose_condi != NULL) {
		if (!strcmp(compose_condi, "simple")){
			type = PARSE_SIMPLE;
		} else if (!strcmp(compose_condi, "complex")) {
			type = PARSE_COMPLEX;
		} else {
			rdb2jsonUsage();
			fprintf(stderr, "You must specify output type by option -t simple or complex.\n");
			exit(0);
		}
	}

    if (access(rdb_file, R_OK) != 0) {
        serverLog(LL_WARNING, "rdb file %s is not exists.\n", rdb_file);
    }
    if (access(lua_file, R_OK) != 0) {
        serverLog(LL_WARNING, "lua file %s is not exists.\n", lua_file);
    }



    initServer();

    //loadDataFromDisk();

	server.rdb_filename = rdb_file;
    server.hash_max_ziplist_entries = 0;

    long long start = ustime();
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;


	if (myRdbLoad(server.rdb_filename, &rsi, output_file, type) == C_OK) {
		serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",(float)(ustime()-start)/1000000);
		if(is_exec_script == 1) {
			call_python(exec_script);
		}
		
	} else if (errno != ENOENT) {
		serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
		exit(1);
	}

    /* Warning the user about suspicious maxmemory setting. */
    if (server.maxmemory > 0 && server.maxmemory < 1024*1024) {
        serverLog(LL_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", server.maxmemory);
    }
    return 0;
}
