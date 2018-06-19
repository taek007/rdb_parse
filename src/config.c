/* Configuration file parsing and CONFIG GET/SET commands implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "cluster.h"

#include <fcntl.h>
#include <sys/stat.h>

/*-----------------------------------------------------------------------------
 * Config file name-value maps.
 *----------------------------------------------------------------------------*/

typedef struct configEnum {
    const char *name;
    const int val;
} configEnum;

configEnum maxmemory_policy_enum[] = {
    {"volatile-lru", MAXMEMORY_VOLATILE_LRU},
    {"volatile-lfu", MAXMEMORY_VOLATILE_LFU},
    {"volatile-random",MAXMEMORY_VOLATILE_RANDOM},
    {"volatile-ttl",MAXMEMORY_VOLATILE_TTL},
    {"allkeys-lru",MAXMEMORY_ALLKEYS_LRU},
    {"allkeys-lfu",MAXMEMORY_ALLKEYS_LFU},
    {"allkeys-random",MAXMEMORY_ALLKEYS_RANDOM},
    {"noeviction",MAXMEMORY_NO_EVICTION},
    {NULL, 0}
};

configEnum syslog_facility_enum[] = {
    {"user",    LOG_USER},
    {"local0",  LOG_LOCAL0},
    {"local1",  LOG_LOCAL1},
    {"local2",  LOG_LOCAL2},
    {"local3",  LOG_LOCAL3},
    {"local4",  LOG_LOCAL4},
    {"local5",  LOG_LOCAL5},
    {"local6",  LOG_LOCAL6},
    {"local7",  LOG_LOCAL7},
    {NULL, 0}
};

configEnum loglevel_enum[] = {
    {"debug", LL_DEBUG},
    {"verbose", LL_VERBOSE},
    {"notice", LL_NOTICE},
    {"warning", LL_WARNING},
    {NULL,0}
};

configEnum supervised_mode_enum[] = {
    {"upstart", SUPERVISED_UPSTART},
    {"systemd", SUPERVISED_SYSTEMD},
    {"auto", SUPERVISED_AUTODETECT},
    {"no", SUPERVISED_NONE},
    {NULL, 0}
};

configEnum aof_fsync_enum[] = {
    {"everysec", AOF_FSYNC_EVERYSEC},
    {"always", AOF_FSYNC_ALWAYS},
    {"no", AOF_FSYNC_NO},
    {NULL, 0}
};

/* Output buffer limits presets. */
clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT] = {
    {0, 0, 0}, /* normal */
    {1024*1024*256, 1024*1024*64, 60}, /* slave */
    {1024*1024*32, 1024*1024*8, 60}  /* pubsub */
};

/*-----------------------------------------------------------------------------
 * Enum access functions
 *----------------------------------------------------------------------------*/

/* Get enum value from name. If there is no match INT_MIN is returned. */
int configEnumGetValue(configEnum *ce, char *name) {
    while(ce->name != NULL) {
        if (!strcasecmp(ce->name,name)) return ce->val;
        ce++;
    }
    return INT_MIN;
}

/* Get enum name from value. If no match is found NULL is returned. */
const char *configEnumGetName(configEnum *ce, int val) {
    while(ce->name != NULL) {
        if (ce->val == val) return ce->name;
        ce++;
    }
    return NULL;
}

/* Wrapper for configEnumGetName() returning "unknown" insetad of NULL if
 * there is no match. */
const char *configEnumGetNameOrUnknown(configEnum *ce, int val) {
    const char *name = configEnumGetName(ce,val);
    return name ? name : "unknown";
}

/* Used for INFO generation. */
const char *evictPolicyToString(void) {
    return configEnumGetNameOrUnknown(maxmemory_policy_enum,server.maxmemory_policy);
}

/*-----------------------------------------------------------------------------
 * Config file parsing
 *----------------------------------------------------------------------------*/

int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) return 1;
    else if (!strcasecmp(s,"no")) return 0;
    else return -1;
}

void appendServerSaveParams(time_t seconds, int changes) {
    server.saveparams = zrealloc(server.saveparams,sizeof(struct saveparam)*(server.saveparamslen+1));
    server.saveparams[server.saveparamslen].seconds = seconds;
    server.saveparams[server.saveparamslen].changes = changes;
    server.saveparamslen++;
}

void resetServerSaveParams(void) {
    zfree(server.saveparams);
    server.saveparams = NULL;
    server.saveparamslen = 0;
}

void queueLoadModule(sds path, sds *argv, int argc) {
    int i;
    struct moduleLoadQueueEntry *loadmod;

    loadmod = zmalloc(sizeof(struct moduleLoadQueueEntry));
    loadmod->argv = zmalloc(sizeof(robj*)*argc);
    loadmod->path = sdsnew(path);
    loadmod->argc = argc;
    for (i = 0; i < argc; i++) {
        loadmod->argv[i] = createRawStringObject(argv[i],sdslen(argv[i]));
    }
    listAddNodeTail(server.loadmodule_queue,loadmod);
}



/*-----------------------------------------------------------------------------
 * CONFIG SET implementation
 *----------------------------------------------------------------------------*/

#define config_set_bool_field(_name,_var) \
    } else if (!strcasecmp(c->argv[2]->ptr,_name)) { \
        int yn = yesnotoi(o->ptr); \
        if (yn == -1) goto badfmt; \
        _var = yn;

#define config_set_numerical_field(_name,_var,min,max) \
    } else if (!strcasecmp(c->argv[2]->ptr,_name)) { \
        if (getLongLongFromObject(o,&ll) == C_ERR) goto badfmt; \
        if (min != LLONG_MIN && ll < min) goto badfmt; \
        if (max != LLONG_MAX && ll > max) goto badfmt; \
        _var = ll;

#define config_set_memory_field(_name,_var) \
    } else if (!strcasecmp(c->argv[2]->ptr,_name)) { \
        ll = memtoll(o->ptr,&err); \
        if (err || ll < 0) goto badfmt; \
        _var = ll;

#define config_set_enum_field(_name,_var,_enumvar) \
    } else if (!strcasecmp(c->argv[2]->ptr,_name)) { \
        int enumval = configEnumGetValue(_enumvar,o->ptr); \
        if (enumval == INT_MIN) goto badfmt; \
        _var = enumval;

#define config_set_special_field(_name) \
    } else if (!strcasecmp(c->argv[2]->ptr,_name)) {

#define config_set_else } else



/*-----------------------------------------------------------------------------
 * CONFIG GET implementation
 *----------------------------------------------------------------------------*/


/*-----------------------------------------------------------------------------
 * CONFIG REWRITE implementation
 *----------------------------------------------------------------------------*/

#define REDIS_CONFIG_REWRITE_SIGNATURE "# Generated by CONFIG REWRITE"

/* We use the following dictionary type to store where a configuration
 * option is mentioned in the old configuration file, so it's
 * like "maxmemory" -> list of line numbers (first line is zero). */
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);
void dictListDestructor(void *privdata, void *val);

/* Sentinel config rewriting is implemented inside sentinel.c by
 * rewriteConfigSentinelOption(). */
void rewriteConfigSentinelOption(struct rewriteConfigState *state);

dictType optionToLineDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictListDestructor          /* val destructor */
};

dictType optionSetDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* The config rewrite state. */
struct rewriteConfigState {
    dict *option_to_line; /* Option -> list of config file lines map */
    dict *rewritten;      /* Dictionary of already processed options */
    int numlines;         /* Number of lines in current config */
    sds *lines;           /* Current lines as an array of sds strings */
    int has_tail;         /* True if we already added directives that were
                             not present in the original config file. */
};

/* Append the new line to the current configuration state. */
void rewriteConfigAppendLine(struct rewriteConfigState *state, sds line) {
    state->lines = zrealloc(state->lines, sizeof(char*) * (state->numlines+1));
    state->lines[state->numlines++] = line;
}

/* Populate the option -> list of line numbers map. */
void rewriteConfigAddLineNumberToOption(struct rewriteConfigState *state, sds option, int linenum) {
    list *l = dictFetchValue(state->option_to_line,option);

    if (l == NULL) {
        l = listCreate();
        dictAdd(state->option_to_line,sdsdup(option),l);
    }
    listAddNodeTail(l,(void*)(long)linenum);
}

/* Add the specified option to the set of processed options.
 * This is useful as only unused lines of processed options will be blanked
 * in the config file, while options the rewrite process does not understand
 * remain untouched. */
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option) {
    sds opt = sdsnew(option);

    if (dictAdd(state->rewritten,opt,NULL) != DICT_OK) sdsfree(opt);
}

/* Read the old file, split it into lines to populate a newly created
 * config rewrite state, and return it to the caller.
 *
 * If it is impossible to read the old file, NULL is returned.
 * If the old file does not exist at all, an empty state is returned. */
struct rewriteConfigState *rewriteConfigReadOldFile(char *path) {
    FILE *fp = fopen(path,"r");
    struct rewriteConfigState *state = zmalloc(sizeof(*state));
    char buf[CONFIG_MAX_LINE+1];
    int linenum = -1;

    if (fp == NULL && errno != ENOENT) return NULL;

    state->option_to_line = dictCreate(&optionToLineDictType,NULL);
    state->rewritten = dictCreate(&optionSetDictType,NULL);
    state->numlines = 0;
    state->lines = NULL;
    state->has_tail = 0;
    if (fp == NULL) return state;

    /* Read the old file line by line, populate the state. */
    while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL) {
        int argc;
        sds *argv;
        sds line = sdstrim(sdsnew(buf),"\r\n\t ");

        linenum++; /* Zero based, so we init at -1 */

        /* Handle comments and empty lines. */
        if (line[0] == '#' || line[0] == '\0') {
            if (!state->has_tail && !strcmp(line,REDIS_CONFIG_REWRITE_SIGNATURE))
                state->has_tail = 1;
            rewriteConfigAppendLine(state,line);
            continue;
        }

        /* Not a comment, split into arguments. */
        argv = sdssplitargs(line,&argc);
        if (argv == NULL) {
            /* Apparently the line is unparsable for some reason, for
             * instance it may have unbalanced quotes. Load it as a
             * comment. */
            sds aux = sdsnew("# ??? ");
            aux = sdscatsds(aux,line);
            sdsfree(line);
            rewriteConfigAppendLine(state,aux);
            continue;
        }

        sdstolower(argv[0]); /* We only want lowercase config directives. */

        /* Now we populate the state according to the content of this line.
         * Append the line and populate the option -> line numbers map. */
        rewriteConfigAppendLine(state,line);
        rewriteConfigAddLineNumberToOption(state,argv[0],linenum);

        sdsfreesplitres(argv,argc);
    }
    fclose(fp);
    return state;
}

/* Rewrite the specified configuration option with the new "line".
 * It progressively uses lines of the file that were already used for the same
 * configuration option in the old version of the file, removing that line from
 * the map of options -> line numbers.
 *
 * If there are lines associated with a given configuration option and
 * "force" is non-zero, the line is appended to the configuration file.
 * Usually "force" is true when an option has not its default value, so it
 * must be rewritten even if not present previously.
 *
 * The first time a line is appended into a configuration file, a comment
 * is added to show that starting from that point the config file was generated
 * by CONFIG REWRITE.
 *
 * "line" is either used, or freed, so the caller does not need to free it
 * in any way. */
void rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force) {
    sds o = sdsnew(option);
    list *l = dictFetchValue(state->option_to_line,o);

    rewriteConfigMarkAsProcessed(state,option);

    if (!l && !force) {
        /* Option not used previously, and we are not forced to use it. */
        sdsfree(line);
        sdsfree(o);
        return;
    }

    if (l) {
        listNode *ln = listFirst(l);
        int linenum = (long) ln->value;

        /* There are still lines in the old configuration file we can reuse
         * for this option. Replace the line with the new one. */
        listDelNode(l,ln);
        if (listLength(l) == 0) dictDelete(state->option_to_line,o);
        sdsfree(state->lines[linenum]);
        state->lines[linenum] = line;
    } else {
        /* Append a new line. */
        if (!state->has_tail) {
            rewriteConfigAppendLine(state,
                sdsnew(REDIS_CONFIG_REWRITE_SIGNATURE));
            state->has_tail = 1;
        }
        rewriteConfigAppendLine(state,line);
    }
    sdsfree(o);
}

/* Write the long long 'bytes' value as a string in a way that is parsable
 * inside redis.conf. If possible uses the GB, MB, KB notation. */
int rewriteConfigFormatMemory(char *buf, size_t len, long long bytes) {
    int gb = 1024*1024*1024;
    int mb = 1024*1024;
    int kb = 1024;

    if (bytes && (bytes % gb) == 0) {
        return snprintf(buf,len,"%lldgb",bytes/gb);
    } else if (bytes && (bytes % mb) == 0) {
        return snprintf(buf,len,"%lldmb",bytes/mb);
    } else if (bytes && (bytes % kb) == 0) {
        return snprintf(buf,len,"%lldkb",bytes/kb);
    } else {
        return snprintf(buf,len,"%lld",bytes);
    }
}

/* Rewrite a simple "option-name <bytes>" configuration option. */
void rewriteConfigBytesOption(struct rewriteConfigState *state, char *option, long long value, long long defvalue) {
    char buf[64];
    int force = value != defvalue;
    sds line;

    rewriteConfigFormatMemory(buf,sizeof(buf),value);
    line = sdscatprintf(sdsempty(),"%s %s",option,buf);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a yes/no option. */
void rewriteConfigYesNoOption(struct rewriteConfigState *state, char *option, int value, int defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %s",option,
        value ? "yes" : "no");

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a string option. */
void rewriteConfigStringOption(struct rewriteConfigState *state, char *option, char *value, char *defvalue) {
    int force = 1;
    sds line;

    /* String options set to NULL need to be not present at all in the
     * configuration file to be set to NULL again at the next reboot. */
    if (value == NULL) {
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    /* Set force to zero if the value is set to its default. */
    if (defvalue && strcmp(value,defvalue) == 0) force = 0;

    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatrepr(line, value, strlen(value));

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a numerical (long long range) option. */
void rewriteConfigNumericalOption(struct rewriteConfigState *state, char *option, long long value, long long defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %lld",option,value);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a octal option. */
void rewriteConfigOctalOption(struct rewriteConfigState *state, char *option, int value, int defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %o",option,value);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite an enumeration option. It takes as usually state and option name,
 * and in addition the enumeration array and the default value for the
 * option. */
void rewriteConfigEnumOption(struct rewriteConfigState *state, char *option, int value, configEnum *ce, int defval) {
    sds line;
    const char *name = configEnumGetNameOrUnknown(ce,value);
    int force = value != defval;

    line = sdscatprintf(sdsempty(),"%s %s",option,name);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the syslog-facility option. */
void rewriteConfigSyslogfacilityOption(struct rewriteConfigState *state) {
    int value = server.syslog_facility;
    int force = value != LOG_LOCAL0;
    const char *name = NULL, *option = "syslog-facility";
    sds line;

    name = configEnumGetNameOrUnknown(syslog_facility_enum,value);
    line = sdscatprintf(sdsempty(),"%s %s",option,name);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the save option. */
void rewriteConfigSaveOption(struct rewriteConfigState *state) {
    int j;
    sds line;

    /* Note that if there are no save parameters at all, all the current
     * config line with "save" will be detected as orphaned and deleted,
     * resulting into no RDB persistence as expected. */
    for (j = 0; j < server.saveparamslen; j++) {
        line = sdscatprintf(sdsempty(),"save %ld %d",
            (long) server.saveparams[j].seconds, server.saveparams[j].changes);
        rewriteConfigRewriteLine(state,"save",line,1);
    }
    /* Mark "save" as processed in case server.saveparamslen is zero. */
    rewriteConfigMarkAsProcessed(state,"save");
}

/* Rewrite the dir option, always using absolute paths.*/
void rewriteConfigDirOption(struct rewriteConfigState *state) {
    char cwd[1024];

    if (getcwd(cwd,sizeof(cwd)) == NULL) {
        rewriteConfigMarkAsProcessed(state,"dir");
        return; /* no rewrite on error. */
    }
    rewriteConfigStringOption(state,"dir",cwd,NULL);
}

/* Rewrite the slaveof option. */
void rewriteConfigSlaveofOption(struct rewriteConfigState *state) {
    char *option = "slaveof";
    sds line;

    /* If this is a master, we want all the slaveof config options
     * in the file to be removed. Note that if this is a cluster instance
     * we don't want a slaveof directive inside redis.conf. */
    if (server.cluster_enabled || server.masterhost == NULL) {
        rewriteConfigMarkAsProcessed(state,"slaveof");
        return;
    }
    line = sdscatprintf(sdsempty(),"%s %s %d", option,
        server.masterhost, server.masterport);
    rewriteConfigRewriteLine(state,option,line,1);
}

/* Rewrite the notify-keyspace-events option. */
void rewriteConfigNotifykeyspaceeventsOption(struct rewriteConfigState *state) {
   
}



/* Free the configuration rewrite state. */
void rewriteConfigReleaseState(struct rewriteConfigState *state) {
    sdsfreesplitres(state->lines,state->numlines);
    dictRelease(state->option_to_line);
    dictRelease(state->rewritten);
    zfree(state);
}

/* At the end of the rewrite process the state contains the remaining
 * map between "option name" => "lines in the original config file".
 * Lines used by the rewrite process were removed by the function
 * rewriteConfigRewriteLine(), all the other lines are "orphaned" and
 * should be replaced by empty lines.
 *
 * This function does just this, iterating all the option names and
 * blanking all the lines still associated. */
void rewriteConfigRemoveOrphaned(struct rewriteConfigState *state) {
    dictIterator *di = dictGetIterator(state->option_to_line);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        list *l = dictGetVal(de);
        sds option = dictGetKey(de);

        /* Don't blank lines about options the rewrite process
         * don't understand. */
        if (dictFind(state->rewritten,option) == NULL) {
            serverLog(LL_DEBUG,"Not rewritten option: %s", option);
            continue;
        }

        while(listLength(l)) {
            listNode *ln = listFirst(l);
            int linenum = (long) ln->value;

            sdsfree(state->lines[linenum]);
            state->lines[linenum] = sdsempty();
            listDelNode(l,ln);
        }
    }
    dictReleaseIterator(di);
}

/* This function overwrites the old configuration file with the new content.
 *
 * 1) The old file length is obtained.
 * 2) If the new content is smaller, padding is added.
 * 3) A single write(2) call is used to replace the content of the file.
 * 4) Later the file is truncated to the length of the new content.
 *
 * This way we are sure the file is left in a consistent state even if the
 * process is stopped between any of the four operations.
 *
 * The function returns 0 on success, otherwise -1 is returned and errno
 * set accordingly. */
int rewriteConfigOverwriteFile(char *configfile, sds content) {
    int retval = 0;
    int fd = open(configfile,O_RDWR|O_CREAT,0644);
    int content_size = sdslen(content), padding = 0;
    struct stat sb;
    sds content_padded;

    /* 1) Open the old file (or create a new one if it does not
     *    exist), get the size. */
    if (fd == -1) return -1; /* errno set by open(). */
    if (fstat(fd,&sb) == -1) {
        close(fd);
        return -1; /* errno set by fstat(). */
    }

    /* 2) Pad the content at least match the old file size. */
    content_padded = sdsdup(content);
    if (content_size < sb.st_size) {
        /* If the old file was bigger, pad the content with
         * a newline plus as many "#" chars as required. */
        padding = sb.st_size - content_size;
        content_padded = sdsgrowzero(content_padded,sb.st_size);
        content_padded[content_size] = '\n';
        memset(content_padded+content_size+1,'#',padding-1);
    }

    /* 3) Write the new content using a single write(2). */
    if (write(fd,content_padded,strlen(content_padded)) == -1) {
        retval = -1;
        goto cleanup;
    }

    /* 4) Truncate the file to the right length if we used padding. */
    if (padding) {
        if (ftruncate(fd,content_size) == -1) {
            /* Non critical error... */
        }
    }

cleanup:
    sdsfree(content_padded);
    close(fd);
    return retval;
}



/*-----------------------------------------------------------------------------
 * CONFIG command entry point
 *----------------------------------------------------------------------------*/


