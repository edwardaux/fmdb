//
//  FMDatabaseQueue.m
//  fmdb
//
//  Created by August Mueller on 6/22/11.
//  Copyright 2011 Flying Meat Inc. All rights reserved.
//

#import "FMDatabaseQueue.h"
#import "FMDatabase.h"

static const char *INTERNAL_QUEUE_IDENTIFIER = "fmdb.internal.queue.id";

// checks if already in current queue, prevents deadlock
void dispatch_sync_reentrant(dispatch_queue_t queue, dispatch_block_t block) {
    if (dispatch_get_specific(INTERNAL_QUEUE_IDENTIFIER) == INTERNAL_QUEUE_IDENTIFIER)
        block();
    else
        dispatch_sync(queue, block);
}

/*
 
 Note: we call [self retain]; before using dispatch_sync, just incase 
 FMDatabaseQueue is released on another thread and we're in the middle of doing
 something in dispatch_sync
 
 */
 
@implementation FMDatabaseQueue

@synthesize path = _path;

+ (instancetype)databaseQueueWithPath:(NSString*)aPath {
    
    FMDatabaseQueue *q = [[self alloc] initWithPath:aPath];
    
    FMDBAutorelease(q);
    
    return q;
}

- (instancetype)initWithPath:(NSString*)aPath {
    
    self = [super init];
    
    if (self != nil) {
        
        _db = [FMDatabase databaseWithPath:aPath];
        FMDBRetain(_db);
        
        if (![_db open]) {
            NSLog(@"Could not create database queue for path %@", aPath);
            FMDBRelease(self);
            return 0x00;
        }
        
        _path = FMDBReturnRetained(aPath);
        
        _queue = dispatch_queue_create([[NSString stringWithFormat:@"fmdb.%@", self] UTF8String], NULL);
        dispatch_queue_set_specific(_queue, INTERNAL_QUEUE_IDENTIFIER, (void *)INTERNAL_QUEUE_IDENTIFIER, NULL);
    }
    
    return self;
}

- (void)dealloc {
    
    FMDBRelease(_db);
    FMDBRelease(_path);
    
    if (_queue) {
        FMDBDispatchQueueRelease(_queue);
        _queue = 0x00;
    }
#if ! __has_feature(objc_arc)
    [super dealloc];
#endif
}

- (void)close {
    FMDBRetain(self);
    dispatch_sync_reentrant(_queue, ^() { 
        [_db close];
        FMDBRelease(_db);
        _db = 0x00;
    });
    FMDBRelease(self);
}

- (FMDatabase*)database {
    if (!_db) {
        _db = FMDBReturnRetained([FMDatabase databaseWithPath:_path]);
        
        if (![_db open]) {
            NSLog(@"FMDatabaseQueue could not reopen database for path %@", _path);
            FMDBRelease(_db);
            _db  = 0x00;
            return 0x00;
        }
    }
    
    return _db;
}

- (void)inDatabase:(void (^)(FMDatabase *db))block {
    [self inDatabase:block error:NULL];
}

- (BOOL)inDatabase:(void (^)(FMDatabase *db))block error:(NSError **)error {
    FMDBRetain(self);
    
    __block BOOL ok = YES;
    dispatch_sync_reentrant(_queue, ^() {
        
        FMDatabase *db = [self database];
        block(db);
        
        if ([db hadError]) {
            ok = NO;
            if (error != NULL)
                *error = [db lastError];
        }

        if ([db hasOpenResultSets]) {
            NSLog(@"Warning: there is at least one open result set around after performing [FMDatabaseQueue inDatabase:]");
        }
    });
    
    FMDBRelease(self);
    return ok;
}


- (BOOL)beginTransaction:(BOOL)useDeferred withBlock:(void (^)(FMDatabase *db, BOOL *rollback))block error:(NSError **)error {
    FMDBRetain(self);

    __block BOOL ok = YES;
    dispatch_sync_reentrant(_queue, ^() {
        
        BOOL shouldRollback = NO;
        
        if (useDeferred) {
            ok = [[self database] beginDeferredTransaction];
        }
        else {
            ok = [[self database] beginTransaction];
        }

        if (!ok)
            return;

        block([self database], &shouldRollback);
        
        if (shouldRollback) {
            ok = [[self database] rollback];
        }
        else {
            ok = [[self database] commit];
        }

        if (!ok && error != NULL) {
            *error = [[self database] lastError];
        }
    });
  
    FMDBRelease(self);
    return ok;
}

- (void)inDeferredTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block {
    [self beginTransaction:YES withBlock:block error:NULL];
}

- (BOOL)inDeferredTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block error:(NSError **)error {
    return [self beginTransaction:YES withBlock:block error:error];
}

- (void)inTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block {
    [self beginTransaction:NO withBlock:block error:NULL];
}

- (BOOL)inTransaction:(void (^)(FMDatabase *db, BOOL *rollback))block error:(NSError **)error {
    return [self beginTransaction:NO withBlock:block error:error];
}

#if SQLITE_VERSION_NUMBER >= 3007000
- (NSError*)inSavePoint:(void (^)(FMDatabase *db, BOOL *rollback))block {
    
    static unsigned long savePointIdx = 0;
    __block NSError *err = 0x00;
    FMDBRetain(self);
    dispatch_sync_reentrant(_queue, ^() { 
        
        NSString *name = [NSString stringWithFormat:@"savePoint%ld", savePointIdx++];
        
        BOOL shouldRollback = NO;
        
        if ([[self database] startSavePointWithName:name error:&err]) {
            
            block([self database], &shouldRollback);
            
            if (shouldRollback) {
                [[self database] rollbackToSavePointWithName:name error:&err];
            }
            else {
                [[self database] releaseSavePointWithName:name error:&err];
            }
            
        }
    });
    FMDBRelease(self);
    return err;
}
#endif

@end
