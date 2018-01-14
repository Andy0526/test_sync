#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author:lewsan
import logging
import multiprocessing
import functools
import psycopg2
import psycopg2.extras
from psycopg2._psycopg import IntegrityError

formats = '[%(asctime)s] %(message)s'
formatter = logging.Formatter(formats)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
fh = logging.FileHandler('log.txt')
fh.setFormatter(formatter)
put_fh = logging.FileHandler('write_log.txt')
put_fh.setFormatter(formatter)
logger = logging.getLogger()
query_logger = logging.getLogger('query')
write_logger = logging.getLogger('write')

for _logger in [logger, query_logger, write_logger]:
    _logger.addHandler(fh)
    _logger.addHandler(ch)

TEST = True
if TEST:
    pg_read_shard = {
        1: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.133',
        2: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.162',
        3: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.168',
    }
    pg_write_shard = {
        1: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.133',
        2: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.162',
        3: 'dbname=sneaky user=sneaky password=77WN88wwc host=172.16.10.168',
    }

else:
    pg_master = 'dbname=sneaky user=sneaky password=77WN88wwc host=192.168.100.109 port=5436'
    pg_shard = {
        1: 'dbname=sneaky user=sneaky password=77WN88wwc host=192.168.100.109 port=5436',
        2: 'dbname=sneaky user=sneaky password=77WN88wwc host=192.168.100.110 port=5436',
    }

NORMAL = 0
LIKED = 1
LIKE = 2
FRIEND = 3
STRANGER_BLOCKED = 4
FRD_BLOCKED = 7
STRANGER_BLOCK = 8
FRD_BLOCK = 11
STRANGER_INTER_BLOCK = 12
FRD_INTER_BLOCK = 15


def get_in_tuple(args):
    args = list(args)
    if len(args) == 1:
        args.append(args[0])
    return tuple(args)


def split_list(data, page_size=100):
    count_all = len(data)
    if count_all % page_size > 0:
        page_count = count_all / page_size + 1
    else:
        page_count = count_all / page_size
    pieces = [data[each * page_size: (each + 1) * page_size] for each in range(page_count)]
    return pieces


class DBManager(object):
    READ_CONNS = {}
    WRITE_CONNS = {}

    def __init__(self):
        self.setup_read_conns()
        self.setup_write_conns()

    def setup_read_conns(self):
        for shard_id, params in pg_read_shard.iteritems():
            conn = psycopg2.connect(params)
            self.READ_CONNS[shard_id] = conn

    def setup_write_conns(self):
        for shard_id, params in pg_write_shard.iteritems():
            conn = psycopg2.connect(params)
            self.WRITE_CONNS[shard_id] = conn

    def query_all(self, query_sql):
        results = []
        try:
            for conn in self.READ_CONNS.itervalues():
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                page_count = cur.execute(query_sql)
                results.extend(cur.fetchall())
        except:
            self.setup_read_conns()
            for conn in self.READ_CONNS.itervalues():
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                page_count = cur.execute(query_sql)
                results.extend(cur.fetchall())
        finally:
            cur.close()
        return results

    def query_master(self, query_sql):
        results = []
        try:
            conn = self.READ_CONNS[1]
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            page_count = cur.execute(query_sql)
            results = cur.fetchall()
        except:
            self.setup_read_conns()
            conn = self.READ_CONNS[1]
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            page_count = cur.execute(query_sql)
            results = cur.fetchall()
        finally:
            cur.close()
        return results

    def write(self, sql_data):
        for shard_id, sql_lst in sql_data.iteritems():
            conn = self.WRITE_CONNS[shard_id]
            cursor = conn.cursor()
            for sql in sql_lst:
                try:
                    cursor.execute(sql)
                    conn.commit()
                except psycopg2.IntegrityError:
                    conn.rollback()
                except psycopg2.InternalError:
                    conn.commit()
                except Exception as e:
                    write_logger.error(e)
                    self.setup_write_conns()
                    conn = self.WRITE_CONNS[shard_id]
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                finally:
                    write_logger.info('finish: {}'.format(sql))

    def close_all(self):
        for conn in self.READ_CONNS.itervalues():
            conn.close()
        for conn in self.WRITE_CONNS.itervalues():
            conn.close()


db_manager = DBManager()

PAGE_SIZE = 500
INSERT_REL_SQL = "insert into pw_relation_new (uid,tuid,relation_type,update_time,create_time) VALUES (%s,%s,%s,'%s','%s')"
INSERT_LIKE_SQL = "insert into pw_like_relation_temp (uid,tuid,relation_type,tuser_name,tuser_note,update_time,create_time) VALUES (%s,%s,%s,%s,%s,'%s','%s')"


def get_block_list(uid_lst, tuid_lst):
    query_sql = 'select uid,tuid from pw_block ' \
                'where uid in {} and tuid in {}'.format(get_in_tuple(uid_lst), get_in_tuple(tuid_lst))
    results = db_manager.query_master(query_sql)
    return [(res['uid'], res['tuid']) for res in results]


def get_contact_data(uid_lst, tuid_lst):
    query_sql = 'select uid,tuid from pw_contact ' \
                'where state!=1 and uid in {} and tuid in {}'.format(get_in_tuple(uid_lst), get_in_tuple(tuid_lst))
    results = db_manager.query_all(query_sql)
    return [(res['uid'], res['tuid']) for res in results]


def get_shard_data(uid_lst):
    query_sql = 'select shard_id,shard_key from pw_user_shard where shard_key in {}'.format(get_in_tuple(uid_lst))
    results = db_manager.query_master(query_sql)
    return {res['shard_key']: res['shard_id'] for res in results}


def get_user_info(uid_lst):
    query_sql = 'select uid,name from pw_user where uid in {}'.format(get_in_tuple(uid_lst))
    results = db_manager.query_all(query_sql)
    return {res['uid']: res['name'] or '' for res in results}


def get_contact_note_data(uids, tuids):
    query_sql = 'select uid,tuid,note from pw_contact_note where uid in {} and tuid in {}'.format(
        get_in_tuple(uids), get_in_tuple(tuids))
    results = db_manager.query_all(query_sql)
    return {(res['uid'], res['tuid']): res['note'] for res in results if res['uid'] != res['tuid']}


def deal_block_data(block_data, rev_block_list=None, contact_list=None, shard_data=None):
    results = {}
    for block in block_data:

        uid = block['uid']
        tuid = block['tuid']
        user_shard_id = shard_data.get(uid)
        tuser_shard_id = shard_data.get(tuid)
        if not user_shard_id or not tuser_shard_id:
            continue
        user_shard_data = results.setdefault(user_shard_id, [])
        tuser_shard_data = results.setdefault(tuser_shard_id, [])
        update_time = create_time = block['update_time']
        user_relation_type = tuser_relation_type = NORMAL
        if (uid, tuid) in contact_list:
            if (uid, tuid) in rev_block_list:
                user_relation_type = tuser_relation_type = FRD_INTER_BLOCK
            else:
                user_relation_type = FRD_BLOCK
                tuser_relation_type = FRD_BLOCKED
        else:
            if (uid, tuid) in rev_block_list:
                user_relation_type = tuser_relation_type = STRANGER_INTER_BLOCK
            else:
                user_relation_type = STRANGER_BLOCK
                tuser_relation_type = STRANGER_BLOCKED
        user_shard_data.append(INSERT_REL_SQL % (uid, tuid, user_relation_type, update_time, create_time))
        tuser_shard_data.append(INSERT_REL_SQL % (tuid, uid, tuser_relation_type, update_time, create_time))
    return results


def sync_block_data(page_size=PAGE_SIZE):
    logger.info('sync_block_data start...')
    index = 0
    POOL = multiprocessing.Pool(processes=3)
    while True:
        offset = index * page_size
        query_sql = 'select uid,tuid,update_time from pw_block limit {} offset {}'.format(page_size, offset)
        query_logger.info('get_block_data limit:{},offset:{},sql:{}'.format(page_size, offset, query_sql))
        results = db_manager.query_all(query_sql)
        if not results:
            break
        uid_lst = []
        tuid_lst = []
        for res in results:
            uid_lst.append(res['uid'])
            tuid_lst.append(res['tuid'])
        reverse_block_uids = get_block_list(tuid_lst, uid_lst)
        contact_lst = get_contact_data(uid_lst, tuid_lst)
        shard_data = get_shard_data(set(uid_lst + tuid_lst))
        query_logger.info('get_block_data end, limit :{}, offset:{}, sql{}'.format(page_size, offset, query_sql))

        pieces = split_list(results)
        func = functools.partial(deal_block_data, rev_block_list=reverse_block_uids, contact_list=contact_lst,
                                 shard_data=shard_data)
        results = POOL.map(func, pieces)
        write_logger.info('sync_block_data insert start...')
        for res in results:
            db_manager.write(res)
            write_logger.info('sync_block_data inserting,num :{} finished'.format(len(res)))
        write_logger.info('sync_block_data insert end')
        index += 1
    POOL.close()
    POOL.join()


def deal_contact_data(contact_data, user_data, note_data, shard_data):
    results = {}
    for contact in contact_data:
        try:
            uid = contact['uid']
            tuid = contact['tuid']
            user_shard_id = shard_data.get(uid)
            tuser_shard_id = shard_data.get(tuid)
            if not user_shard_id or not tuser_shard_id:
                continue
            user_shard_data = results.setdefault(user_shard_id, [])
            tuser_shard_data = results.setdefault(tuser_shard_id, [])
            update_time = create_time = contact['create_time']
            user_relation_type = tuser_relation_type = FRIEND
            user_name = user_data.get(uid, '')
            tuser_name = user_data.get(tuid, '')
            user_note = note_data.get((tuid, uid), '')
            tuser_note = note_data.get((uid, tuid), '')
            user_shard_data.append(INSERT_REL_SQL % (uid, tuid, user_relation_type, update_time, create_time))
            user_shard_data.append(
                INSERT_LIKE_SQL % (uid, tuid, user_relation_type, tuser_name, tuser_note, update_time, create_time))
            tuser_shard_data.append(INSERT_REL_SQL % (tuid, uid, tuser_relation_type, update_time, create_time))
            tuser_shard_data.append(
                INSERT_LIKE_SQL % (tuid, uid, tuser_relation_type, user_name, user_note, update_time, create_time))
        except:
            import traceback
            traceback.print_exc()
            raise
    return results


def sync_contact_data(page_size=PAGE_SIZE):
    index = 0
    POOL = multiprocessing.Pool(processes=3)
    while True:
        offset = index * page_size
        query_sql = 'select uid,tuid,create_time from pw_contact where state=0 order by uid,tuid limit {} offset {}'.format(
            page_size, offset)
        results = db_manager.query_all(query_sql)
        if not results:
            break
        uid_lst = []
        tuid_lst = []
        for res in results:
            uid_lst.append(res['uid'])
            tuid_lst.append(res['tuid'])
        all_uids = set(uid_lst + tuid_lst)
        shard_data = get_shard_data(all_uids)
        pieces = split_list(results)
        user_data = get_user_info(all_uids)
        note_data = get_contact_note_data(all_uids, all_uids)
        func = functools.partial(deal_contact_data, user_data=user_data, note_data=note_data, shard_data=shard_data)
        res = POOL.map(func, pieces)
        print res
        index += 1
    POOL.close()
    POOL.join()


def deal_like_req_data(like_req_data, user_data, shard_data):
    results = {}
    for like_req in like_req_data:
        uid = like_req['uid']
        tuid = like_req['tuid']
        user_shard_id = shard_data.get(uid)
        tuser_shard_id = shard_data.get(tuid)
        if not user_shard_id or not tuser_shard_id:
            continue
        user_shard_data = results.setdefault(user_shard_id, [])
        tuser_shard_data = results.setdefault(tuser_shard_id, [])
        update_time = create_time = like_req['update_time']
        user_relation_type = LIKE
        tuser_relation_type = LIKED
        user_name = user_data.get(uid, '')
        tuser_name = user_data.get(tuid, '')
        user_shard_data.append(INSERT_REL_SQL % (uid, tuid, user_relation_type, update_time, create_time))
        user_shard_data.append(
            INSERT_LIKE_SQL % (uid, tuid, user_relation_type, tuser_name, '', update_time, create_time))
        tuser_shard_data.append(INSERT_REL_SQL % (tuid, uid, tuser_relation_type, update_time, create_time))
        tuser_shard_data.append(
            INSERT_LIKE_SQL % (tuid, uid, tuser_relation_type, user_name, '', update_time, create_time))

    return results


def sync_like_req(page_size=PAGE_SIZE):
    index = 0
    POOL = multiprocessing.Pool(processes=3)
    while True:
        offset = index * page_size
        query_sql = 'select uid,tuid,update_time from pw_contact_request where state=0 order by uid,tuid limit {} offset {}'.format(
            page_size, offset)
        results = db_manager.query_all(query_sql)
        if not results:
            break
        uid_lst = []
        tuid_lst = []
        for res in results:
            uid_lst.append(res['uid'])
            tuid_lst.append(res['tuid'])
        all_uids = set(uid_lst + tuid_lst)
        shard_data = get_shard_data(all_uids)
        pieces = split_list(results)
        user_data = get_user_info(all_uids)
        func = functools.partial(deal_contact_data, user_data=user_data, shard_data=shard_data)
        res = POOL.map(func, pieces)
        print res
        index += 1
    POOL.close()
    POOL.join()


if __name__ == '__main__':
    sync_block_data()
    # sync_contact_data()
    # sync_like_req()
    db_manager.close_all()
