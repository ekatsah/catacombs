import sqlite3
from utils import md5Checksum

def _create_tables():
    cursor = connect.cursor()
    cursor.execute('drop table if exists books')
    cursor.execute('drop table if exists peers')
    cursor.execute('''create table books (
                          id integer primary key asc,
                          name text,
                          path text, 
                          hash text unique)''')
    cursor.execute('''create table peers (
                          id integer primary key asc, 
                          url text unique, 
                          name text)''')
    cursor.close()

def _format_peer(peer):
    return {'id': str(peer[0]), 'url': peer[1], 'name': peer[2]}

def _format_book(book):
    return {'id': str(book[0]), 
            'info_url': '/info/%d' % book[0],
            'get_url': '/get/%d' % book[0],
            'name': book[1]}

# check for multi thread safety!! FIXME
def get_books():
    cursor = connect.execute('select * from books')
    return map(_format_book, cursor)

def get_peers():
    cursor = connect.execute('select * from peers')
    return map(_format_peer, cursor)

def get_a_book(id, remove_path=True):
    cursor = connect.execute('select * from books where id=?', (id,))
    book = cursor.fetchone()
    results = _format_book(book)
    if not remove_path:
        results['path'] = book[2]
        results['hash'] = book[3]
    return results

def add_a_book(book_path, file_name):
    file_hash = md5Checksum(book_path)
    cursor = connect.execute('select * from books where hash=?', (file_hash,))
    if cursor.rowcount == 1:
        print "Error: file '%s' is already in the db with path '%s'" % (book_path, cursor[0][2])
        if book_path != cursor[0][2]:
            print "Updating file path with new path"
            cursor.execute('update books set path=? where hash=?', 
                           (book_path, file_hash))

    else:
        cursor.execute('insert into books (name, path, hash) values (?,?,?)',
                       (file_name, book_path, file_hash))
    connect.commit()
    return None

def add_a_peer(peer_url, name):
    cursor = connect.execute('select * from peers where url=?', (peer_url,))
    if cursor.rowcount == 1:
        print "error: peer with url '%s' already exists" % peer_url
    else:
        cursor.execute('insert into peers (url, name) values (?,?)',
                       (peer_url, name))
        connect.commit()
    return None

connect = sqlite3.connect('db.sql')
try:
    connect.execute('select * from peers limit 1')
except:
    _create_tables()
