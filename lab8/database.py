import sqlite3
import threading
import os
import time
from contextlib import contextmanager


class Database:
    def __init__(self, db_path):
        self.db_path = db_path
        self.local = threading.local()
        self._init_db()
        
    def get_conn(self):
        """Получение соединения с БД (thread-local)"""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(self.db_path)
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn
    
    def commit(self):
        """Коммит транзакции"""
        if hasattr(self.local, 'conn'):
            self.local.conn.commit()
    
    def close(self):
        """Закрытие соединения"""
        if hasattr(self.local, 'conn'):
            self.local.conn.close()
            del self.local.conn
    
    @contextmanager
    def transaction(self):
        """Контекстный менеджер для транзакций"""
        conn = self.get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def _init_db(self):
        '''Создание таблиц, если не существуют'''
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # Таблица для метаданных файлов/директорий
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS inodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mode INTEGER NOT NULL,
                    uid INTEGER NOT NULL,
                    gid INTEGER NOT NULL,
                    size INTEGER DEFAULT 0,
                    atime REAL NOT NULL,
                    mtime REAL NOT NULL,
                    ctime REAL NOT NULL,
                    nlink INTEGER DEFAULT 1
                )
            ''')
            
            # Таблица для имен файлов/директорий
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS entries (
                    parent_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    inode_id INTEGER NOT NULL,
                    FOREIGN KEY (parent_id) REFERENCES inodes(id),
                    FOREIGN KEY (inode_id) REFERENCES inodes(id),
                    PRIMARY KEY (parent_id, name)
                )
            ''')
            
            # Таблица для данных файлов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_data (
                    inode_id INTEGER NOT NULL,
                    chunk_num INTEGER NOT NULL,
                    data BLOB,
                    FOREIGN KEY (inode_id) REFERENCES inodes(id),
                    PRIMARY KEY (inode_id, chunk_num)
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_inode ON entries(inode_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_inode ON file_data(inode_id)')
            
            cursor.execute('SELECT id FROM inodes WHERE id = 1')
            if cursor.fetchone() is None:
                current_time = time.time()
                cursor.execute('''
                    INSERT INTO inodes (id, mode, uid, gid, size, atime, mtime, ctime, nlink)
                    VALUES (1, ?, ?, ?, 0, ?, ?, ?, 2)
                ''', (0o755 | 0o40000, os.getuid(), os.getgid(), 
                     current_time, current_time, current_time))
                
                cursor.execute('''
                    INSERT INTO entries (parent_id, name, inode_id)
                    VALUES (1, '.', 1)
                ''')
                cursor.execute('''
                    INSERT INTO entries (parent_id, name, inode_id)
                    VALUES (1, '..', 1)
                ''')
    
    def get_inode_by_path(self, path):
        """Получение inode по пути"""
        if path == '/':
            return self.get_inode(1)
        
        parts = [p for p in path.split('/') if p]
        current_id = 1
        
        with self.transaction() as conn:
            cursor = conn.cursor()
            for part in parts:
                cursor.execute('''
                    SELECT e.inode_id 
                    FROM entries e
                    WHERE e.parent_id = ? AND e.name = ?
                ''', (current_id, part))
                row = cursor.fetchone()
                if row is None:
                    return None
                current_id = row['inode_id']
            
            return self.get_inode(current_id)
    
    def get_inode(self, inode_id):
        """Получение информации об inode по ID"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM inodes WHERE id = ?', (inode_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def create_entry(self, parent_path, name, mode, uid, gid):
        """Создание нового файла/директории"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            parent_inode = self.get_inode_by_path(parent_path)
            if not parent_inode:
                raise Exception("Parent directory not found")
            parent_id = parent_inode['id']
            
            cursor.execute('''
                SELECT 1 FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (parent_id, name))
            if cursor.fetchone():
                raise Exception("Entry already exists")
            
            current_time = time.time()
            cursor.execute('''
                INSERT INTO inodes (mode, uid, gid, size, atime, mtime, ctime, nlink)
                VALUES (?, ?, ?, 0, ?, ?, ?, ?)
            ''', (mode, uid, gid, current_time, current_time, current_time, 
                 2 if (mode & 0o40000) else 1))
            
            new_inode_id = cursor.lastrowid
            
            cursor.execute('''
                INSERT INTO entries (parent_id, name, inode_id)
                VALUES (?, ?, ?)
            ''', (parent_id, name, new_inode_id))
            
            if mode & 0o40000:  # Если это директория
                cursor.execute('''
                    INSERT INTO entries (parent_id, name, inode_id)
                    VALUES (?, '.', ?)
                ''', (new_inode_id, new_inode_id))
                cursor.execute('''
                    INSERT INTO entries (parent_id, name, inode_id)
                    VALUES (?, '..', ?)
                ''', (new_inode_id, parent_id))
            
            return new_inode_id
    
    def remove_entry(self, parent_path, name):
        """Удаление файла/директории"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            parent_inode = self.get_inode_by_path(parent_path)
            if not parent_inode:
                raise Exception("Parent directory not found")
            parent_id = parent_inode['id']
            
            cursor.execute('''
                SELECT inode_id FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (parent_id, name))
            row = cursor.fetchone()
            if not row:
                raise Exception("Entry not found")
            
            inode_id = row['inode_id']
            
            inode_info = self.get_inode(inode_id)
            if not inode_info:
                raise Exception("Inode not found")
            
            if inode_info['mode'] & 0o40000:  # Директория
                cursor.execute('''
                    SELECT COUNT(*) as cnt FROM entries 
                    WHERE parent_id = ? AND name NOT IN ('.', '..')
                ''', (inode_id,))
                count = cursor.fetchone()['cnt']
                if count > 0:
                    raise Exception("Directory not empty")
            
            # Удаляем запись из entries
            cursor.execute('''
                DELETE FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (parent_id, name))
            
            # Уменьшаем nlink
            cursor.execute('''
                UPDATE inodes SET nlink = nlink - 1 
                WHERE id = ?
            ''', (inode_id,))
            
            # Если это был последний hard link, удаляем данные
            cursor.execute('SELECT nlink FROM inodes WHERE id = ?', (inode_id,))
            nlink = cursor.fetchone()['nlink']
            if nlink <= 0:
                cursor.execute('DELETE FROM file_data WHERE inode_id = ?', (inode_id,))
                cursor.execute('DELETE FROM entries WHERE inode_id = ?', (inode_id,))
                cursor.execute('DELETE FROM inodes WHERE id = ?', (inode_id,))
            
            return True
    
    def list_directory(self, path):
        """Список содержимого директории"""
        inode = self.get_inode_by_path(path)
        if not inode:
            return []
        
        if not (inode['mode'] & 0o40000):
            raise Exception("Not a directory")
        
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name FROM entries 
                WHERE parent_id = ?
            ''', (inode['id'],))
            
            return [row['name'] for row in cursor.fetchall()]
    
    def write_data(self, inode_id, offset, data):
        """Запись данных в файл (чанкованную)"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # Размер чанка (например, 4KB)
            CHUNK_SIZE = 4096
            data_len = len(data)
            
            # Проверяем, существует ли inode
            cursor.execute('SELECT size FROM inodes WHERE id = ?', (inode_id,))
            row = cursor.fetchone()
            if row is None:
                raise Exception("Inode not found")
            
            current_size = row['size']
            new_size = max(current_size, offset + data_len)
            
            # Обновляем размер файла
            cursor.execute('''
                UPDATE inodes 
                SET size = ?, mtime = ?
                WHERE id = ?
            ''', (new_size, time.time(), inode_id))
            
            # Записываем данные по чанкам
            start_chunk = offset // CHUNK_SIZE
            end_chunk = (offset + data_len - 1) // CHUNK_SIZE
            
            data_pos = 0
            for chunk_num in range(start_chunk, end_chunk + 1):
                chunk_start = chunk_num * CHUNK_SIZE
                chunk_end = (chunk_num + 1) * CHUNK_SIZE
                
                # Определяем часть данных для этого чанка
                write_start = max(offset, chunk_start)
                write_end = min(offset + data_len, chunk_end)
                
                if write_start >= write_end:
                    continue
                
                # Получаем текущие данные чанка
                cursor.execute('''
                    SELECT data FROM file_data 
                    WHERE inode_id = ? AND chunk_num = ?
                ''', (inode_id, chunk_num))
                row = cursor.fetchone()
                
                # Создаем или получаем данные чанка
                if row and row['data']:
                    chunk_data = bytearray(row['data'])
                    # Если данные короче CHUNK_SIZE, дополняем нулями
                    if len(chunk_data) < CHUNK_SIZE:
                        chunk_data.extend(b'\x00' * (CHUNK_SIZE - len(chunk_data)))
                else:
                    # Создаем новый чанк с нулями
                    chunk_data = bytearray(CHUNK_SIZE)
                
                # Записываем новые данные в чанк
                chunk_offset = write_start - chunk_start
                write_len = write_end - write_start
                chunk_data[chunk_offset:chunk_offset + write_len] = \
                    data[data_pos:data_pos + write_len]
                data_pos += write_len
                
                # Сохраняем чанк
                cursor.execute('''
                    INSERT OR REPLACE INTO file_data (inode_id, chunk_num, data)
                    VALUES (?, ?, ?)
                ''', (inode_id, chunk_num, bytes(chunk_data)))
            
            return data_len
    
    def read_data(self, inode_id, offset, length):
        """Чтение данных из файла"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # Проверяем, существует ли inode
            cursor.execute('SELECT size FROM inodes WHERE id = ?', (inode_id,))
            row = cursor.fetchone()
            if row is None:
                return b''
            
            file_size = row['size']
            
            if offset >= file_size:
                return b''
            
            # Корректируем длину
            length = min(length, file_size - offset)
            
            # Размер чанка
            CHUNK_SIZE = 4096
            result = bytearray()
            
            start_chunk = offset // CHUNK_SIZE
            end_chunk = (offset + length - 1) // CHUNK_SIZE
            
            for chunk_num in range(start_chunk, end_chunk + 1):
                cursor.execute('''
                    SELECT data FROM file_data 
                    WHERE inode_id = ? AND chunk_num = ?
                ''', (inode_id, chunk_num))
                row = cursor.fetchone()
                
                if row and row['data']:
                    chunk_data = row['data']
                else:
                    # Если чанка нет в базе, это нулевые байты
                    chunk_start = chunk_num * CHUNK_SIZE
                    chunk_end = min(chunk_start + CHUNK_SIZE, file_size)
                    chunk_data = b'\x00' * (chunk_end - chunk_start)
                
                # Определяем часть чанка для чтения
                chunk_start = chunk_num * CHUNK_SIZE
                chunk_end = min(chunk_start + len(chunk_data), file_size)
                
                read_start = max(offset, chunk_start)
                read_end = min(offset + length, chunk_end)
                
                if read_start < read_end:
                    chunk_offset = read_start - chunk_start
                    result.extend(chunk_data[chunk_offset:chunk_offset + (read_end - read_start)])
            
            return bytes(result)
    
    def truncate(self, inode_id, length):
        """Изменение размера файла"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # Получаем текущий размер
            cursor.execute('SELECT size FROM inodes WHERE id = ?', (inode_id,))
            row = cursor.fetchone()
            if not row:
                raise Exception("Inode not found")
            
            current_size = row['size']
            
            # Обновляем размер в метаданных
            cursor.execute('''
                UPDATE inodes 
                SET size = ?, mtime = ?
                WHERE id = ?
            ''', (length, time.time(), inode_id))
            
            # Размер чанка
            CHUNK_SIZE = 4096
            
            if length < current_size:
                # Уменьшаем размер - удаляем ненужные чанки
                start_chunk = (length + CHUNK_SIZE - 1) // CHUNK_SIZE
                
                cursor.execute('''
                    DELETE FROM file_data 
                    WHERE inode_id = ? AND chunk_num >= ?
                ''', (inode_id, start_chunk))
                
                # Обрезаем последний чанк, если нужно
                if length > 0 and length % CHUNK_SIZE != 0:
                    last_chunk = (length - 1) // CHUNK_SIZE
                    cursor.execute('''
                        SELECT data FROM file_data 
                        WHERE inode_id = ? AND chunk_num = ?
                    ''', (inode_id, last_chunk))
                    row = cursor.fetchone()
                    
                    if row and row['data']:
                        chunk_data = row['data']
                        new_length = length - last_chunk * CHUNK_SIZE
                        if len(chunk_data) > new_length:
                            new_data = chunk_data[:new_length]
                            cursor.execute('''
                                UPDATE file_data 
                                SET data = ?
                                WHERE inode_id = ? AND chunk_num = ?
                            ''', (new_data, inode_id, last_chunk))
            else:
                # Увеличиваем размер - добавляем нулевые байты в последний чанк
                if current_size > 0 and current_size % CHUNK_SIZE != 0:
                    # Если был неполный последний чанк
                    last_chunk = (current_size - 1) // CHUNK_SIZE
                    cursor.execute('''
                        SELECT data FROM file_data 
                        WHERE inode_id = ? AND chunk_num = ?
                    ''', (inode_id, last_chunk))
                    row = cursor.fetchone()
                    
                    if row and row['data']:
                        chunk_data = row['data']
                        # Дополняем нулями до границы чанка
                        needed = CHUNK_SIZE - len(chunk_data)
                        if needed > 0:
                            chunk_data += b'\x00' * needed
                            cursor.execute('''
                                UPDATE file_data 
                                SET data = ?
                                WHERE inode_id = ? AND chunk_num = ?
                            ''', (chunk_data, inode_id, last_chunk))
    
    def update_times(self, inode_id, atime=None, mtime=None, ctime=None):
        """Обновление временных меток"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            updates = []
            params = []
            
            if atime is not None:
                updates.append("atime = ?")
                params.append(atime)
            
            if mtime is not None:
                updates.append("mtime = ?")
                params.append(mtime)
            
            if ctime is not None:
                updates.append("ctime = ?")
                params.append(ctime)
            
            if updates:
                params.append(inode_id)
                cursor.execute(f'''
                    UPDATE inodes 
                    SET {', '.join(updates)}
                    WHERE id = ?
                ''', params)
    
    def chmod(self, inode_id, mode):
        """Изменение прав доступа"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE inodes 
                SET mode = ?, ctime = ?
                WHERE id = ?
            ''', (mode, time.time(), inode_id))
    
    def chown(self, inode_id, uid, gid):
        """Изменение владельца"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE inodes 
                SET uid = ?, gid = ?, ctime = ?
                WHERE id = ?
            ''', (uid, gid, time.time(), inode_id))
    
    def rename(self, old_parent_path, old_name, new_parent_path, new_name):
        """Переименование/перемещение файла"""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # Получаем старый parent_id
            old_parent_inode = self.get_inode_by_path(old_parent_path)
            if not old_parent_inode:
                raise Exception("Old parent directory not found")
            old_parent_id = old_parent_inode['id']
            
            # Получаем новый parent_id
            new_parent_inode = self.get_inode_by_path(new_parent_path)
            if not new_parent_inode:
                raise Exception("New parent directory not found")
            new_parent_id = new_parent_inode['id']
            
            # Проверяем, существует ли исходный элемент
            cursor.execute('''
                SELECT inode_id FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (old_parent_id, old_name))
            row = cursor.fetchone()
            if not row:
                raise Exception("Source not found")
            
            inode_id = row['inode_id']
            
            # Проверяем, не существует ли уже целевой элемент
            cursor.execute('''
                SELECT 1 FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (new_parent_id, new_name))
            if cursor.fetchone():
                raise Exception("Target already exists")
            
            # Если это перемещение в другую директорию
            if old_parent_id != new_parent_id:
                # Проверяем, не пытаемся ли переместить директорию в саму себя
                # Для этого получаем все родительские директории новой директории
                current_id = new_parent_id
                while current_id != 1:
                    if current_id == inode_id:
                        raise Exception("Cannot move directory into itself")
                    
                    # Находим parent_id текущей директории через запись '..'
                    cursor.execute('''
                        SELECT e2.inode_id 
                        FROM entries e1
                        JOIN entries e2 ON e1.inode_id = e2.parent_id
                        WHERE e1.parent_id = ? AND e1.name = '..'
                    ''', (current_id,))
                    row = cursor.fetchone()
                    if not row:
                        break
                    current_id = row['inode_id']
                
                # Проверяем корень
                if inode_id == 1:
                    raise Exception("Cannot move root directory")
            
            # Удаляем старую запись
            cursor.execute('''
                DELETE FROM entries 
                WHERE parent_id = ? AND name = ?
            ''', (old_parent_id, old_name))
            
            # Добавляем новую запись
            cursor.execute('''
                INSERT INTO entries (parent_id, name, inode_id)
                VALUES (?, ?, ?)
            ''', (new_parent_id, new_name, inode_id))
            
            # Обновляем ctime
            cursor.execute('''
                UPDATE inodes 
                SET ctime = ?
                WHERE id = ?
            ''', (time.time(), inode_id))