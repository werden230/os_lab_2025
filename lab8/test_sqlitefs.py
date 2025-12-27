#!/usr/bin/env python3
import unittest
import tempfile
import os
import stat
import time
import sqlite3
from pathlib import Path
import sys
from database import Database
from sqlitefs import SQLiteFS

FUSE_AVAILABLE = True


class TestDatabase(unittest.TestCase):
    """Тесты для класса Database."""
    
    def setUp(self):
        """Создание временной базы данных для тестов."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        self.db = Database(self.db_path)
        
    def tearDown(self):
        """Очистка после тестов."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_01_database_initialization(self):
        """Тест инициализации базы данных."""
        self.assertTrue(os.path.exists(self.db_path))
        
        root_inode = self.db.get_inode(1)
        self.assertIsNotNone(root_inode)
        self.assertEqual(root_inode['id'], 1)
        self.assertTrue(root_inode['mode'] & stat.S_IFDIR)
        
    def test_02_create_and_get_file(self):
        """Тест создания файла."""
        file_inode_id = self.db.create_entry('/', 'testfile.txt', 
                                           stat.S_IFREG | 0o644,
                                           1000, 1000)
        
        self.assertGreater(file_inode_id, 1)
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertIsNotNone(file_inode)
        self.assertTrue(file_inode['mode'] & stat.S_IFREG)
        self.assertEqual(file_inode['uid'], 1000)
        self.assertEqual(file_inode['gid'], 1000)
        self.assertEqual(file_inode['size'], 0)
        
        file_by_path = self.db.get_inode_by_path('/testfile.txt')
        self.assertIsNotNone(file_by_path)
        self.assertEqual(file_by_path['id'], file_inode_id)
    
    def test_03_create_and_get_directory(self):
        """Тест создания директории."""
        dir_inode_id = self.db.create_entry('/', 'testdir', 
                                          stat.S_IFDIR | 0o755,
                                          1000, 1000)
        
        dir_inode = self.db.get_inode(dir_inode_id)
        self.assertIsNotNone(dir_inode)
        self.assertTrue(dir_inode['mode'] & stat.S_IFDIR)
        
        entries = self.db.list_directory('/testdir')
        self.assertIn('.', entries)
        self.assertIn('..', entries)
    
    def test_04_write_and_read_data(self):
        """Тест записи и чтения данных."""
        file_inode_id = self.db.create_entry('/', 'datafile.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        test_data = b'Hello, SQLiteFS!'
        written = self.db.write_data(file_inode_id, 0, test_data)
        self.assertEqual(written, len(test_data))
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], len(test_data))
        
        read_data = self.db.read_data(file_inode_id, 0, len(test_data))
        self.assertEqual(read_data, test_data)
        
        partial_data = self.db.read_data(file_inode_id, 7, 5)
        self.assertEqual(partial_data, b'SQLit')
    
    def test_05_write_data_with_offset(self):
        """Тест записи данных со смещением."""
        file_inode_id = self.db.create_entry('/', 'offset.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.write_data(file_inode_id, 1000, b'end')
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 1003)
        
        read_data = self.db.read_data(file_inode_id, 0, 1000)
        self.assertEqual(len(read_data), 1000)
        self.assertEqual(read_data, b'\x00' * 1000)
        
        read_end = self.db.read_data(file_inode_id, 1000, 3)
        self.assertEqual(read_end, b'end')
    
    def test_06_chunked_storage(self):
        """Тест чанкового хранения больших файлов."""
        file_inode_id = self.db.create_entry('/', 'bigfile.bin', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        chunk_size = 4096
        large_data = b'X' * (chunk_size * 3 + 100)
        
        written = self.db.write_data(file_inode_id, 0, large_data)
        self.assertEqual(written, len(large_data))
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], len(large_data))
        
        read_data = self.db.read_data(file_inode_id, 0, len(large_data))
        self.assertEqual(read_data, large_data)
        
        cross_chunk_data = self.db.read_data(file_inode_id, 
                                           chunk_size - 50, 100)
        expected = b'X' * 100
        self.assertEqual(cross_chunk_data, expected)
    
    def test_07_truncate_file(self):
        """Тест изменения размера файла."""
        file_inode_id = self.db.create_entry('/', 'truncate.txt', 
                                        stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.write_data(file_inode_id, 0, b'Hello, World!')
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 13)
        
        read_data = self.db.read_data(file_inode_id, 0, 13)
        self.assertEqual(read_data, b'Hello, World!')
        
        self.db.truncate(file_inode_id, 5)
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 5)
        
        read_data = self.db.read_data(file_inode_id, 0, 10)
        self.assertEqual(read_data, b'Hello')
        
        self.db.truncate(file_inode_id, 20)
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 20)
        
        read_data = self.db.read_data(file_inode_id, 0, 20)
        self.assertEqual(len(read_data), 20)
        self.assertEqual(read_data[:5], b'Hello')
        self.assertEqual(read_data[5:20], b'\x00' * 15)
        
        self.db.write_data(file_inode_id, 15, b'End')
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 20)
        
        read_end = self.db.read_data(file_inode_id, 15, 3)
        self.assertEqual(read_end, b'End')
        
        read_data = self.db.read_data(file_inode_id, 0, 20)
        expected = b'Hello' + b'\x00' * 10 + b'End' + b'\x00' * 2
        self.assertEqual(read_data, expected)
        
        self.db.write_data(file_inode_id, 25, b'Extra')
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 30)
        
        read_data = self.db.read_data(file_inode_id, 0, 30)
        expected = b'Hello' + b'\x00' * 10 + b'End' + b'\x00' * 7 + b'Extra'
        self.assertEqual(read_data, expected)
    
    def test_08_remove_file(self):
        """Тест удаления файла."""
        file_inode_id = self.db.create_entry('/', 'toremove.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.assertIsNotNone(self.db.get_inode_by_path('/toremove.txt'))
        
        result = self.db.remove_entry('/', 'toremove.txt')
        self.assertTrue(result)
        
        self.assertIsNone(self.db.get_inode_by_path('/toremove.txt'))
        
        self.assertIsNone(self.db.get_inode(file_inode_id))
    
    def test_09_remove_directory(self):
        """Тест удаления директории."""
        dir_inode_id = self.db.create_entry('/', 'testdir', 
                                          stat.S_IFDIR | 0o755, 1000, 1000)
        
        self.db.create_entry('/testdir', 'nested.txt', 
                          stat.S_IFREG | 0o644, 1000, 1000)
        
        with self.assertRaises(Exception) as cm:
            self.db.remove_entry('/', 'testdir')
        self.assertIn("Directory not empty", str(cm.exception))
        
        self.db.remove_entry('/testdir', 'nested.txt')
        
        result = self.db.remove_entry('/', 'testdir')
        self.assertTrue(result)
        
        self.assertIsNone(self.db.get_inode_by_path('/testdir'))
    
    def test_10_list_directory(self):
        """Тест листинга директории."""
        self.db.create_entry('/', 'file1.txt', stat.S_IFREG | 0o644, 1000, 1000)
        self.db.create_entry('/', 'file2.txt', stat.S_IFREG | 0o644, 1000, 1000)
        self.db.create_entry('/', 'dir1', stat.S_IFDIR | 0o755, 1000, 1000)
        
        entries = self.db.list_directory('/')
        
        self.assertIn('.', entries)
        self.assertIn('..', entries)
        self.assertIn('file1.txt', entries)
        self.assertIn('file2.txt', entries)
        self.assertIn('dir1', entries)
        
        self.db.create_entry('/dir1', 'nested.txt', stat.S_IFREG | 0o644, 1000, 1000)
        nested_entries = self.db.list_directory('/dir1')
        self.assertIn('.', nested_entries)
        self.assertIn('..', nested_entries)
        self.assertIn('nested.txt', nested_entries)
    
    def test_11_chmod(self):
        """Тест изменения прав доступа."""
        file_inode_id = self.db.create_entry('/', 'perms.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        new_mode = (stat.S_IFREG | 0o777)
        self.db.chmod(file_inode_id, new_mode)
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['mode'] & 0o777, 0o777)
        
        self.assertTrue(file_inode['mode'] & stat.S_IFREG)
    
    def test_12_chown(self):
        """Тест изменения владельца."""
        file_inode_id = self.db.create_entry('/', 'owner.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.chown(file_inode_id, 2000, 3000)
        
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['uid'], 2000)
        self.assertEqual(file_inode['gid'], 3000)
    
    def test_13_rename_file(self):
        """Тест переименования файла."""
        file_inode_id = self.db.create_entry('/', 'oldname.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.rename('/', 'oldname.txt', '/', 'newname.txt')
        
        self.assertIsNone(self.db.get_inode_by_path('/oldname.txt'))
        
        new_inode = self.db.get_inode_by_path('/newname.txt')
        self.assertIsNotNone(new_inode)
        self.assertEqual(new_inode['id'], file_inode_id)
    
    def test_14_rename_move_between_dirs(self):
        """Тест перемещения файла между директориями."""
        self.db.create_entry('/', 'dir1', stat.S_IFDIR | 0o755, 1000, 1000)
        self.db.create_entry('/', 'dir2', stat.S_IFDIR | 0o755, 1000, 1000)
        
        file_inode_id = self.db.create_entry('/dir1', 'file.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.rename('/dir1', 'file.txt', '/dir2', 'moved.txt')
        
        self.assertIsNone(self.db.get_inode_by_path('/dir1/file.txt'))
        moved = self.db.get_inode_by_path('/dir2/moved.txt')
        self.assertIsNotNone(moved)
        self.assertEqual(moved['id'], file_inode_id)
    
    def test_15_update_times(self):
        """Тест обновления временных меток."""
        file_inode_id = self.db.create_entry('/', 'timestamps.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        initial_inode = self.db.get_inode(file_inode_id)
        initial_atime = initial_inode['atime']
        initial_mtime = initial_inode['mtime']
        
        time.sleep(0.1)
        
        new_atime = time.time()
        self.db.update_times(file_inode_id, atime=new_atime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['atime'], new_atime, delta=0.01)
        self.assertEqual(updated_inode['mtime'], initial_mtime)  # mtime не должен измениться
        
        new_mtime = time.time()
        self.db.update_times(file_inode_id, mtime=new_mtime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['mtime'], new_mtime, delta=0.01)
        
        new_both = time.time()
        self.db.update_times(file_inode_id, atime=new_both, mtime=new_both)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['atime'], new_both, delta=0.01)
        self.assertAlmostEqual(updated_inode['mtime'], new_both, delta=0.01)
        
        new_ctime = time.time()
        self.db.update_times(file_inode_id, ctime=new_ctime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['ctime'], new_ctime, delta=0.01)
    
    def test_16_persistence(self):
        """Тест персистентности данных между перезапусками."""
        file_inode_id = self.db.create_entry('/', 'persistent.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        test_data = b'This should persist after restart'
        self.db.write_data(file_inode_id, 0, test_data)
        
        self.db.close()
        
        new_db = Database(self.db_path)
        
        persistent_inode = new_db.get_inode_by_path('/persistent.txt')
        self.assertIsNotNone(persistent_inode)
        
        read_data = new_db.read_data(persistent_inode['id'], 0, len(test_data))
        self.assertEqual(read_data, test_data)
        
        new_db.close()
    
    def test_17_hard_links(self):
        """Тест поддержки hard links."""
        file_inode_id = self.db.create_entry('/', 'original.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        self.db.write_data(file_inode_id, 0, b'Shared data')
        
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 1)
        
        self.db.create_entry('/', 'links', stat.S_IFDIR | 0o755, 1000, 1000)
        
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO entries (parent_id, name, inode_id)
                VALUES (?, ?, ?)
            ''', (1, 'link.txt', file_inode_id))
            
            cursor.execute('''
                UPDATE inodes SET nlink = nlink + 1 
                WHERE id = ?
            ''', (file_inode_id,))
        
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 2)
        
        original = self.db.get_inode_by_path('/original.txt')
        link = self.db.get_inode_by_path('/link.txt')
        self.assertEqual(original['id'], link['id'])
        
        self.db.remove_entry('/', 'link.txt')
        
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 1)
        self.assertIsNotNone(self.db.get_inode_by_path('/original.txt'))
        
        self.db.remove_entry('/', 'original.txt')
        
        self.assertIsNone(self.db.get_inode(file_inode_id))


@unittest.skipIf(not FUSE_AVAILABLE, "FUSE modules not available")
class TestSQLiteFS(unittest.TestCase):
    """Тесты для класса SQLiteFS (интеграционные)."""
    
    def setUp(self):
        """Создание временной базы данных и файловой системы."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'fs_test.db')
        
        self.db = Database(self.db_path)
        
        self.fs = SQLiteFS(self.db_path)
    
    def tearDown(self):
        """Очистка после тестов."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_01_getattr_root(self):
        """Тест получения атрибутов корневой директории."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            attrs = self.fs.getattr('/')
            
            self.assertIsInstance(attrs, dict)
            self.assertTrue(attrs['st_mode'] & stat.S_IFDIR)  # Проверяем, что это директория
            self.assertEqual(attrs['st_uid'], 1000)  # UID из mock_get_context
            self.assertEqual(attrs['st_nlink'], 2)  # '.' и '..'
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_02_create_and_unlink_file(self):
        """Тест создания и удаления файла через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            fd = self.fs.create('/testfile.txt', 0o644)
            self.assertIsInstance(fd, int)
            
            attrs = self.fs.getattr('/testfile.txt')
            self.assertTrue(attrs['st_mode'] & stat.S_IFREG)  # Проверяем, что это файл
            
            result = self.fs.unlink('/testfile.txt')
            self.assertEqual(result, 0)
            
            with self.assertRaises(Exception) as cm:
                self.fs.getattr('/testfile.txt')
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_03_mkdir_and_rmdir(self):
        """Тест создания и удаления директории через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            result = self.fs.mkdir('/testdir', 0o755)
            self.assertEqual(result, 0)
            
            attrs = self.fs.getattr('/testdir')
            self.assertTrue(attrs['st_mode'] & stat.S_IFDIR)
            
            entries = self.fs.readdir('/testdir', None)
            self.assertIn('.', entries)
            self.assertIn('..', entries)
            
            result = self.fs.rmdir('/testdir')
            self.assertEqual(result, 0)
            
            with self.assertRaises(Exception) as cm:
                self.fs.getattr('/testdir')
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_04_read_and_write_file(self):
        """Тест чтения и записи файла через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            fd = self.fs.create('/data.txt', 0o644)
            
            test_data = b'Test data for FUSE'
            written = self.fs.write('/data.txt', test_data, 0, fd)
            self.assertEqual(written, len(test_data))
            
            read_data = self.fs.read('/data.txt', len(test_data), 0, fd)
            self.assertEqual(read_data, test_data)
            
            self.fs.release('/data.txt', fd)
            
            fd2 = self.fs.open('/data.txt', os.O_RDONLY)
            
            read_data2 = self.fs.read('/data.txt', len(test_data), 0, fd2)
            self.assertEqual(read_data2, test_data)
            
            self.fs.release('/data.txt', fd2)
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_05_truncate_file(self):
        """Тест изменения размера файла через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            fd = self.fs.create('/truncate.txt', 0o644)
            
            self.fs.write('/truncate.txt', b'Hello, World!', 0, fd)
            self.fs.release('/truncate.txt', fd)
            
            self.fs.truncate('/truncate.txt', 5)
            
            attrs = self.fs.getattr('/truncate.txt')
            self.assertEqual(attrs['st_size'], 5)
            
            fd2 = self.fs.open('/truncate.txt', os.O_RDONLY)
            data = self.fs.read('/truncate.txt', 10, 0, fd2)
            self.assertEqual(data, b'Hello')
            self.fs.release('/truncate.txt', fd2)
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_06_chmod(self):
        """Тест изменения прав доступа через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            self.fs.create('/chmod_test.txt', 0o644)
            
            self.fs.chmod('/chmod_test.txt', 0o777)
            
            attrs = self.fs.getattr('/chmod_test.txt')
            self.assertEqual(attrs['st_mode'] & 0o777, 0o777)
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_07_rename(self):
        """Тест переименования через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            self.fs.create('/oldname.txt', 0o644)
            
            self.fs.rename('/oldname.txt', '/newname.txt')
            
            with self.assertRaises(Exception) as cm:
                self.fs.getattr('/oldname.txt')
            
            attrs = self.fs.getattr('/newname.txt')
            self.assertIsNotNone(attrs)
        finally:
            sqlitefs.fuse_get_context = original_get_context
    
    def test_08_utimens(self):
        """Тест обновления временных меток через FUSE API."""
        import sqlitefs
        original_get_context = sqlitefs.fuse_get_context
        
        def mock_get_context():
            return (1000, 1000, 1000)
        
        sqlitefs.fuse_get_context = mock_get_context
        
        try:
            self.fs.create('/time_test.txt', 0o644)
            
            current_time = time.time()
            
            self.fs.utimens('/time_test.txt', (current_time, current_time))
            
            attrs = self.fs.getattr('/time_test.txt')
            self.assertAlmostEqual(attrs['st_atime'], current_time, delta=0.01)
            self.assertAlmostEqual(attrs['st_mtime'], current_time, delta=0.01)
        finally:
            sqlitefs.fuse_get_context = original_get_context


class TestIntegration(unittest.TestCase):
    """Интеграционные тесты."""
    
    def test_concurrent_access(self):
        """Тест конкурентного доступа к базе данных."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, 'concurrent.db')
        
        try:
            db1 = Database(db_path)
            
            file_id = db1.create_entry('/', 'concurrent.txt', 
                                     stat.S_IFREG | 0o644, 1000, 1000)
            db1.write_data(file_id, 0, b'Data from connection 1')
            
            db2 = Database(db_path)
            
            data = db2.read_data(file_id, 0, 100)
            self.assertEqual(data, b'Data from connection 1')
            
            current_size = db2.get_inode(file_id)['size']
            db2.write_data(file_id, current_size, b' More data from connection 2')
            
            data = db1.read_data(file_id, 0, 200)
            self.assertEqual(data, b'Data from connection 1 More data from connection 2')
            
            db1.close()
            db2.close()
        finally:
            import shutil
            shutil.rmtree(temp_dir)
    
    def test_error_handling(self):
        """Тест обработки ошибок."""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, 'errors.db')
        db = Database(db_path)
        
        try:
            result = db.get_inode_by_path('/nonexistent')
            self.assertIsNone(result)
            
            with self.assertRaises(Exception):
                db.remove_entry('/', 'nonexistent')
            
            with self.assertRaises(Exception):
                db.create_entry('/nonexistent', 'file.txt', 
                              stat.S_IFREG | 0o644, 1000, 1000)
            
            data = db.read_data(99999, 0, 100)
            self.assertEqual(data, b'')
            
            with self.assertRaises(Exception):
                db.write_data(99999, 0, b'data')
        finally:
            db.close()
            import shutil
            shutil.rmtree(temp_dir)


def run_tests():
    """Запуск всех тестов."""
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestDatabase)
    
    if FUSE_AVAILABLE:
        test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSQLiteFS))
    
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIntegration))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)