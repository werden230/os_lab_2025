#!/usr/bin/env python3
"""Unit tests for SQLiteFS filesystem implementation."""

import unittest
import tempfile
import os
import stat
import time
import sqlite3
from pathlib import Path
import sys

# Добавляем текущую директорию в путь для импорта
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from database import Database
    from sqlitefs import SQLiteFS
    FUSE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import required modules: {e}")
    print("Some tests will be skipped.")
    FUSE_AVAILABLE = False


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
        # Проверяем, что база данных создана
        self.assertTrue(os.path.exists(self.db_path))
        
        # Проверяем, что корневая директория создана
        root_inode = self.db.get_inode(1)
        self.assertIsNotNone(root_inode)
        self.assertEqual(root_inode['id'], 1)
        self.assertTrue(root_inode['mode'] & stat.S_IFDIR)  # Проверяем, что это директория
        
    def test_02_create_and_get_file(self):
        """Тест создания файла."""
        # Создаем файл в корневой директории
        file_inode_id = self.db.create_entry('/', 'testfile.txt', 
                                           stat.S_IFREG | 0o644,  # Обычный файл
                                           1000, 1000)
        
        # Проверяем, что файл создан
        self.assertGreater(file_inode_id, 1)
        
        # Получаем информацию о файле
        file_inode = self.db.get_inode(file_inode_id)
        self.assertIsNotNone(file_inode)
        self.assertTrue(file_inode['mode'] & stat.S_IFREG)  # Проверяем тип файла
        self.assertEqual(file_inode['uid'], 1000)
        self.assertEqual(file_inode['gid'], 1000)
        self.assertEqual(file_inode['size'], 0)
        
        # Получаем файл по пути
        file_by_path = self.db.get_inode_by_path('/testfile.txt')
        self.assertIsNotNone(file_by_path)
        self.assertEqual(file_by_path['id'], file_inode_id)
    
    def test_03_create_and_get_directory(self):
        """Тест создания директории."""
        # Создаем директорию
        dir_inode_id = self.db.create_entry('/', 'testdir', 
                                          stat.S_IFDIR | 0o755,  # Директория
                                          1000, 1000)
        
        # Проверяем, что директория создана
        dir_inode = self.db.get_inode(dir_inode_id)
        self.assertIsNotNone(dir_inode)
        self.assertTrue(dir_inode['mode'] & stat.S_IFDIR)  # Проверяем, что это директория
        
        # Проверяем, что созданы записи '.' и '..'
        entries = self.db.list_directory('/testdir')
        self.assertIn('.', entries)
        self.assertIn('..', entries)
    
    def test_04_write_and_read_data(self):
        """Тест записи и чтения данных."""
        # Создаем файл
        file_inode_id = self.db.create_entry('/', 'datafile.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Записываем данные
        test_data = b'Hello, SQLiteFS!'
        written = self.db.write_data(file_inode_id, 0, test_data)
        self.assertEqual(written, len(test_data))
        
        # Проверяем размер файла
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], len(test_data))
        
        # Читаем данные
        read_data = self.db.read_data(file_inode_id, 0, len(test_data))
        self.assertEqual(read_data, test_data)
        
        # Читаем часть данных
        partial_data = self.db.read_data(file_inode_id, 7, 5)
        self.assertEqual(partial_data, b'SQLit')
    
    def test_05_write_data_with_offset(self):
        """Тест записи данных со смещением."""
        file_inode_id = self.db.create_entry('/', 'offset.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Записываем данные с большим смещением
        self.db.write_data(file_inode_id, 1000, b'end')
        
        # Проверяем размер файла
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 1003)
        
        # Читаем с начала (должны быть нули)
        read_data = self.db.read_data(file_inode_id, 0, 1000)
        self.assertEqual(len(read_data), 1000)
        self.assertEqual(read_data, b'\x00' * 1000)
        
        # Читаем записанные данные
        read_end = self.db.read_data(file_inode_id, 1000, 3)
        self.assertEqual(read_end, b'end')
    
    def test_06_chunked_storage(self):
        """Тест чанкового хранения больших файлов."""
        file_inode_id = self.db.create_entry('/', 'bigfile.bin', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Создаем данные размером больше размера чанка (4KB)
        chunk_size = 4096
        large_data = b'X' * (chunk_size * 3 + 100)  # 3 чанка + 100 байт
        
        # Записываем большие данные
        written = self.db.write_data(file_inode_id, 0, large_data)
        self.assertEqual(written, len(large_data))
        
        # Проверяем размер файла
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], len(large_data))
        
        # Читаем все данные
        read_data = self.db.read_data(file_inode_id, 0, len(large_data))
        self.assertEqual(read_data, large_data)
        
        # Читаем данные, пересекающие границы чанков
        cross_chunk_data = self.db.read_data(file_inode_id, 
                                           chunk_size - 50, 100)
        expected = b'X' * 100
        self.assertEqual(cross_chunk_data, expected)
    
    def test_07_truncate_file(self):
        """Тест изменения размера файла."""
        file_inode_id = self.db.create_entry('/', 'truncate.txt', 
                                        stat.S_IFREG | 0o644, 1000, 1000)
        
        # Записываем данные
        self.db.write_data(file_inode_id, 0, b'Hello, World!')
        
        # Проверяем начальный размер
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 13)
        
        # Проверяем, что данные записались
        read_data = self.db.read_data(file_inode_id, 0, 13)
        self.assertEqual(read_data, b'Hello, World!')
        
        # Уменьшаем размер
        self.db.truncate(file_inode_id, 5)
        
        # Проверяем размер
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 5)
        
        # Читаем усеченные данные
        read_data = self.db.read_data(file_inode_id, 0, 10)
        self.assertEqual(read_data, b'Hello')
        
        # Увеличиваем размер до 20 байт
        self.db.truncate(file_inode_id, 20)
        
        # Проверяем новый размер
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 20)
        
        # Проверяем, что новые байты нулевые
        read_data = self.db.read_data(file_inode_id, 0, 20)
        self.assertEqual(len(read_data), 20)
        self.assertEqual(read_data[:5], b'Hello')
        self.assertEqual(read_data[5:20], b'\x00' * 15)  # 15 нулей с позиции 5 до 20
        
        # Записываем данные в позицию 15
        self.db.write_data(file_inode_id, 15, b'End')
        
        # Проверяем, что размер ОСТАЛСЯ 20 байт (truncate установил размер)
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 20)
        
        # Читаем данные с позиции 15 (должны получить "End")
        read_end = self.db.read_data(file_inode_id, 15, 3)
        self.assertEqual(read_end, b'End')
        
        # Читаем все 20 байт
        read_data = self.db.read_data(file_inode_id, 0, 20)
        # Ожидаем: 5 байт "Hello" + 10 нулей + 3 байта "End" + 2 нуля
        expected = b'Hello' + b'\x00' * 10 + b'End' + b'\x00' * 2
        self.assertEqual(read_data, expected)
        
        # Тест: запись ЗА пределами текущего размера должна увеличить размер
        self.db.write_data(file_inode_id, 25, b'Extra')
        
        # Проверяем, что размер увеличился до 30 (25 + 5)
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['size'], 30)
        
        # Проверяем данные
        read_data = self.db.read_data(file_inode_id, 0, 30)
        # 0-4: "Hello", 5-14: нули, 15-17: "End", 18-24: нули, 25-29: "Extra"
        expected = b'Hello' + b'\x00' * 10 + b'End' + b'\x00' * 7 + b'Extra'
        self.assertEqual(read_data, expected)
    
    def test_08_remove_file(self):
        """Тест удаления файла."""
        # Создаем файл
        file_inode_id = self.db.create_entry('/', 'toremove.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Проверяем, что файл создан
        self.assertIsNotNone(self.db.get_inode_by_path('/toremove.txt'))
        
        # Удаляем файл
        result = self.db.remove_entry('/', 'toremove.txt')
        self.assertTrue(result)
        
        # Проверяем, что файл удален
        self.assertIsNone(self.db.get_inode_by_path('/toremove.txt'))
        
        # Проверяем, что inode удален
        self.assertIsNone(self.db.get_inode(file_inode_id))
    
    def test_09_remove_directory(self):
        """Тест удаления директории."""
        # Создаем директорию
        dir_inode_id = self.db.create_entry('/', 'testdir', 
                                          stat.S_IFDIR | 0o755, 1000, 1000)
        
        # Создаем файл в директории
        self.db.create_entry('/testdir', 'nested.txt', 
                          stat.S_IFREG | 0o644, 1000, 1000)
        
        # Пытаемся удалить непустую директорию (должна быть ошибка)
        with self.assertRaises(Exception) as cm:
            self.db.remove_entry('/', 'testdir')
        self.assertIn("Directory not empty", str(cm.exception))
        
        # Удаляем файл
        self.db.remove_entry('/testdir', 'nested.txt')
        
        # Теперь можем удалить директорию
        result = self.db.remove_entry('/', 'testdir')
        self.assertTrue(result)
        
        # Проверяем, что директория удалена
        self.assertIsNone(self.db.get_inode_by_path('/testdir'))
    
    def test_10_list_directory(self):
        """Тест листинга директории."""
        # Создаем несколько файлов и директорий
        self.db.create_entry('/', 'file1.txt', stat.S_IFREG | 0o644, 1000, 1000)
        self.db.create_entry('/', 'file2.txt', stat.S_IFREG | 0o644, 1000, 1000)
        self.db.create_entry('/', 'dir1', stat.S_IFDIR | 0o755, 1000, 1000)
        
        # Получаем список
        entries = self.db.list_directory('/')
        
        # Проверяем содержимое (должны быть '.', '..' и созданные файлы/директории)
        self.assertIn('.', entries)
        self.assertIn('..', entries)
        self.assertIn('file1.txt', entries)
        self.assertIn('file2.txt', entries)
        self.assertIn('dir1', entries)
        
        # Проверяем для вложенной директории
        self.db.create_entry('/dir1', 'nested.txt', stat.S_IFREG | 0o644, 1000, 1000)
        nested_entries = self.db.list_directory('/dir1')
        self.assertIn('.', nested_entries)
        self.assertIn('..', nested_entries)
        self.assertIn('nested.txt', nested_entries)
    
    def test_11_chmod(self):
        """Тест изменения прав доступа."""
        file_inode_id = self.db.create_entry('/', 'perms.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Меняем права (только права, сохраняя тип файла)
        new_mode = (stat.S_IFREG | 0o777)
        self.db.chmod(file_inode_id, new_mode)
        
        # Проверяем новые права
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['mode'] & 0o777, 0o777)
        
        # Проверяем, что тип файла сохранился
        self.assertTrue(file_inode['mode'] & stat.S_IFREG)
    
    def test_12_chown(self):
        """Тест изменения владельца."""
        file_inode_id = self.db.create_entry('/', 'owner.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Меняем владельца
        self.db.chown(file_inode_id, 2000, 3000)
        
        # Проверяем нового владельца
        file_inode = self.db.get_inode(file_inode_id)
        self.assertEqual(file_inode['uid'], 2000)
        self.assertEqual(file_inode['gid'], 3000)
    
    def test_13_rename_file(self):
        """Тест переименования файла."""
        # Создаем файл
        file_inode_id = self.db.create_entry('/', 'oldname.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Переименовываем
        self.db.rename('/', 'oldname.txt', '/', 'newname.txt')
        
        # Проверяем, что старый путь не существует
        self.assertIsNone(self.db.get_inode_by_path('/oldname.txt'))
        
        # Проверяем, что новый путь существует
        new_inode = self.db.get_inode_by_path('/newname.txt')
        self.assertIsNotNone(new_inode)
        self.assertEqual(new_inode['id'], file_inode_id)
    
    def test_14_rename_move_between_dirs(self):
        """Тест перемещения файла между директориями."""
        # Создаем директории
        self.db.create_entry('/', 'dir1', stat.S_IFDIR | 0o755, 1000, 1000)
        self.db.create_entry('/', 'dir2', stat.S_IFDIR | 0o755, 1000, 1000)
        
        # Создаем файл в dir1
        file_inode_id = self.db.create_entry('/dir1', 'file.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Перемещаем в dir2
        self.db.rename('/dir1', 'file.txt', '/dir2', 'moved.txt')
        
        # Проверяем
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
        
        # Ждем немного
        time.sleep(0.1)
        
        # Обновляем время доступа
        new_atime = time.time()
        self.db.update_times(file_inode_id, atime=new_atime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['atime'], new_atime, delta=0.01)
        self.assertEqual(updated_inode['mtime'], initial_mtime)  # mtime не должен измениться
        
        # Обновляем время модификации
        new_mtime = time.time()
        self.db.update_times(file_inode_id, mtime=new_mtime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['mtime'], new_mtime, delta=0.01)
        
        # Обновляем оба времени
        new_both = time.time()
        self.db.update_times(file_inode_id, atime=new_both, mtime=new_both)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['atime'], new_both, delta=0.01)
        self.assertAlmostEqual(updated_inode['mtime'], new_both, delta=0.01)
        
        # Обновляем ctime
        new_ctime = time.time()
        self.db.update_times(file_inode_id, ctime=new_ctime)
        
        updated_inode = self.db.get_inode(file_inode_id)
        self.assertAlmostEqual(updated_inode['ctime'], new_ctime, delta=0.01)
    
    def test_16_persistence(self):
        """Тест персистентности данных между перезапусками."""
        # Создаем файл
        file_inode_id = self.db.create_entry('/', 'persistent.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Записываем данные
        test_data = b'This should persist after restart'
        self.db.write_data(file_inode_id, 0, test_data)
        
        # Закрываем соединение
        self.db.close()
        
        # Создаем новое соединение с той же базой
        new_db = Database(self.db_path)
        
        # Проверяем, что данные сохранились
        persistent_inode = new_db.get_inode_by_path('/persistent.txt')
        self.assertIsNotNone(persistent_inode)
        
        # Читаем данные
        read_data = new_db.read_data(persistent_inode['id'], 0, len(test_data))
        self.assertEqual(read_data, test_data)
        
        new_db.close()
    
    def test_17_hard_links(self):
        """Тест поддержки hard links."""
        # Создаем файл
        file_inode_id = self.db.create_entry('/', 'original.txt', 
                                           stat.S_IFREG | 0o644, 1000, 1000)
        
        # Записываем данные
        self.db.write_data(file_inode_id, 0, b'Shared data')
        
        # Проверяем начальное количество ссылок
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 1)
        
        # Создаем директорию
        self.db.create_entry('/', 'links', stat.S_IFDIR | 0o755, 1000, 1000)
        
        # Вручную создаем hard link (имитируем добавление записи с тем же inode_id)
        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO entries (parent_id, name, inode_id)
                VALUES (?, ?, ?)
            ''', (1, 'link.txt', file_inode_id))
            
            # Увеличиваем счетчик ссылок
            cursor.execute('''
                UPDATE inodes SET nlink = nlink + 1 
                WHERE id = ?
            ''', (file_inode_id,))
        
        # Проверяем количество ссылок
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 2)
        
        # Проверяем, что оба пути ведут к одному inode
        original = self.db.get_inode_by_path('/original.txt')
        link = self.db.get_inode_by_path('/link.txt')
        self.assertEqual(original['id'], link['id'])
        
        # Удаляем одну ссылку
        self.db.remove_entry('/', 'link.txt')
        
        # Проверяем, что файл еще существует
        inode = self.db.get_inode(file_inode_id)
        self.assertEqual(inode['nlink'], 1)
        self.assertIsNotNone(self.db.get_inode_by_path('/original.txt'))
        
        # Удаляем последнюю ссылку
        self.db.remove_entry('/', 'original.txt')
        
        # Проверяем, что файл удален
        self.assertIsNone(self.db.get_inode(file_inode_id))


@unittest.skipIf(not FUSE_AVAILABLE, "FUSE modules not available")
class TestSQLiteFS(unittest.TestCase):
    """Тесты для класса SQLiteFS (интеграционные)."""
    
    def setUp(self):
        """Создание временной базы данных и файловой системы."""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, 'fs_test.db')
        
        # Создаем базу данных
        self.db = Database(self.db_path)
        
        # Создаем файловую систему
        self.fs = SQLiteFS(self.db_path)
    
    def tearDown(self):
        """Очистка после тестов."""
        self.db.close()
        import shutil
        shutil.rmtree(self.temp_dir)
    
    def test_01_getattr_root(self):
        """Тест получения атрибутов корневой директории."""
        # Мокаем fuse_get_context
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
            # Создаем файл
            fd = self.fs.create('/testfile.txt', 0o644)
            self.assertIsInstance(fd, int)
            
            # Проверяем, что файл создан
            attrs = self.fs.getattr('/testfile.txt')
            self.assertTrue(attrs['st_mode'] & stat.S_IFREG)  # Проверяем, что это файл
            
            # Удаляем файл
            result = self.fs.unlink('/testfile.txt')
            self.assertEqual(result, 0)
            
            # Проверяем, что файл удален
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
            # Создаем директорию
            result = self.fs.mkdir('/testdir', 0o755)
            self.assertEqual(result, 0)
            
            # Проверяем, что директория создана
            attrs = self.fs.getattr('/testdir')
            self.assertTrue(attrs['st_mode'] & stat.S_IFDIR)
            
            # Проверяем листинг
            entries = self.fs.readdir('/testdir', None)
            self.assertIn('.', entries)
            self.assertIn('..', entries)
            
            # Удаляем директорию
            result = self.fs.rmdir('/testdir')
            self.assertEqual(result, 0)
            
            # Проверяем, что директория удалена
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
            # Создаем файл
            fd = self.fs.create('/data.txt', 0o644)
            
            # Записываем данные
            test_data = b'Test data for FUSE'
            written = self.fs.write('/data.txt', test_data, 0, fd)
            self.assertEqual(written, len(test_data))
            
            # Читаем данные
            read_data = self.fs.read('/data.txt', len(test_data), 0, fd)
            self.assertEqual(read_data, test_data)
            
            # Закрываем файл
            self.fs.release('/data.txt', fd)
            
            # Открываем файл для чтения
            fd2 = self.fs.open('/data.txt', os.O_RDONLY)
            
            # Читаем снова
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
            # Создаем файл
            fd = self.fs.create('/truncate.txt', 0o644)
            
            # Записываем данные
            self.fs.write('/truncate.txt', b'Hello, World!', 0, fd)
            self.fs.release('/truncate.txt', fd)
            
            # Уменьшаем размер
            self.fs.truncate('/truncate.txt', 5)
            
            # Проверяем размер
            attrs = self.fs.getattr('/truncate.txt')
            self.assertEqual(attrs['st_size'], 5)
            
            # Открываем и читаем
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
            # Создаем файл
            self.fs.create('/chmod_test.txt', 0o644)
            
            # Меняем права
            self.fs.chmod('/chmod_test.txt', 0o777)
            
            # Проверяем новые права
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
            # Создаем файл
            self.fs.create('/oldname.txt', 0o644)
            
            # Переименовываем
            self.fs.rename('/oldname.txt', '/newname.txt')
            
            # Проверяем, что старый файл не существует
            with self.assertRaises(Exception) as cm:
                self.fs.getattr('/oldname.txt')
            
            # Проверяем, что новый файл существует
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
            # Создаем файл
            self.fs.create('/time_test.txt', 0o644)
            
            # Получаем текущее время
            current_time = time.time()
            
            # Обновляем временные метки
            self.fs.utimens('/time_test.txt', (current_time, current_time))
            
            # Проверяем
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
            # Создаем первую базу данных
            db1 = Database(db_path)
            
            # Создаем файл через первую базу
            file_id = db1.create_entry('/', 'concurrent.txt', 
                                     stat.S_IFREG | 0o644, 1000, 1000)
            db1.write_data(file_id, 0, b'Data from connection 1')
            
            # Создаем второе соединение с той же базой
            db2 = Database(db_path)
            
            # Читаем данные через второе соединение
            data = db2.read_data(file_id, 0, 100)
            self.assertEqual(data, b'Data from connection 1')
            
            # Записываем через второе соединение (сразу после существующих данных)
            current_size = db2.get_inode(file_id)['size']
            db2.write_data(file_id, current_size, b' More data from connection 2')
            
            # Читаем через первое соединение
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
            # Пытаемся получить несуществующий путь
            result = db.get_inode_by_path('/nonexistent')
            self.assertIsNone(result)
            
            # Пытаемся удалить несуществующий файл
            with self.assertRaises(Exception):
                db.remove_entry('/', 'nonexistent')
            
            # Пытаемся создать файл в несуществующей директории
            with self.assertRaises(Exception):
                db.create_entry('/nonexistent', 'file.txt', 
                              stat.S_IFREG | 0o644, 1000, 1000)
            
            # Пытаемся прочитать из несуществующего inode
            data = db.read_data(99999, 0, 100)
            self.assertEqual(data, b'')  # Должен вернуть пустую строку
            
            # Пытаемся записать в несуществующий inode
            with self.assertRaises(Exception):
                db.write_data(99999, 0, b'data')
        finally:
            db.close()
            import shutil
            shutil.rmtree(temp_dir)


def run_tests():
    """Запуск всех тестов."""
    # Создаем тестовый набор
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestDatabase)
    
    if FUSE_AVAILABLE:
        test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestSQLiteFS))
    
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestIntegration))
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)