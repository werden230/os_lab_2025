#!/usr/bin/env python3
from fuse import FUSE, FuseOSError, Operations, fuse_get_context
import os
import errno
import stat
import time
from pathlib import Path
from database import Database


class SQLiteFS(Operations):
    def __init__(self, database_path):
        self.db = Database(database_path)
        self.fd_counter = 0
        self.open_files = {}
    
    def _split_path(self, path):
        """Разделение пути на директорию и имя файла"""
        if path == '/':
            return '/', ''
        return os.path.split(path)
    
    def getattr(self, path, fh=None):
        """Получение атрибутов файла/директории"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        uid, gid, pid = fuse_get_context()
        
        return {
            'st_mode': inode['mode'],
            'st_nlink': inode['nlink'],
            'st_uid': inode['uid'],
            'st_gid': inode['gid'],
            'st_size': inode['size'],
            'st_atime': inode['atime'],
            'st_mtime': inode['mtime'],
            'st_ctime': inode['ctime'],
            'st_blocks': (inode['size'] + 511) // 512,
            'st_blksize': 4096
        }
    
    def readdir(self, path, fh):
        """Список содержимого директории"""
        entries = self.db.list_directory(path)
        return ['.', '..'] + entries
    
    def mkdir(self, path, mode):
        """Создание директории"""
        dirname, basename = self._split_path(path)
        uid, gid, pid = fuse_get_context()
        
        parent_inode = self.db.get_inode_by_path(dirname)
        if not parent_inode:
            raise FuseOSError(errno.ENOENT)
        
        existing = self.db.get_inode_by_path(path)
        if existing:
            raise FuseOSError(errno.EEXIST)
        
        if mode == 0:
            mode = 0o755  # rwxr-xr-x
        
        # Создаем директорию с правильными правами
        dir_mode = (mode & 0o777) | stat.S_IFDIR
        try:
            self.db.create_entry(dirname, basename, dir_mode, uid, gid)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def create(self, path, mode, fi=None):
        """Создание файла"""
        dirname, basename = self._split_path(path)
        uid, gid, pid = fuse_get_context()
        
        # Проверяем, существует ли родительская директория
        parent_inode = self.db.get_inode_by_path(dirname)
        if not parent_inode:
            raise FuseOSError(errno.ENOENT)
        
        existing = self.db.get_inode_by_path(path)
        if existing:
            raise FuseOSError(errno.EEXIST)
        
        if mode == 0:
            mode = 0o644  # rw-r--r--
        
        # Создаем файл с правильными правами
        file_mode = (mode & 0o777) | stat.S_IFREG
        try:
            inode_id = self.db.create_entry(dirname, basename, file_mode, uid, gid)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        # Создаем файловый дескриптор
        self.fd_counter += 1
        fd = self.fd_counter
        self.open_files[fd] = {
            'path': path,
            'inode_id': inode_id,
            'flags': os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        }
        
        return fd
    
    def open(self, path, flags):
        """Открытие файла"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        if stat.S_ISDIR(inode['mode']):
            # Для директорий возвращаем фиктивный файловый дескриптор
            self.fd_counter += 1
            fd = self.fd_counter
            self.open_files[fd] = {
                'path': path,
                'inode_id': inode['id'],
                'flags': flags
            }
            return fd
        
        # Создаем файловый дескриптор
        self.fd_counter += 1
        fd = self.fd_counter
        self.open_files[fd] = {
            'path': path,
            'inode_id': inode['id'],
            'flags': flags
        }
        
        if flags & os.O_RDONLY or flags & os.O_RDWR:
            self.db.update_times(inode['id'], atime=time.time())
        
        return fd
    
    def read(self, path, length, offset, fh):
        """Чтение из файла"""
        if fh not in self.open_files:
            raise FuseOSError(errno.EBADF)
        
        file_info = self.open_files[fh]
        
        try:
            data = self.db.read_data(file_info['inode_id'], offset, length)
            self.db.update_times(file_info['inode_id'], atime=time.time())
            return data
        except Exception as e:
            print(f"Ошибка чтения: {e}")
            raise FuseOSError(errno.EIO)
    
    def write(self, path, data, offset, fh):
        """Запись в файл"""
        if fh not in self.open_files:
            raise FuseOSError(errno.EBADF)
        
        file_info = self.open_files[fh]
        
        try:
            written = self.db.write_data(file_info['inode_id'], offset, data)
            self.db.update_times(file_info['inode_id'], mtime=time.time())
            return written
        except Exception as e:
            print(f"Ошибка записи: {e}")
            raise FuseOSError(errno.EIO)
    
    def release(self, path, fh):
        """Закрытие файла"""
        if fh in self.open_files:
            del self.open_files[fh]
        return 0
    
    def unlink(self, path):
        """Удаление файла"""
        dirname, basename = self._split_path(path)
        
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        if stat.S_ISDIR(inode['mode']):
            raise FuseOSError(errno.EISDIR)
        
        try:
            self.db.remove_entry(dirname, basename)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def truncate(self, path, length, fh=None):
        """Изменение размера файла"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        if stat.S_ISDIR(inode['mode']):
            raise FuseOSError(errno.EISDIR)
        
        try:
            self.db.truncate(inode['id'], length)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def chmod(self, path, mode):
        """Изменение прав доступа"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        # Сохраняем тип файла, меняем только права
        new_mode = (inode['mode'] & ~0o777) | (mode & 0o777)
        
        try:
            self.db.chmod(inode['id'], new_mode)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def chown(self, path, uid, gid):
        """Изменение владельца"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        current_uid, current_gid, pid = fuse_get_context()
        
        if current_uid != 0:
            raise FuseOSError(errno.EPERM)
        
        try:
            self.db.chown(inode['id'], uid, gid)
            self.db.update_times(inode['id'], ctime=time.time())
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def rename(self, old, new):
        """Переименование/перемещение файла"""
        old_dir, old_name = self._split_path(old)
        new_dir, new_name = self._split_path(new)
        
        try:
            self.db.rename(old_dir, old_name, new_dir, new_name)
        except Exception as e:
            if "Target already exists" in str(e):
                raise FuseOSError(errno.EEXIST)
            elif "Cannot move directory" in str(e):
                raise FuseOSError(errno.EINVAL)
            else:
                raise FuseOSError(errno.EIO)
        
        return 0
    
    def utimens(self, path, times=None):
        """Обновление временных меток"""
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        if times is None:
            atime = mtime = time.time()
        else:
            atime, mtime = times
        
        try:
            self.db.update_times(inode['id'], atime=atime, mtime=mtime)
        except Exception as e:
            raise FuseOSError(errno.EIO)
        
        return 0
    
    def statfs(self, path):
        """Получение статистики файловой системы"""
        return {
            'f_bsize': 4096,
            'f_frsize': 4096,
            'f_blocks': 1000000,
            'f_bfree': 500000,
            'f_bavail': 500000,
            'f_files': 100000,
            'f_ffree': 50000,
            'f_favail': 50000,
            'f_flag': 0,
            'f_namemax': 255
        }
    
    def rmdir(self, path):
        """Удаление директории"""
        dirname, basename = self._split_path(path)
        
        inode = self.db.get_inode_by_path(path)
        if not inode:
            raise FuseOSError(errno.ENOENT)
        
        if not stat.S_ISDIR(inode['mode']):
            raise FuseOSError(errno.ENOTDIR)
        
        try:
            self.db.remove_entry(dirname, basename)
        except Exception as e:
            if "Directory not empty" in str(e):
                raise FuseOSError(errno.ENOTEMPTY)
            raise FuseOSError(errno.EIO)
        
        return 0


def main():
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='SQLite FUSE filesystem')
    parser.add_argument('mountpoint', help='Mount point')
    parser.add_argument('--database', default='sqlitefs.db', 
                       help='SQLite database file (default: sqlitefs.db)')
    parser.add_argument('--foreground', action='store_true',
                       help='Run in foreground')
    
    args = parser.parse_args()
    
    mount_point = args.mountpoint
    if not os.path.exists(mount_point):
        os.makedirs(mount_point, exist_ok=True)
    
    fs = SQLiteFS(args.database)
    
    print(f"Mounting SQLiteFS at {mount_point}")
    print(f"Database: {args.database}")
    print("Press Ctrl+C to unmount")
    
    fuse = FUSE(fs, mount_point, foreground=args.foreground, 
                nothreads=False, allow_other=False, ro=False,
                nonempty=True)


if __name__ == '__main__':
    main()
