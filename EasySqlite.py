import sqlite3
import os
import pickle

class EasySqlite(object):
    def __init__(self, db_path, partition_num=1):
        assert partition_num>=1 and partition_num<=1000
        if os.path.isdir(db_path):
            os.makedirs(db_path, exist_ok=True)
            if partition_num==1:
                paths = [(db_path, 'easysqlite_data.db'  )]
            else:
                paths = [os.path.join(db_path, 'easysqlite_data_%03d_%03d.db' % (i, partition_num)) for i in range(partition_num)]
        else:
            dir_name, file_name = os.path.dirname(db_path), os.path.basename(db_path).split('.')[0] #db_path.split('.')[0]
            os.makedirs(dir_name, exist_ok=True)
            paths = [os.path.join(dir_name, f'{file_name}_%03d_%03d.db' % (i, partition_num)) for i in range(partition_num)] 
         
        self.partition_num = partition_num
        self.partition_cur = 0
        self.conn_list = [sqlite3.connect(tmp) for tmp in paths]
        self.cursor_list = [conn.cursor() for conn in self.conn_list]
        self.intervals, self.total_length = [], 0
        self.table_name = 'data'
        self.put_sql = f'insert into {self.table_name} (data) values (?)'
        self.get_sql = f'select data from {self.table_name} where rowid=?'

        self.execute(f'create table if not exists {self.table_name} (data BLOB)')
        self.init_length()
        # print('Psqlite3 length:', self.total_length)
    
    def init_length(self):
        self.intervals = []
        num = 0
        min_nums = -1
        self.total_length = 0
        for i, cursor in enumerate(self.cursor_list):
            cursor.execute(f'select max(rowid) from {self.table_name}')
            nums = cursor.fetchall()[0][0]
            if nums is None: continue
            self.total_length += nums
            if nums<min_nums or min_nums<0:
                min_nums = nums
                self.partition_cur = i 
            self.intervals.append((num, num+nums-1))
            num = num + nums
        return num
    

    def get_length(self):
        self.init_length()
        return self.total_length
    
    def put(self, data, partition=None, many=False):
        assert partition<self.partition_num
        if partition is not None:
            if many:
                data = [(pickle.dumps(tmp,protocol=pickle.HIGHEST_PROTOCOL),) for tmp in data]
                self.execute(self.put_sql, value=data, partition=partition, many=True)
                self.partition_cur = partition
            else:
                data = (pickle.dumps(data,protocol=pickle.HIGHEST_PROTOCOL),)
                self.execute(self.put_sql, value=data, partition=partition, many=False)
                self.partition_cur = partition
        else: 
            if many:
                data = [(pickle.dumps(tmp,protocol=pickle.HIGHEST_PROTOCOL),) for tmp in data]
                self.execute(self.put_sql, value=data, partition=self.partition_cur, many=True)
                self.partition_cur = (self.partition_cur+1) % self.partition_num
            else:
                data = (pickle.dumps(data,protocol=pickle.HIGHEST_PROTOCOL),)
                self.execute(self.put_sql, value=data, partition=self.partition_cur, many=False)
                self.partition_cur = (self.partition_cur+1) % self.partition_num



    def get(self, index):
        assert index < self.total_length
        if self.partition_num>1:
            partition = self.select_by_index(index)
            relative_index = index-self.intervals[partition][0] 
            rowid = relative_index+1
        else:
            partition, rowid = 0, index+1
        # print(self.intervals, f'index:{index}', f'partition:{partition}', f'rowid:{rowid}')
        assert self.intervals[partition][0]<=index<=self.intervals[partition][1]
        cursor = self.cursor_list[partition]
        cursor.execute(self.get_sql, (rowid,))
        data = cursor.fetchone()[0]
        data = pickle.loads(data)
        return data

    
    def select_by_index(self, index):
        assert self.total_length>0 
        left = 0
        right = len(self.intervals)-1
        while left <= right:
            mid = (left+right) // 2
            if index >= self.intervals[mid][0] and index <= self.intervals[mid][1]:
                return mid
            elif index<self.intervals[mid][0]:
                right = mid - 1
            else:
                left = mid + 1
        return -1
    
    def execute(self, sql, value=None, partition=None, many=False): 
        if partition is not None:
            assert partition < self.partition_num
            cursor = self.cursor_list[partition]
            if value:
                if many:
                    cursor.executemany(sql, value)
                else:
                    cursor.execute(sql, value)
            else:
                cursor.execute(sql)
        else:
            if value:
                if many:
                    for cursor in self.cursor_list: cursor.executemany(sql, value)
                else:
                    for cursor in self.cursor_list: cursor.execute(sql, value)
            else:
                for cursor in self.cursor_list: cursor.execute(sql)
    
    def commit(self, partition=None):
        if partition:
            self.conn_list[partition].commit()
        else:
            for conn in self.conn_list: conn.commit()
    
    def conn_close(self, partition=None):
        if partition:
            self.conn_list[partition].close()
        else:
            for conn in self.conn_list: conn.close()
    
    def cursor_close(self, partition=None):
        if partition:
            self.cursor_list[partition].close()
        else:
            for cursor in self.cursor_list: cursor.close()
    
    def close(self, partition=None):
        if partition: 
            self.cursor_list[partition].close()
            self.conn_list[partition].close()
        else:
            for cursor in self.cursor_list: cursor.close()
            for conn in self.conn_list: conn.close()
    


            

            
