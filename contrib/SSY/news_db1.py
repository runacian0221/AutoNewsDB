import pymysql
import pandas as pd

class Database:

    
    # 데이터 config파일로 옮기기
    def __init__(self, configs) -> None:
        try:
            self.DB = pymysql.connect(**configs)
            print('데이터베이스 연결 성공')
        except pymysql.err.OperationalError as e:
            print("데이터베이스 연결 실패:", e)

        tmp = [l.rstrip().split(',') for l in open('./main_category', encoding='utf-8').readlines()]
        self.MAIN_CATEGORY_DICT = {v: k for k, v in tmp}
        tmp = [l.rstrip().split(',') for l in open('./sub_category', encoding='utf-8').readlines()]
        self.SUB_CATEGORY_DICT = {v: k for k, v in tmp}
        tmp = [l.rstrip().split(',') for l in open('./platform_info', encoding='utf-8').readlines()]
        self.PLATFORM_DICT = {v: k for k, v in tmp}
                

    def __del__(self) -> None:
        # 데이터베이스 연결 해제
        self.DB.close()

    def insert_news(self, df):
        table_columns = ['main_category', 'sub_category', 'content', 'platform', 'title', 'writed_at', 'writer']
            #'news' : ['news_id', 'main_id', 'sub_id', 'platform_id', 'title', 'writer', 'content', 'writed_at', 'url']           
        
        df.fillna('', inplace=True)


        # set으로 처리, 차집합,

        required_columns = set(table_columns) - set(df.columns)
        # if required_columns:
        #     raise ValueError('테이블의 캍럼갯수가 부족합니다.')
        assert not required_columns, '테이플 칼럼갯수가 부족합니다.'
                
        ## 클랜징 clean_title(), clean_content() 넣기 ##

        # 데이터 넣기 insert문
        df['main_id'] = df['main_category'].apply(lambda x: self.MAIN_CATEGORY_DICT[x])
        df['sub_id'] = df['sub_category'].apply(lambda x: self.SUB_CATEGORY_DICT[x])
        df['platform_id'] = df['platform'].apply(lambda x: self.PLATFORM_DICT[x])
        df[['title','writer','content','writed_at']].values.tolist()

        insert_cmd = f"INSERT INTO news ({','.join(table_columns)}) VALUES ({','.join('[%s]'*len(table_columns))})"

        
        for i in range(len(df.values) // 10000): # 1GB RAM
            start_idx = i * 10000
            data = [tuple(value) for value in df.values(start_idx, start_idx + 10000)]
            with self.connection.cursor() as cur:
                cur.executemany(insert_cmd, data)
            self.connection.commit()
            
        print('Insert news done!')
            
    def select_news(self, start_date=None, end_date=None, platform=None, main_category=None, sub_category = None, offset = 0, limit = None):
        where_sql = []

        if start_date and end_date:
            where_sql.append(f"writed_at BETWEEN '{start_date}' AND '{end_date}'")
        elif start_date:
            where_sql.append(f"writed_at >= '{start_date}'")
        elif end_date:
            where_sql.append(f"writed_at <= '{end_date}'")

        if main_category:
            main_id = self.MAIN_CATEGORY_DICT[main_category]
            where_sql.append(f"main_id = {main_id}")

        if sub_category:
            sub_id = self.SUB_CATEGORY_DICT[sub_category]
            where_sql.append(f"sub_id = {sub_id}")   

        if platform:
            where_sql.append(f"platform_id = {self.PLATFORM_DICT[platform]}")

        main_query = f'SELECT * FROM news'

        if where_sql:
            main_query += f' WHERE {" AND ".join(where_sql)}'

        #if limit:
            # main_query += f' LIMIT {limit} OFFSET {offset}'
            # 데이터가 10000개가 넘는다면
            # offset += limit
            # 반복

        cursor = self.DB.cursor()
        # offset, limit
        cursor.execute(main_query)
        result = cursor.fetchall()
        for row in result:
            return result   

if __name__ == '__main__':
    with open('./db_config', 'r') as f:
        lines = [l.rstrip().split('=') for l in f.readlines()]
        configs = {k.strip(): v.strip().strip("'") for k, v in lines}
        configs['port'] = int(configs['port'])
        print(configs)

    my_db = Database(configs)

    tmp = [l.rstrip().split(',') for l in open('./main_category', encoding='utf-8').readlines()]
    my_db.MAIN_CATEGORY_DICT = {v: k for k, v in tmp}
    tmp = [l.rstrip().split(',') for l in open('./sub_category', encoding='utf-8').readlines()]
    my_db.SUB_CATEGORY_DICT = {v: k for k, v in tmp}
    tmp = [l.rstrip().split(',') for l in open('./platform_info', encoding='utf-8').readlines()]
    my_db.PLATFORM_DICT = {v: k for k, v in tmp}

    my_db.cursor = my_db.DB.cursor()