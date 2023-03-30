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
        index = 0
        count = 0
        rows = []
        # iterrows 함수 : 열 데이터를 인덱스, 시리즈로 반환
        for i, row in df.iterrows():
            row['main_id'] = self.MAIN_CATEGORY_DICT[row['main_category']]
            row['sub_id'] = self.SUB_CATEGORY_DICT[row['sub_category']]
            row['platform_id'] = self.PLATFORM_DICT[row['platform']]
            rows.append((
                row['main_id'],
                row['sub_id'],
                row['platform_id'],
                row['title'],
                row['writer'],
                row['content'],
                pd.to_datetime(row['writed_at']).strftime('%Y-%m-%d %H:%M:%S'),
            ))
            index += 1

            # 데이터가 10000개가 되면 INSERT 하기 (예시로 100개)
            # url도 빼기
            if index == 10000:
                query = f"INSERT INTO news (`main_id`, `sub_id`, `platform_id`, `title`, `writer`, `content`, `writed_at`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                cursor = self.DB.cursor()
                cursor.executemany(query, rows)
                self.DB.commit()
                count += 1
                print(f'{count}번 데이터 넣기 성공')
                rows = []
                index = 0

        # 마지막에 데이터가 10000개가 안되는 나머지 데이터를 넣기
        if index != 0:
            query = f"INSERT INTO news (`main_id`, `sub_id`, `platform_id`, `title`, `writer`, `content`, `writed_at`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor = self.DB.cursor()
            cursor.executemany(query, rows)
            self.DB.commit()
            print('나머지 데이터 넣기 성공')

            
    def select_news(self, start_date=None, end_date=None, platform=None, main_category=None, sub_category = None):
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

        cursor = self.DB.cursor()
        cursor.execute(main_query)
        result = cursor.fetchall()
        for row in result:
            print(row)    

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