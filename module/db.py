# 여기에 통합본 업로드 예정
import pymysql
import pandas as pd

from .cleansing import cleansing

class NewsDB:

    def __init__(self, configs) -> None:
        try:
            configs['port'] = int(configs.pop('port'))
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

    def insert_news(self, df:pd.DataFrame):
        required_columns = ['main_category', 'sub_category', 'content', 'platform', 'title', 'writed_at', 'writer']
        assert not set(required_columns) - set(df.columns), '테이플 칼럼갯수가 부족합니다.'

        df.fillna('', inplace=True)

        ## 클랜징 clean_title(), clean_content() 넣기 ##
        df['title'] = df['title'].apply(lambda x : x.replace('\n', ' ')[:160])
        df['content'] = df.apply(lambda x: cleansing(x['content'], x['writer'] if x['writer'] else ''), axis=1)

        df['main_id'] = df['main_category'].apply(lambda x: self.MAIN_CATEGORY_DICT[x])
        df['sub_id'] = df['sub_category'].apply(lambda x: self.SUB_CATEGORY_DICT[x])
        df['platform_id'] = df['platform'].apply(lambda x: self.PLATFORM_DICT[x])

        try:
            df['url'] = df['url']
        except:
            df['url'] = ''

        # 데이터 넣기 insert문
        news_column = ['main_id', 'sub_id', 'platform_id', 'title', 'writer', 
                       'content', 'writed_at', 'url']
        insert_sql = f"INSERT INTO news (`{'`,`'.join(news_column)}`) VALUES ({','.join(['%s']*len(news_column))})"
        for i in range((len(df.values) // 10000)+1): # 1GB RAM
            start_idx = i * 10000
            data = [tuple(value) for value in df[news_column].values[start_idx:start_idx+10000]]
            with self.DB.cursor() as cur:
                cur.executemany(insert_sql, data)
            self.DB.commit()
            
        print('Insert news done!')

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

        # 1GB Ram 제한 (limit, offset)
        pagination_sql = ' LIMIT 100000 OFFSET {}'
        offset = 0
        final_result = []
        while True:
            with self.DB.cursor() as cur:
                cur.execute(main_query + pagination_sql.format(offset))
                result = cur.fetchall()
                final_result.extend(result)

            if len(result) < 100000:
                break

            offset += 100000 # LIMIT

        news_column = ['news_id','title', 'writer','content',  'writed_at','url','main_category','sub_category', 'platform']
        df = pd.DataFrame(final_result, columns=news_column)
        self.MAIN_CATEGORY_ID2NAME = {int(v): k for k, v in self.MAIN_CATEGORY_DICT.items()}
        self.SUB_CATEGORY_ID2NAME = {int(v): k for k, v in self.SUB_CATEGORY_DICT.items()}
        self.PLATFORM_ID2NAME = {int(v): k for k, v in self.PLATFORM_DICT.items()}

        df['main_category'] = df['main_category'].map(self.MAIN_CATEGORY_ID2NAME)
        df['sub_category'] = df['sub_category'].map(self.SUB_CATEGORY_ID2NAME)
        df['platform'] = df['platform'].map(self.PLATFORM_ID2NAME)

        return df
