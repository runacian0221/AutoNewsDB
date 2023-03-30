import pymysql
import pandas as pd

class Database:
    def __init__(self) -> None:
        # 데이터베이스 연결 정보
        self.host = '127.0.0.1'
        self.port = 3306
        self.user = 'root'
        self.password = 'root'
        self.database = 'news_db'
        self.charset = 'utf8mb4'


    def connect(self):
        # 데이터베이스 연결
        try:
            self.DB = pymysql.connect(host=self.host, 
                                      port=self.port, 
                                      user=self.user, 
                                      password=self.password, 
                                      database=self.database, 
                                      charset=self.charset)
            self.cursor = self.DB.cursor()
            print("데이터베이스 연결 성공")
        except pymysql.err.OperationalError as e:
            print("데이터베이스 연결 실패:", e)

   
    def __del__(self) -> None:
        # 데이터베이스 연결 해제
        self.DB.close()


    def insert_news(self, csvfile):
        table_columns = {
            'News' : ['news_id', 'main_id', 'sub_id', 'platform_id', 'title', 'writer', 'content', 'writed_at', 'url']           
        }

        # 넣을 데이터프레임의 칼럼과 table_columns가 일치하는지 확인하고 다르면 오류출력
        df = pd.read_csv(csvfile)
        if not all(df.columns == table_columns['News']):
            raise ValueError('테이블의 캍럼이름이 일치하지 않습니다.')
                
        ## 클랜징기 clean_title(), clean_content() 넣기 ##

        # 데이터 넣기 insert문
        index = 0
        count = 0
        rows = []
        # iterrows 함수 : 행/열 데이터를 인덱스, 시리즈로 반환
        for i, row in df.iterrows():
            rows.append((
                int(row['news_id']),
                int(row['main_id']),
                int(row['sub_id']),
                int(row['platform_id']),
                str(row['title']),
                str(row['writer']),
                str(row['content']),
                pd.to_datetime(row['writed_at']).strftime('%Y-%m-%d %H:%M:%S'),
                str(row['url'])
            ))
            index += 1

            # 데이터가 100개가 되면 INSERT 하기 (예시로 100개)
            if index == 100:
                query = f"INSERT INTO News VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                self.cursor.executemany(query, rows)
                self.DB.commit()
                count += 1
                print(f'News 데이터 {count}번 데이터 넣기 성공')
                rows = []
                index = 0

        # 마지막에 데이터가 100개가 안되는 나머지 데이터를 넣기
        if index != 0:
            query = f"INSERT INTO News VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.executemany(query, rows)
            self.DB.commit()
            print('나머지 데이터 넣기 성공')



    def select_news(self):
            quary = "SELECT * FROM News WHERE main_id=1 AND sub_id=2"

            self.cursor.execute(quary)

            result = self.cursor.fetchall()
            for row in result:
                print(row)
            """
            인자 : 데이터를 꺼내올 때 사용할 parameters
            DB에 들어있는 데이터를 꺼내올 것인데, 어떻게 꺼내올지를 고민
            인자로 맏은 파라미터 별 조건을 넣은 select SQL문 작성 (1GB 램 고려)
            DB 엑세스를 줄이는 방법도 한번쯤 생각해보면 좋음
            (캐싱이라는 개념, 여기서 구현하는것은 아님)
            """

    # if __name__ == '__main__':
    #     # 테스트코드 작성

    #     pass
            
DB = Database()
DB.connect()
#DB.insert_news('news.csv')
DB.select_news()