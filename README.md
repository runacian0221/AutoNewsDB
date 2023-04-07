# 뉴스 통합 데이터베이스 관리 레포

### 프로젝트 내려받기

```
git clone https://github.com/kdt-service/AutoNewsDB.git
```

## 0. 패키지 다운로드

### 0.1 requirements.txt

```
pip install -r requirements.txt
```

## 1. 뉴스 데이터 받아오기

### 1.1 프로젝트 폴더로 이동

```
cd AutoNewsDB
```

### 1.2 실행 명령어

> 옵션 
> 
> 0. filename=뉴스.csv
> 1. platform=다음
> 2. main_category=스포츠
> 3. sub_category=해외축구
> 4. start_date=2023-02-01
> 5. end_date=2023-02-03

```
python main.py platform=다음 main_category=스포츠 sub_category=해외축구 start_date=2023-02-01 end_date=2023-02-03 filename=뉴스.csv
```
