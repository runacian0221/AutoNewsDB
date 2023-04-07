import sys

from module.db import NewsDB
from module.secret import read_config

if __name__ == '__main__':
    arg_list = sys.argv[1:]
    for i, arg in enumerate(arg_list):
        if '=' not in arg:
            arg_list[i-1] += ' ' + arg
            arg_list[i] = ''

    args = [arg.split('=') for arg in arg_list if arg]
    args = {k: v for k, v in args}
    print(args)

    filename = args.pop('filename')
    
    configs = read_config('./db_config')
    # print(configs)

    my_db = NewsDB(configs)
    
    # DB에서 select 후 file로 저장
    df = my_db.select_news(**args)
    df.to_csv(filename, index=None)
    