"""
angora metadata 를 지정한 개수 만큼 자동으로 넣어주는 스크립트
mariadb 의 host, port, user, password 를 입력하고, 개수를 넣으면 자동으로 생성해준다
"""
import pymysql
import argparse
import uuid
import json


def run(args):
    host = args.host
    port = args.port
    user = args.user
    password = args.password
    num = args.num

    conn = pymysql.connect(host=host, port=port, user=user, password=password, db='ANGORA_METASTORE', charset='utf8')
    cur = conn.cursor()

    len_num = len(str(num))
    name = str(uuid.uuid4())[:-len_num - 1]
    print('=' * 30)
    print(f"Temp datamodel name will be '{name}_<number>'")
    print('=' * 30)

    base_query = "insert into ANGORA_DATAMODELS (NAME, USERID, ID, DATASET, FIELDS) values (%s, %s, %s, %s, %s)"
    datas = []
    for n in range(num):
        new_name = name + '_' + f"{n:0{len_num}}"
        datas.append([
            new_name,
            'root',
            new_name,
            json.dumps({"format": "iris", "table": "NOTABLE"}),
            json.dumps([{"name": "HOST", "type": "TEXT", "alias": ""}]),
        ])

    start_msg = f"Start inserting {num} data to maraidb!"
    print('+' * len(start_msg))
    print(start_msg)
    cur.executemany(base_query, datas)
    end_msg = f"End inserting {num} data to maraidb!"
    print(end_msg)
    print('+' * len(start_msg))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Insert temp data to angora data-model.')
    parser.add_argument('num', metavar='N', type=int, help='How many data do you want to insert?')
    parser.add_argument('--host', type=str, required=True, help='Host ip of angora metastore(mariadb)')
    parser.add_argument('--port', type=int, required=True, help='port of angora metastore(mariadb)')
    parser.add_argument('-u', '--user', type=str, required=True, help='user of angora metastore(mariadb)')
    parser.add_argument('-p', '--password', type=str, required=True, help='password of angora metastore(mariadb)')

    args = parser.parse_args()
    print(args)

    run(args)
