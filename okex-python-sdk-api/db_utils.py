import sqlite3

conn = sqlite3.connect('darkCoin.db')


def exeUpdate(sql):
    return exeNoQuerySql(sql)

def exeDelete(sql):
    return exeNoQuerySql(sql)

def exeInsert(sql):
    return exeNoQuerySql(sql)

#执行Insert Delete Update
def exeNoQuerySql(sql):
    print(f'exeNoQuerySql:{sql}')
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.rowcount
        cur.close()
        conn.commit()
    except BaseException as ex:
        print(f" db insert error: {ex}")
    return 1

#执行Insert Delete Update
def exeQuery(sql):
    print(f'exeQuery:{sql}')
    try:
        cur = conn.cursor()
        cur.execute(sql)
        #使用featchall获得结果集（list）
        values = cur.fetchall()
        print('exeQuery.result:', values) #result:[('1', 'Michael')]
        #关闭cursor
        #关闭conn
        cur.close()
        return values
    except BaseException as ex:
        print(f" db insert error: {ex}")
    return []

def saveLog(sql):
    print(f'exec.sql:{sql}')
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cur.rowcount
        cur.close()
        conn.commit()
    except BaseException as ex:
        print(f" db insert error: {ex}")
    return 1