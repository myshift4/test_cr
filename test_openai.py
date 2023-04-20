import sys,os
import pymysql.cursors
import pandas as pd
from datetime import datetime
from clickhouse_driver import Client

# 连接ClickHouse
def get_ck_execute(run_sql):
    ck_conn = Client(host='192.168.119.136', port=9000, user='default', password='123456', database=app, send_receive_timeout=5)
    result = ck_conn.execute(run_sql)
    return result

def main(run_date):
    data_type_list = {"7d":"7d","14d":"14d","21d":"21d","0m":"30d","1m":"60d","2m":"90d","3m":"120d","4m":"150d"}
    result = pd.DataFrame()
    for type_key in data_type_list:
        data = None
        y = run_date.split('-')[0]
        datafile = "/home/rsynccheck/funnel_pay_info/ml/{}_EMdata/usedata{}_{}.csv".format(y,type_key,run_date)
        print("filename:", datafile)
        if os.path.exists(datafile):
            data = pd.read_csv(datafile, sep=',', encoding='utf-8')
            #针对一些关键指标来检查,新只针对付费玩家
            data = data[data['pay_money_'+data_type_list[type_key]] > 0]
            data = data[(data['login_count'] <= 1) & (data['gems_change_count'] <= 0) & (data['add_hero_count'] <= 0) & (data['player_exp_change_count'] <= 0)]

            #插入前检验有没有行为数据，如果新映射的user_id也没有行为数据，那就不改变。
            sql = """select user_id,
                    argMax(behavior['log_login_counts'], update_time) as login_count,
                    argMax(behavior['log_player_exp_change_counts'], update_time) as player_exp_change_count,
                    argMax(behavior['log_add_hero_counts'], update_time) as add_hero_count,
                    argMax(behavior['log_gems_change_counts'], update_time) as gems_change_count
                    from em.bi_mleltv_behavior 
                    where rev_type='{0}' and user_id in {1} 
                    group by user_id;""".format(data_type_list[type_key], tuple(data['user_id'].astype(str).to_list()))
            tmp = get_ck_execute(sql)
            tmp = pd.DataFrame(tmp, columns=['user_id','login_count', 'player_exp_change_count', 'add_hero_count','gems_change_count'])
            tmp = tmp.fillna(0)           
            tmp = tmp[(tmp['login_count'] >= 1) & (tmp['gems_change_count'] > 0) & (tmp['add_hero_count'] > 0) & (tmp['player_exp_change_count'] > 0)]
            tmp['user_id'] = tmp['user_id'].astype(int)
            result = pd.merge(tmp,data,on=['user_id'], how='left')
            result.to_csv("/data1/mleltv_tmp/check_getdata_result/data/{0}_{1}.csv".format(run_date,type_key))
        else:
            print("file not exist: ", datafile)
            continue
    
if __name__ == '__main__':
    #获取有付费但是没有行为数据的那部分uuid,先针对这部分uuid去修正他们所映射的user_id,这种方式最简单且最快
    date = str(sys.argv[1])
    main(date)
    print('run_finish aaa')


