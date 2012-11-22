import cx_Oracle
import math
import time

#生产环境数据库连接
#dsn_tns = '(DESCRIPTION= (LOAD_BALANCE=on)(FAILOVER=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP) (HOST=172.25.24.5)(PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=172.25.24.6)(PORT=1521))) (CONNECT_DATA=(SERVICE_NAME=LMS)))'
#conn = 'lms_owt'
#pwd = 'owt_lms'

#测试环境数据库连接
dsn_tns = '''(DESCRIPTION=
    (ADDRESS=
      (PROTOCOL=TCP)
      (HOST=10.4.58.4)
      (PORT=1521)
    )
    (CONNECT_DATA=
      (SERVICE_NAME=mydfl2012)
    )
  )'''
conn = 'mynissan'
pwd = 'ecrm'


#获取游标
curso = get_curso()

def get_curso(conn, user, pwd):
    db = cx_Oracle.connect(user,pwd,conn)
    return db.cursor()

#输入活动名称
source_desc = input("Please input the campaign_name:")

#输入活动媒体 
media_names = input("Please input media names(seperated by blank space):")

#在活动名称后添加占位符，为媒体名称准备
media_campaign_name = source_desc + '-%s'

#插入父活动sql
campaign_insert = '''  insert into mynissan.crm_lead_campaign_ven (campaign_desc,is_new_car_campaign,status,create_time)
values (:campaign_desc,'0','1',sysdate)'''

#查询父活动ID sql
campaign_select = '''select source_id from lms.crm_lead_campaign where
		campaign_desc =:campaign_desc '''

#更新父活动parent字段sql
campaign_update = ''' update lms.crm_lead_campaign set fk_parent_id = :campaign_id where source_id = :campaign_id '''


source_child_insert = ''' insert into lms.crm_lead_info_source_child (SOURCE_DESC,IS_PV,IS_SHARE,FK_CAMPAIGN_ID,FK_SOURCE_ID,STATUS)
values (:source_desc,'1','0',:campaign_id,'1028','1') '''


source_child_select = ''' select source_id from lms.crm_lead_info_source_child where
        source_desc = :source_desc '''

#插入子活动sql
media_campaign_insert = ''' insert into lms.crm_lead_campaign (campaign_desc,is_new_car_campaign,fk_parent_id,status,create_time,media_name)
values (:1,'0',:2,'1',sysdate,:3) '''

#查询子活动ID sql
media_campaign_select = ''' select media_name,source_id from lms.crm_lead_campaign where fk_parent_id = :campaign_id'''


curso.execute(campaign_insert,campaign_desc = source_desc)

curso.execute(campaign_select,campaign_desc = source_desc)

row_data = curso.fetchall()

#定义存放key值的数组
md5_code = []

for x in row_data:
    for y in x:
        campaign_id = y

curso.execute(source_child_insert,source_desc = source_desc,campaign_id = campaign_id)


if media_names.strip():
    #将媒体名称存入数组
    media_name_list = media_names.split()
    array = []
    #开始遍历媒体名称
    for media_name in media_name_list:
        #存入待插入的参数数组
        array.append((media_campaign_name % media_name,campaign_id,media_name))
        #根据时间戳和媒体名称生成md5码
        md5_code.append(generate_md5_code(media_name))
    curso.prepare(media_campaign_insert)
    curso.executemany(None,array)
    print(array)

curso.execute(campaign_update,campaign_id = campaign_id)
    
db.commit()

curso.execute(source_child_select,source_desc = source_desc)

row_data = curso.fetchall()

for x in row_data:
    for y in x:
        source_child_id = y

curso.execute(media_campaign_select,campaign_id = campaign_id)

campaign_data = curso.fetchall()

out_put = ''' Campaign_id = %s；Cust_info_source_child_id = %s '''

print(out_put % (campaign_id,source_child_id))
print(campaign_data)

def generate_md5_code(campaign_name):
	time = time.time()
	md5_code = hashlib.md5()
	md5_code.update((str(time) + campaign_name).encode("UTF-8"))
	return md5_code.hexdigest()

def save_md5_campaign_mapping(md5_arry, campaign_arry):
    #插入映射表sql
    mapping_sql = ''' INSERT INTO MYNISSAN.MD5_CAMPAIGN_MAPPING ID,MD5_STRING,CONTACT_WAY,CUST_INFO_SOURCE,CUST_IN
                    FO_SOURCE_CHILD,CAMPAIGN_NAME,MEDIA_NAME,
    


