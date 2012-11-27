import cx_Oracle
import math
import time
import hashlib

#生产环境数据库连接
#conn = '(DESCRIPTION= (LOAD_BALANCE=on)(FAILOVER=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP) (HOST=172.25.24.5)(PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=172.25.24.6)(PORT=1521))) (CONNECT_DATA=(SERVICE_NAME=LMS)))'
#user = 'lms_owt'
#pwd = 'owt_lms'

#测试环境数据库连接
conn = '''(DESCRIPTION=
    (ADDRESS=
      (PROTOCOL=TCP)
      (HOST=10.4.58.4)
      (PORT=1521)
    )
    (CONNECT_DATA=
      (SERVICE_NAME=mydfl2012)
    )
  )'''
user = 'mynissan'
pwd = 'ecrm'
db = cx_Oracle.connect(user,pwd,conn)

def get_curso(conn, user, pwd):
    
    return db.cursor()


#插入key值映射到数据表中
def save_md5_campaign_mapping(campaign_arry, curso):
    #获取插入用sql
    mapping_arry = fetch_mapping_arry(campaign_arry)
    #插入映射表sql
    mapping_sql = ''' INSERT INTO MYNISSAN.serial_campaign_mapping (ID,MD5_STRING,CONTACT_WAY,CUST_INFO_SOURCE,CUST_INFO_SOURCE_CHILD
                    ,CAMPAIGN_ID,CAMPAIGN_NAME,MEDIA_NAME,SOURCE_TYPE,SOURCE_TYPE_CHILD,CAMPAIGN_CODE) 
                    SELECT MYNISSAN.SEQ_serial_campaign_mapping.NEXTVAL,:1,'103','1028',:2,:3,:4,:5,'others','child','1' from dual
                    '''
    curso.prepare(mapping_sql)
    curso.executemany(None,mapping_arry)

#生成映射用数组
def fetch_mapping_arry(campaign_arry):
    i = 0
    mapping_arry = []
    for x in campaign_arry:
        #print((generate_md5_code(x[1]),source_child_id,x[1],x[2],x[0]))
        mapping_arry.append((generate_md5_code(x[1]),source_child_id,x[1],x[2],x[0]))
    return mapping_arry


#根据活动名称和时间戳生成活动对应的key值
def generate_md5_code(campaign_name):
	current_time = time.time()
	md5_code = hashlib.md5()
	md5_code.update((str(current_time) + str(campaign_name)).encode("UTF-8"))
	return md5_code.hexdigest()




#获取游标
curso = get_curso(conn, user, pwd)


#输入活动名称
source_desc = input("Please input the campaign_name:")

#输入活动媒体 
media_names = input("Please input media names(seperated by blank space):")

#在活动名称后添加占位符，为媒体名称准备
media_campaign_name = source_desc + '-%s'

#----------开始定义SQL----------
#插入父活动sql
campaign_insert = '''  insert into mynissan.crm_lead_campaign_ven (source_id,campaign_desc,is_new_car_campaign,status,create_time)
 select mynissan.seq_crm_lead_campaign_ven.nextval,:campaign_desc,'0','1',sysdate from dual'''

#查询父活动ID sql
campaign_select = '''select source_id from mynissan.crm_lead_campaign_ven where
		campaign_desc =:campaign_desc '''

#更新父活动parent字段sql
campaign_update = ''' update mynissan.crm_lead_campaign_ven set fk_parent_id = :campaign_id where source_id = :campaign_id '''


source_child_insert = ''' insert into mynissan.crm_lead_info_source_child_ven (source_id,SOURCE_DESC,IS_PV,IS_SHARE,FK_CAMPAIGN_ID,FK_SOURCE_ID,STATUS)
 select mynissan.seq_crm_info_source_child_ven.nextval,:source_desc,'1','0',:campaign_id,'1028','1' from dual'''


source_child_select = ''' select source_id from mynissan.crm_lead_info_source_child_ven where
        source_desc = :source_desc '''

#插入子活动sql
media_campaign_insert = ''' insert into mynissan.crm_lead_campaign_ven (source_id,campaign_desc,is_new_car_campaign,fk_parent_id,status,create_time,media_name)
select mynissan.seq_crm_lead_campaign_ven.nextval,:1,'0',:2,'1',sysdate,:3 from dual '''

#查询子活动ID sql
media_campaign_select = ''' select media_name,source_id,campaign_desc from mynissan.crm_lead_campaign_ven where fk_parent_id = :campaign_id
                        and media_name is not null'''

#查询key值和活动基础数据
md5_mapping_select = ''' select media_name,md5_string,campaign_id from mynissan.serial_campaign_mapping where cust_info_source_child = :cust_info_source_child
                    and media_name is not null'''
#----------完成定义SQL----------

#----------开始添加活动基础数据----------
#插入父活动
curso.execute(campaign_insert,campaign_desc = source_desc)

#查询父活动ID
curso.execute(campaign_select,campaign_desc = source_desc)

row_data = curso.fetchall()

for x in row_data:
    for y in x:
        campaign_id = y

#将父活动ID更新到父活动记录的fk_parent_id字段中
curso.execute(campaign_update,campaign_id = campaign_id)
#插入一条记录到crm_lead_info_source_child
curso.execute(source_child_insert,source_desc = source_desc,campaign_id = campaign_id)

#开始遍历参加活动的媒体名称
if media_names.strip():
    #将媒体名称存入数组
    media_name_list = media_names.split()
    array = []
    #开始遍历媒体名称
    for media_name in media_name_list:
        #存入待插入的参数数组
        array.append((media_campaign_name % media_name,campaign_id,media_name))
    #将子活动插入活动表
    curso.prepare(media_campaign_insert)
    curso.executemany(None,array)
    #print(array)
    
db.commit()
#----------完成活动基础数据的添加----------

#获取cust_info_source_child
curso.execute(source_child_select,source_desc = source_desc)

row_data = curso.fetchall()

for x in row_data:
    for y in x:
        source_child_id = y

#获取各活动子类的基础数据
curso.execute(media_campaign_select,campaign_id = campaign_id)

campaign_data = curso.fetchall()

#print(campaign_data)

#开始执行key值到活动基础数据的映射
save_md5_campaign_mapping(campaign_data, curso)
db.commit()

#查询key值和基础数据的映射
curso.execute(md5_mapping_select,cust_info_source_child = source_child_id)

key_mapping_data = curso.fetchall()

for x in key_mapping_data:
    print(x)





    
    


