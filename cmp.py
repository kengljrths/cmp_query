import sys
import os
import subprocess
import time
from datetime import timedelta, datetime

#Define Null Variable
FILE_NAME = ''
#LOCAL_PATH_FILE = '/Data/cmp'
LOCAL_PATH_FILE = '/Users/kengljr/Downloads'
LANDINGZONE_PATH = ''
COMMAND = ''

#If input argument more than 3 then settings these to time
if len(sys.argv) > 3 :
    try:
        hour = sys.argv[3]
        minute_condition = '>= 30' if int(sys.argv[4]) < 30 else '< 30'
        date_filter = sys.argv[5]
        
    except:
        print('python3 cmp.py <trigger_name> <download/upload> <hour> <45/15> <today/yesterday>')
else:
    
    hour = (datetime.today()-timedelta(minutes=30)).strftime('%H')
    minute_condition = '>= 30' if int(datetime.today().strftime('%M')) < 30 else '< 30'
    date_filter = (datetime.today()-timedelta(minutes=30)).strftime('%Y-%m-%d')


filter = {
    'download' : 'bytes_in',
    'upload' : 'bytes_out',
}

table = {
    'download' : 'custom.dpi_cmp',
    'upload' : 'custom.dpi_cmp_upload',
}

try:
    trigger = sys.argv[1]
    trigger_type = sys.argv[2]
except:
    print('python3 cmp.py <trigger_name> <download/upload> <hour> <45/15> <today/yesterday>')
    exit()
column = filter[trigger_type]

# filter = 100000000

TRIGGER_QUERY = {
    'content_streaming' : f'''"
        SELECT time_stamp,
        gateway,
        'content streaming' as trigger_name,
        signature_service_name,
        download
        FROM
            (
                SELECT  time_stamp,
                        gateway,
                        CASE
                            WHEN server_hostname = 'atime.live' THEN 'ATIME'
                            ELSE signature_service_name
                        END AS signature_service_name,
                        SUM({column}) AS download,
                        ROW_NUMBER()
                        over
                            (
                                partition by gateway
                                order by SUM({column}) desc
                            )
                        AS row_num
                FROM traffic.stats
                WHERE gateway NOT IN
                ( 
                    SELECT  gateway
                    FROM    {table[trigger_type]} 
                    WHERE DATE(time_stamp) =  '{date_filter}'
                    AND trigger_name = 'content streaming'
                )
                AND DATE(time_stamp) =  '{date_filter}'
                AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
                AND
                (
                    server_hostname = 'atime.live'
                    OR signature_service_name IN ('Facebook','TikTok','YouTube','Instagram','YouTube Music','Spotify','Apple Music','Joox Music')
                )
                GROUP BY 1,2,3
                HAVING SUM({column}) > 100000) df
                WHERE row_num = 1"''',

    'movie_streaming' : f'''"
        SELECT  time_stamp,
                gateway,
                'movie streaming' as trigger_name,
                signature_service_name,
                sum_bw
                FROM
                (
                    SELECT  time_stamp,
                            gateway,
                            CASE
                            WHEN signature_service_name IN ('trueID OTT + Hybrid','trueID APP+WEB TV','trueID OTT RN-PSN',
                                                            'trueID OTT RN-SKA','trueID OTT RN-AYT','trueID OTT RN-NKR',
                                                            'trueID OTT RN-KKN','trueID OTT RN-PSN','trueID OTT RN-NKT') THEN 'TrueID TV'
                            WHEN server_hostname = 'mgtv123.com' THEN 'MANGGOUTV'
                            WHEN server_hostname = 'doomovie.com' THEN 'doomovie'
                            WHEN server_hostname = 'bugaboo.tv' THEN 'BUGABOO TV'
                            ELSE signature_service_name
                            END AS signature_service_name,
                            sum({column}) AS sum_bw,
                            ROW_NUMBER() over
                            (
                                partition by gateway
                                order by sum({column}) desc
                            ) AS row_num
                    FROM traffic.stats
                    WHERE gateway NOT IN
                    (
                        SELECT  gateway
                        FROM    {table[trigger_type]} 
                        WHERE DATE(time_stamp) = '{date_filter}'
                        AND trigger_name = 'movie streaming'
                    )
                    AND DATE(time_stamp) = '{date_filter}'
                    AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
                    AND (
                            server_hostname IN ('doomovie.com','bugaboo.tv','mgtv123.com')
                            OR signature_service_name IN ('Netflix', 'HBO', 'Disney+ Hotstar', 'Viu', 'WeTV', 'HOOQ', 'iFlix', 'Apple TV+',
                                                            'LINE TV', 'MONO MAX', 'iQIYI','Amazon Prime','Amazon Prime Web','trueID OTT + Hybrid',
                                                            'trueID APP+WEB TV', 'trueID OTT RN-PSN', 'trueID OTT RN-SKA', 'trueID OTT RN-AYT',
                                                            'trueID OTT RN-NKR', 'trueID OTT RN-KKN', 'trueID OTT RN-PSN', 'trueID OTT RN-NKT')
                        )
                    GROUP BY 1,2,3
                    HAVING sum({column}) > 100000
                ) df WHERE row_num = 1"''',

    'work_group' : f'''"
        SELECT  time_stamp,
                gateway,
                'work group' as trigger_name,
                signature_service_name,
                sum_bw
        FROM
        (
            SELECT  time_stamp,
                    gateway,
                    signature_service_name,
                    sum({column}) AS sum_bw,
                    ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'work group')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND signature_service_name IN (
                'Vroom_True','Microsoft', 'Microsoft Teams', 'OneDrive', 'Google Cloud Storage', 'Office 365',
                'Google Meet', 'Microsoft Azure', 'Microsoft Push Notification', 'Microsoft Auto Update', 'Outlook.com',
                'Outlook 365', 'OneNote 365', 'Office Click-to-Run', 'Microsoft BITS', 'Microsoft Connectivity Status test',
                'Zoom', 'Microsoft Teams Call', 'Sway Office 365', 'PowerPoint 365', 'Trend Micro AntiVirus',
                'Microsoft SQL Server', 'Microsoft MQ', 'Microsoft Stream', 'Microsoft Media Services', 'Microsoft Teams Consumer Call',
                'Microsoft Lync Control', 'Microsoft Lync', 'Microsoft ILS', 'Microsoft Lync Media', 'Microsoft Online Crash Analysis',
                'Microsoft Software Quality Metrics', 'Microsoft WinDbg')
            GROUP BY 1,2,3
            HAVING sum({column}) > 10000
        ) df WHERE row_num = 1"''',

    'gaming' : f'''"
        SELECT time_stamp,
        gateway,
        'gaming' as trigger_name,
        signature_service_name,
        sum_bw
        FROM (
            SELECT time_stamp,
            gateway,
            CASE
            WHEN server_hostname IN ('zepeto.me','zepeto.com') THEN 'Zepeto'
            WHEN server_hostname IN ('subwaysurfers.com','subwaysurfersgame.io','subwaysurfersgame.org','subwaysurferapk.net') THEN 'Subway Surfers'
            WHEN server_hostname IN ('lastwar.com','lastwar.wiki','lastwarpack.com','lastwartutorial.com') THEN 'Last War : Survival'
            WHEN server_hostname = 'ragnarokx.com' THEN 'ROX (Ragnarok X : Next Generation)'
            WHEN server_hostname = 'ragnaroketernallove.com' THEN 'ROM (Ragnarok M : Eternal Love)'
            WHEN server_hostname = 'rov.in.th' THEN 'ROV (Arena of Valor)'
            WHEN server_hostname = 'poki.com' THEN 'Poki'
            WHEN server_hostname = 'y8.com' THEN 'Y8'
            WHEN server_hostname = 'crazygames.com' THEN 'CrazyGames'
            WHEN server_hostname = 'playhop.com' THEN 'Playhop'
            WHEN server_hostname = 'steampowered.com' THEN 'Steam'
            WHEN server_hostname = 'codashop.com' THEN 'Codashop'
            WHEN server_hostname = 'razer.com' THEN 'Razer'
            WHEN server_hostname = 'god.in.th' THEN 'Godlike'
            WHEN server_hostname = 'lnwtrue.com' THEN 'Inwtrue'
            WHEN server_hostname = 'xbox.com' THEN 'Xbox'
            WHEN server_hostname = 'tencent.com' THEN 'Tencent'
            WHEN server_hostname = 'steamcommunity.com' THEN 'Steam Game server'
            WHEN server_hostname = 'steampowered.com' THEN 'Steam'
            ELSE signature_service_name
            END AS signature_service_name,
            sum({column}) AS sum_bw,
            ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'gaming')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND (
                server_hostname IN ('poki.com', 'y8.com', 'crazygames.com', 'playhop.com','steampowered.com', 'codashop.com', 'razer.com', 'god.in.th',
                    'lnwtrue.com', 'xbox.com', 'tencent.com', 'steamcommunity.com', 'steampowered.com',
                    'zepeto.me', 'zepeto.com', 'subwaysurfers.com','subwaysurfersgame.io','subwaysurfersgame.org','subwaysurferapk.net',
                    'lastwar.com','lastwar.wiki','lastwarpack.com','lastwartutorial.com','ragnarokx.com', 'ragnaroketernallove.com',
                    'rov.in.th')
                OR signature_service_name IN ('ROBLOX', 'Call of Duty', 'Genshin Impact','PUBG Mobile','FIFA','Garena+',
                        'Garena Free Fire')
            )
            GROUP BY 1,2,3
            HAVING sum({column}) > 100000
        ) df WHERE row_num = 1"''',

    'ais_play' : f'''"
        SELECT time_stamp,
        gateway,
        'ais play' as trigger_name,
        signature_service_name,
        sum_bw
        FROM (
            SELECT time_stamp,
            gateway,
            signature_service_name,
            sum({column}) AS sum_bw,
            ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'ais play')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND signature_service_name in ('AIS_PLAY','AIS Play')
            GROUP BY 1,2,3
            HAVING sum({column}) > 100000
        ) df WHERE row_num = 1"''',

    'visit_competitor_website' : f'''"
        SELECT time_stamp,
        gateway,
        'visit competitor website' as trigger_name,
        signature_service_name,
        sum_bw
        FROM (
            SELECT time_stamp,
            gateway,
            CASE
            WHEN signature_service_name IN ('3BB','3BB CLOUDTV','3BB Privilege','3BB Speed Test') THEN '3BB'
            WHEN server_hostname = 'ntplc.co.th' THEN 'NT Broadband'
            ELSE signature_service_name
            END AS signature_service_name,
            sum({column}) AS sum_bw,
            ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'visit competitor website')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND (
                signature_service_name IN ('My AIS','AIS Online Store','AIS','AIS Report','AIS_MOBILE_MNP',
                        'AIS_BUSSINESS','AIS_FIXEDBB','AIS FIBRE HOME','AIS_MOBILE','AIS Call Center',
                        'AIS Academy','AIS FIBRE','3BB FIBER','3BB','3BB CLOUDTV','3BB Privilege','3BB Speed Test')
                OR server_hostname = 'ntplc.co.th'
            )
            GROUP BY 1,2,3
            HAVING sum({column}) > 100000
        ) df WHERE row_num = 1"''',

    'sport' : f'''"
        SELECT time_stamp,
        gateway,
        'sport' as trigger_name,
        signature_service_name,
        sum_bw
        FROM (
            SELECT time_stamp,
            gateway,
            CASE
            WHEN signature_service_name IN ('BeIN Sports','beIN CONNECT') THEN 'beIN Sports Connect'
            WHEN server_hostname = 'truevisions.co.th' THEN 'TrueVisions Now'
            WHEN server_hostname = 'uefa.tv' THEN 'UEFA.tv'
            WHEN server_hostname IN ('livescores.com','livescores.biz','livescores.bz','livescores.pro','livescoresfootball.org') THEN 'LiveScore : Live Sports Scores'
            WHEN server_hostname = 'uefa.tv' THEN 'UEFA.tv'
            WHEN server_hostname = 'siamsport.co.th' THEN 'Siamsport'
            WHEN server_hostname = 'goal.com' THEN 'Goal'
            WHEN server_hostname = 'ballsodlive.com' THEN 'Eleven Sports'
            ELSE signature_service_name
            END AS signature_service_name,
            sum({column}) AS sum_bw,
            ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'sport')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND (
            signature_service_name IN ('DAZN','BeIN Sports','beIN CONNECT','ESPN')
            OR server_hostname IN ('truevisions.co.th', 'uefa.tv','livescores.com','livescores.biz','livescores.bz','livescores.pro',
                'livescoresfootball.org','siamsport.co.th','goal.com','ballsodlive.com')
            )
            GROUP BY 1,2,3
            HAVING sum({column}) > 100000
        ) df WHERE row_num = 1"''',

    'coupon_hub' : f'''"
        SELECT time_stamp,
        gateway,
        'coupon hub' as trigger_name,
        signature_service_name,
        sum_bw
        FROM (
            SELECT time_stamp,
            gateway,
            CASE
            WHEN server_hostname = 'temu.com' THEN 'TEMU'
            WHEN server_hostname = 'lineman.me' THEN 'Lineman'
            WHEN server_hostname = 'grab.com' THEN 'Grab'
            WHEN server_hostname = 'priceza.com' THEN 'Priceza'
            WHEN server_hostname = 'ensogo.com' THEN 'Ensogo'
            WHEN server_hostname IN ('shopback.co.th','shopback.com') THEN 'ShopBack'
            ELSE signature_service_name
            END AS signature_service_name,
            sum({column}) AS sum_bw,
            ROW_NUMBER() over(partition by gateway order by sum({column}) desc) as row_num
            FROM traffic.stats
            WHERE gateway NOT IN ( SELECT gateway
                FROM {table[trigger_type]} 
                WHERE DATE(time_stamp) = '{date_filter}'
                AND trigger_name = 'coupon hub')
            AND DATE(time_stamp) = '{date_filter}'
            AND HOUR(time_stamp) = {hour} AND MINUTE(time_stamp) {minute_condition}
            AND (
            signature_service_name IN ('Shopee','Lazada','Amazon','Amazon AWS','Taobao','TrueYou',
                    'ZALORA','eBay','Alibaba.com')
            OR server_hostname IN ('temu.com','lineman.me','grab.com','priceza.com','ensogo.com','shopback.com',
                'shopback.co.th')
            )
            GROUP BY 1,2,3
            HAVING sum({column}) > 100000
        ) df WHERE row_num = 1"'''
}

def query(sql,fullpath):
    global COMMAND
    ids_connection = '10.185.188.5 -U dbadmin -w 7q20b6eZ23ztgFET-YWW/wS9LzAzXf4104HkhCK2pQICVUQTV3A7Q.LDd-4gHRA/M8KkVe78jGoyKIMwFjIOcJxRhlgvOBtU@3hZ'
    command = "/opt/vertica/bin/vsql -h {0} -Atc {1}  > {2}".format(ids_connection,sql,fullpath)
    COMMAND = command
    # print(command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return

def update_db(fullpath):
    ids_connection = '10.185.188.5 -U dbadmin -w 7q20b6eZ23ztgFET-YWW/wS9LzAzXf4104HkhCK2pQICVUQTV3A7Q.LDd-4gHRA/M8KkVe78jGoyKIMwFjIOcJxRhlgvOBtU@3hZ'
    sql = f"COPY {table[trigger_type]} FROM LOCAL '{fullpath}' WITH DELIMITER as '|' "
    command = '/opt/vertica/bin/vsql -h {0} -c "{1}" '.format(ids_connection,sql)
    # print(command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return

def upload_landingzone(fullpath):
    command = f"sshpass -p \"Pke3*bm8gn\" sftp  procera@172.19.131.69:/LandingZone/inbound/network/procera/ <<<  $'put {fullpath}'"
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return

def run():
    full_path = f'{LOCAL_PATH_FILE}/{trigger}_{trigger_type}.csv'
    print(full_path)
    query(TRIGGER_QUERY[trigger],full_path)
    update_db(full_path)
    return full_path

def test_checkfile_resultquery(path_file):
    
    data = {}
    duplicate = False
    raw = open(path_file,'r').read().strip().split('\n')
    if raw != ['']:
        for _ in raw:
            tmp = _.split('|')
            if tmp[2] == trigger.replace('_',' '):
                pass
            else:
                print('Trigger wrong',tmp[2] , trigger.replace('_',' '),tmp)
            gateway= tmp[1]
            if gateway in data:
                duplicate = True
                data[gateway].append(tmp)
                print('gateway duplicate',data[gateway])
            else:
                data[gateway] = [tmp]

            if duplicate:
                print(COMMAND)
    print('Test done on',trigger,trigger_type,"start upload to DB")
    update_db(path_file)
    print('upload to DB done on',trigger,trigger_type,)

def test_checkdata_importDB():
    return

def test():
    path_file =  run()
    test_checkfile_resultquery(path_file)
    return

if __name__ == '__main__':

    st = time.time()
    time_stamp = datetime.today().strftime('%Y-%m-%d_%H:%M:%S')
    run()
    duration = time.time()-st
    time_end = datetime.today().strftime('%Y-%m-%d_%H:%M:%S')
    os.system(f'echo "{time_stamp} {trigger}_{column} : {duration}s, end : {time_end}" >> /Users/kengljr/Downloads/log_cmp.txt')
    print("end end")
    