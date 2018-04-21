# -*- coding: utf-8 -*-

import configparser
import csv
import re
import sys
from datetime import datetime
from enum import IntEnum
from operator import itemgetter

import dataset
import dateutil.parser
# ドライバをimport
import mysql.connector


class Fields_Index(IntEnum):
    PROGRAM_ID = 0
    PROGRAM_TYPE = 1
    GAME_GENRE = 2
    TITLE = 3
    KANA_TITLE = 4
    OTHER_TITLE_01 = 5
    OTHER_TITLE_02 = 6
    BROADCAST_START_ON = 9

def dump_config(conf):
    print(conf.get('database','user'))
    print(conf.get('database','passwd'))
    print(conf.get('database','db'))
    print(conf.get('database','host'))
    print(conf.get('database','port'))
    print(conf.get('database','charaset'))

def load_config():
    inifile = configparser.ConfigParser()
    inifile.read('config.ini','UTF-8')
    dump_config(inifile)
    return inifile

def get_program_types(cursor):
    sql='select id , code , name from program_types;'
    cursor.execute(sql)
    return cursor.fetchall()

def get_game_genres(cursor):
    sql='select id , name  from game_genres;'
    cursor.execute(sql)
    return cursor.fetchall()

def ensure_object(obj_dictionary_list,target_obj,compared_key):
    matched_obj = [obj for obj in obj_dictionary_list if re.fullmatch(target_obj[compared_key],obj[compared_key])]
    if len(matched_obj)!=0:
        return matched_obj[0]
    else:
        target_obj['id'] = get_last_item_id(obj_dictionary_list) + 1
        obj_dictionary_list.append(target_obj)
        return target_obj
        
def get_last_item_id(obj_dictionary_list):
    last_item = sorted(obj_dictionary_list,key=itemgetter('id'),reverse=True)[0]
    return last_item['id']

def upsert_program_types(cursor,dictionary):
    insert_dup = 'insert into program_types (id , name) values ( %(id)s , %(name)s ) on duplicate key update name = %(name)s'
    cursor.execute(insert_dup,dictionary)
    pass

def upsert_game_genres(cursor,dictionary):
    insert_dup = 'insert into game_genres (id , name) values ( %(id)s , %(name)s ) on duplicate key update name = %(name)s'
    cursor.execute(insert_dup,dictionary)
    pass

def upsert_program(cursor,program):
    insert_dup = 'insert into programs (' + \
                 'title , kana_title , other_title_01 , other_title_02 , anisoninfo_program_id , program_type_id , game_genre_id , broadcast_start_on' + \
                 ') values ('+\
                 '%(title)s , %(kana_title)s , %(other_title_01)s , %(other_title_02)s , %(program_id)s , %(program_type_id)s , %(game_genre_id)s , %(broadcast_start_on)s' +\
                 ') on duplicate key update ' + \
                 'title = %(title)s , kana_title = %(kana_title)s , other_title_01 = %(other_title_01)s , other_title_02 = %(other_title_02)s,'+\
                 'program_type_id = %(program_type_id)s , game_genre_id = %(game_genre_id)s , broadcast_start_on = %(broadcast_start_on)s;'
    cursor.execute(insert_dup,program)
    pass

def ensure_date(target):
    tmp = target
    try:
        dateutil.parser.parse(tmp)
        return target
    except:
        return '0000-00-00'

def parse_program(field,pro_type_id,game_genre_id):
    return {
        'program_id':field[Fields_Index.PROGRAM_ID],
        'program_type_id':pro_type_id,
        'game_genre_id':game_genre_id,
        'title':field[Fields_Index.TITLE],
        'kana_title':field[Fields_Index.KANA_TITLE],
        'other_title_01':field[Fields_Index.OTHER_TITLE_01],
        'other_title_02':field[Fields_Index.OTHER_TITLE_02],
        'broadcast_start_on':ensure_date(field[Fields_Index.BROADCAST_START_ON])
    }

def load_csv(reader):
    # ヘッダーを飛ばす
    header = next(reader)
    print(header)
    # 一気に読み込む
    fields = []
    for row in reader:
        fields.append(row)
    return fields


print ('starting program at',datetime.now())

conf = load_config()

args = sys.argv
file_path = args[1]

# csv読み込み
csv_file = open(file_path,'r',encoding='utf8')
reader = csv.reader(csv_file , delimiter=',',doublequote=True,lineterminator="\r\n",quotechar='"',skipinitialspace=True)

#一気に読み込む
fields = load_csv(reader)

# データベースに接続
try:
    connect = mysql.connector.connect( \
        user=conf.get('database','user'), passwd=conf.get('database','passwd'), host=conf.get('database','host'), \
        port=conf.getint('database','port'), db=conf.get('database','db'),charset=conf.get('database','charaset') \
        )
    cursor = connect.cursor(buffered=True,dictionary=True)

    # コードのマスターを取得
    program_types = get_program_types(cursor)
    game_genres = get_game_genres(cursor)

    # 一括upsert用のdictionaryリストを作成
    programs = []    
    for field in fields:

        # 番組種別をマスターから取得　なかったら作る
        tmp_program_type = {'name':field[Fields_Index.PROGRAM_TYPE]}
        pro_type = ensure_object(program_types,tmp_program_type,'name')
        
        # ゲームジャンルをマスターから取得　無かったら作る
        tmp_game_genre = {'name':field[Fields_Index.GAME_GENRE]}
        g_genre = ensure_object(game_genres,tmp_game_genre,'name')
        
        # csvのフィールドをprogramにパース
        programs.append(parse_program(field,pro_type['id'],g_genre['id']))

    # それぞれのマスターを更新
    for program_type in program_types:
        program_type['table'] = 'program_types'
        upsert_program_types(cursor,program_type)

    for game_genre in game_genres:
        game_genre['table'] = 'game_genres'
        upsert_game_genres(cursor,game_genre)
    
    # program.csv内にマスター新規追加分があった場合にデータベース上に
    # それぞれのエンティティが必要なためコミット
    connect.commit()

    # programを挿入
    for program in programs:
        upsert_program(cursor,program)


    connect.commit()
finally:
    cursor.close
    connect.close

print ('terminating program at',datetime.now())
