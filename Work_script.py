import os
import json
import time
import wave
import glob
import shutil
import warnings
import pandas as pd
import mysql.connector
from os import path
from pydub import AudioSegment
from selenium import webdriver
from more_itertools import last
from fast_bitrix24 import Bitrix
from sqlalchemy import create_engine
from vosk import Model, KaldiRecognizer
from selenium.webdriver.common.by import By

warnings.filterwarnings('ignore')

def convert(): # Convert from mp3 to wav and decoding recordings
    mp3 = "mp3.mp3"
    wav = "voice.wav"

    sound = AudioSegment.from_mp3(mp3)
    sound.export(wav, format = 'wav')
    print('Файл конвертирован')


    model = Model(r"C:\Users\Admin\Documents\GitHub\Vosk\vosk-model-small-ru-0.22")
    freq = 16000
    wf = wave.open('voice.wav', 'rb')
    rec = KaldiRecognizer(model, freq)

    result = ''

    last_n = False

    while True:
        data = wf.readframes(freq)
        if len(data) == 0:
            break

        if rec.AcceptWaveform(data):
            res = json.loads(rec.Result())

            if res['text'] != '':
                result += f" {res['text']}"
                last_n = False
            elif not last_n:
                result += '/n'
                last_n = True

    res = json.loads(rec.FinalResult())
    result += f" {res['text']}"
    print('Запись расшифрована')

    return result

### get data from SQL

Month = 20221001 

cnx = mysql.connector.connect(user = 'user', password = 'pass', host = 'host', database = 'db')

query = f"""---- bi_decoding_rec order by id desc limit 1"""

max_id = pd.read_sql(query,cnx)

max_id = max_id['id']

for i in max_id:
    max_id = i

print(max_id)

query = f"""---
where name like '%mp3%'
and u.role_group_name = 'МПП'
and size > 90000
and date(create_time) >= {Month}
and d.id > {max_id}
order by d.id"""

data = pd.read_sql(query,cnx)

id_f = data['id']
l_n = data['last_name']
r_n = data['name']

webhook = ('webhook')
b = Bitrix(webhook)

records = []

z = 0

engine = create_engine('mysql+pymysql://user:pass@host/db')

for i in id_f:
    id = i
    print(id)
    method = 'disk.file.getExternalLink' # method which to get external link for download file from disk bitrix24

    params = {'id': id}

    res = b.call(method,params)
    res = json.dumps(res)
    res = json.loads(res)

    url = res

    down_fold = r'C:\Users\Admin\Downloads\*.mp3'
    scr_fold = r'C:\Users\Admin\Documents\GitHub\Vosk\mp3.mp3'
    path = r'C:\Users\Admin\Downloads\voice'
    path = path.replace('voice','')
    print(path)

    driver = webdriver.Chrome() # Use Selenium to download records

    driver.get(url)
    text_box = driver.find_element(by=By.TAG_NAME, value = 'a')
    text_box.click()
    time.sleep(4)
    list_of_files = glob.glob(down_fold)
    latest_file = max(list_of_files, key=os.path.getctime) # Defenition path to file
    print(latest_file,path + r_n[z])
    if latest_file == path + r_n[z]: # Check
        driver.close()
        print(z)
        shutil.move(latest_file, scr_fold)

        result = convert()
        records.append([id,l_n[z],r_n[z].replace('.mp3',''),result])
        df = [[id,l_n[z],r_n[z].replace('.mp3',''),result]]
        df = pd.DataFrame(df, columns = ['Id','last_name','name_records','Text'])
        print(df)
        print('Запись расшифрована и записана!')
        df.to_sql('bi_decoding_rec', con = engine, if_exists = 'append', index = False) # write data to database
        z+=1
        print(z)
    else: 
        driver.close()
        print('Файл не найден')
        raise
        

records = pd.DataFrame(records, columns = ['Id','last_name','name_records','Text'])
records.to_excel('data.xlsx',index = False)