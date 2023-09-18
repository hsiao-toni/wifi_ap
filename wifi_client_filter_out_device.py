import sys
from bson import ObjectId
import pymongo
from pymongo import MongoClient
import pandas as pd
import csv
import os
from datetime import datetime
import time



mongo_url_01 =  "mongodb://admin:bmwee8097218@140.118.122.115:30415/?connectTimeoutMS=30000&socketTimeoutMS=30000"
mongo_url_02 =  "mongodb://admin:bmwee8097218@140.118.122.115:30415/?connectTimeoutMS=30000&socketTimeoutMS=30000"


def WIFI_WriteInDB(DB, Collection, new_data):
    global mongo_url_01, mongo_url_02
    try:
        conn = MongoClient(mongo_url_01)
        db = conn[DB]
        collection = db[Collection]
        collection.insert_many(new_data)
    except:

        conn = MongoClient(mongo_url_02)
        db = conn[DB]
        collection = db[Collection]
        collection.insert_many(new_data)


def WIFI_AllData(DB, Collection):
    global mongo_url_01, mongo_url_02
    try:
        conn = MongoClient(mongo_url_01)
        db = conn[DB]
        collection = db[Collection]
        cursor = collection.find()
        data = [d for d in cursor]
    except:
        conn = MongoClient(mongo_url_02)
        db = conn[DB]
        collection = db[Collection]
        cursor = collection.find()
        data = [d for d in cursor]
    if data == []:
        return False
    else:
        return data

# 提取原始数据
raw_data =  WIFI_AllData('AP_test','April_Client')
print("ok")


# 在 Python 中进行处理和判断，找到每个用户连接了不同 ap_identifier 种类数量最多的设备
user_devices = {}

for document in raw_data:
    # 获取用户名、设备 MAC 和 AP 标识符
    user_name = document.get("client_user_name", "")
    device_mac = document.get("sta_mac_address", "")
    ap_identifier = document.get("ap_name", "")

    # 检查用户名中是否包含@，然后进行相应的处理
    if '@' in user_name:
        user_name = user_name.split('@')[0].lower()
    else:
        user_name = user_name.lower()

    if user_name and device_mac and ap_identifier:
        key = f"{user_name}_{device_mac}"
    if key not in user_devices:
        user_devices[key] = {"user_name": user_name, "device_mac": device_mac, "ap_identifiers": set()}
    user_devices[key]["ap_identifiers"].add(ap_identifier)
    
# 找到每个用户连接了 ap_identifier 种类数量最多的设备
top_devices = {}

# 考虑同一用户可能只有一个设备的情况
for key, data in user_devices.items():
    user_name = data["user_name"]
    if user_name not in top_devices:
        top_devices[user_name] = [data]
    else:
        top_devices[user_name].append(data)


# 找到每个用户连接了不同 ap_identifier 种类数量最多的设备
final_top_devices = []

for user_name, devices in top_devices.items():
    devices.sort(key=lambda x: len(x["ap_identifiers"]), reverse=True)
    top_device = devices[0]
    final_top_devices.append({
        "user_name": top_device["user_name"],
        "device_mac": top_device["device_mac"],
        "ap_identifiers_count": len(top_device["ap_identifiers"])
    })

# 打印最终结果
#for device in final_top_devices:
    #print(device)
WIFI_WriteInDB("AP_test","MACaddress_test3" , final_top_devices )



# 创建一个集合来存储要保留的设备 MAC 地址
set_of_device_macs_to_keep = set()

# 填充要保留的设备 MAC 地址集合
for top_device in final_top_devices:
    set_of_device_macs_to_keep.add(top_device["device_mac"])

# 创建一个列表来存储要插入新数据库的文档
documents_to_insert = []

for document in raw_data:
    # 获取设备 MAC
    device_mac = document.get("sta_mac_address", "")

    # 检查是否需要保留此设备
    if device_mac in set_of_device_macs_to_keep:
        # 将此文档添加到要插入的文档列表中
        documents_to_insert.append(document)
print(documents_to_insert)
# 插入要保留的文档到新的数据库
WIFI_WriteInDB("AP_test","MACaddress_test4" , documents_to_insert )




