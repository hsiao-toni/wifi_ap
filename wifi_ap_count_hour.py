import sys
import pymongo
from pymongo import MongoClient
import pandas as pd
import csv
import os
from datetime import datetime ,timedelta
import time
import  wifi_ap_count_hour_sub  as sub

#可以連續執行，不用手動新增刪除假資料，並可以每日執行一次

#判斷是否第一次執行，新增假資料
if sub.WIFI_CheckData("AP_test", "Usage_Hour_count")==0:
    print("No.1")
    odd_data=sub.WIFI_OldData("AP_test", "Controller4")

    if(odd_data!=False):

            odd_data1=odd_data[0]["Datetime"].date()-timedelta(days=1)
            odd_start1=datetime.combine(odd_data1,datetime.min.time())

            insert_fake_data=[]
            insert_fake_data.append({"ap_name":str(odd_data[0]['ap_name']),"radio_band":str(odd_data[0]['radio_band']),"rx_total":0,"tx_total":0,"DateTime":odd_start1})
            sub.WIFI_WriteInDB("AP_test","Usage_Hour_count",insert_fake_data)
            
            data_find_id=sub.WIFI_LastData("AP_test", "Usage_Hour_count")
            del_find_id=data_find_id[0]['_id']           
            tag=1
    else:
            print("No Odd data")
            tag=0
else:
     print("Not No.1")

     tag=0


#while(1):
#取得目前最新一筆資料的時間
last_data=sub.WIFI_LastData("AP_test", "Usage_Hour_count")

if(last_data!=False):#如果有找到最後一筆資料
    #轉化取得最新一筆資料
    last_data= last_data[0]["DateTime"].date()+timedelta(days=1)
     #取當前時間的整點 為了判斷是否要處理資料
    now=datetime.now()
    now_hour=datetime(now.year, now.month, now.day, now.hour, 0, 0)
    start_hour=datetime.combine(last_data,datetime.min.time())
    print(now_hour)
    print(start_hour)
    
##判斷不是今天>>>>>>>>>處理資料
    while(1):
        end_hour=start_hour+timedelta(hours=1)
        print(end_hour)
        # 將 end_hour 的時間部分設置為和 start_hour 相同的時間
        #end_hour_iso = end_hour.isoformat()

        # print(end_hour_iso)
        if(start_hour<now_hour):#判斷這個小時過完了>>>處理資料
            #尋找該日期資料
            Search1={'$and': [
            {"Datetime": {'$gt':start_hour,"$lte":end_hour} }, {"radio_band": {"$in": ["0", "1"]}}]}  # First condition
            data_1=sub.WIFI_FindData("AP_test","Controller4",Search1)
            #如果有找到符合搜尋條件的所有資料
            if(data_1!=False):
                AP_data={}
                insert_data=[]
            
                for i in range(len(data_1)):
                    if str(data_1[i]['ap_name']) is not None and str(data_1[i]['ap_name']) != '' and str(data_1[i]['radio_band']) is not None and  str(data_1[i]['radio_band'])!= '' : 
                        ap_name = str(data_1[i]['ap_name'])
                        radio_band = str(data_1[i]['radio_band'])
                        # 檢查是否已存在 ap_name 和 radio_band 的組合
                        key = (ap_name, radio_band)  # 以 ap_name 和 radio_band 組合為 key

                        if key not in AP_data:
                            AP_data[key] = len(insert_data) # 將索引儲存到字典中
                            insert_data.append({
                            "ap_name": str(data_1[i]['ap_name']),
                            "radio_band": str(data_1[i]['radio_band']),
                            "sta_count": 0,
                            "count": 0,
                            "sta_count_avg": 0,
                            "rx_total": 0,
                            "tx_total": 0,
                            "DateTime": start_hour
                            })
                
                        
                        #處理rx,tx,sta_count
                        rx_data_bytes = data_1[i].get("rx_data_bytes")
                        tx_data_bytes = data_1[i].get("tx_bytes_transmitted")
                        sta_count= data_1[i].get("sta_count")
                        #資料非空值再去處理資料
                        if rx_data_bytes is not None and rx_data_bytes != '' and  tx_data_bytes is not None and tx_data_bytes!= ''  and  sta_count is not None and sta_count!= '' :
                                inwhere = AP_data[key]
                                insert_data[inwhere]["rx_total"]=insert_data[inwhere]["rx_total"]+int(data_1[i]['rx_data_bytes'])*5
                                insert_data[inwhere]["tx_total"]=insert_data[inwhere]['tx_total']+int(data_1[i]['tx_bytes_transmitted'])*5
                                insert_data[inwhere]["count"]=insert_data[inwhere]['count']+1
                                insert_data[inwhere]["sta_count"]=insert_data[inwhere]['sta_count']+int(data_1[i]['sta_count'])
                                insert_data[inwhere]["sta_count_avg"]=insert_data[inwhere]["sta_count"]/insert_data[inwhere]["count"]                         
                        else:
                    
                            print("Missing data")
                    else:
                        print("this data no ap_name/radio_band")             
                #寫入資料
                sub.WIFI_WriteInDB("AP_test","Usage_Hour_count",insert_data)
                print("Success")  
           
            else:
                print("No data")
        #今天尚未過完>>>>>>>>>暫不處理
        else:
            print("Wait!")
        
        start_hour=end_hour  
        
else:
   
    print("No have last data")
   
    #break

#判斷是否第一次執行，刪除假資料
if tag==1:
    if sub.WIFI_CheckData("AP_test", "Usage_Hour_count")==0: 
        sub.WIFI_DelData("AP_test","Usage_Hour_count",del_find_id)    
        print("fake del")
else:
     print("no fake no del")