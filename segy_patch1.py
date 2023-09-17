import concurrent.futures as cfs
import csv
import datetime as dt
import multiprocessing as mp
import operator
import os
import subprocess
import time

from csv import DictWriter

import numpy as np
import pandas as pd
import cProfile,pstats,io


path_N=[]


def segy(a,b,o):
    print("elemenop")
    exists=[]   
    tfsss=[]
    global kr
    

    for h in range(a,b,1):
        
        f= open(f"{o[h]}",mode='r',encoding='utf8', newline='\n')

        for i,line in enumerate(f):  
            x = line.strip("\n")
            temp=x.split("|")
            
            # print(temp)
    #         print(temp)
    #         print(f"the len of temp is {len(temp)}")
    #         print("--------------------------------------------------------------------------------------------------------------------------------------")

            if temp==[''] or temp==[''] or len(temp)<3:
                continue
            else:
                
                # print("going inside")
                [DID,CITY,LOC,i_d] = [temp[0],temp[1],temp[2],temp[3]]

    #             print(i_d)
                i_d=int(i_d)
    #             print("below is id number")
    #             print(f"the id number is {temp[3]}")
    #             print("--------------------------------------------------------------------------------------------------------------------------------------")
                if i_d == 1 or i_d == 2 or i_d == 3:
                    ts = temp[6]
    #                 print(temp[6])
                elif i_d == 4 or i_d==5 or i_d==6:
                    ts = temp[5]
    #                 print(temp[5])
                else :
                    ts = temp[67]
    #                 print(temp[7])

                date = dt.datetime.fromtimestamp(int(ts)) 
                month = date.strftime("%m")
                day = date.strftime("%d")
                time = date.strftime("%H:%M:%S")

    #             print(date)

                real_pathv1 =  "E:\ZEDI" + "\\" 
                real_pathv2 =  real_pathv1 + str(DID) # E:\ZEDI\\dev_id
                sub_path5m = real_pathv2+"\\"+"5-min" # E:\ZEDI\\dev_id\\5-min
                sub_path1s = real_pathv2+"\\"+"1-sec" # E:\ZEDI\\dev_id\\1-sec
                sub_pathfreq = sub_path1s+"\\"+"Frequency"  # E:\ZEDI\\dev_id\\1-sec\\Frequency
                sub_pathIV = sub_path1s+"\\"+"Current-Voltage" # E:\ZEDI\\dev_id\\1-sec\\Current-Voltage
                sub_pathother = sub_path5m+"\\"+"Other-Sensor" # E:\ZEDI\\dev_id\\5-min\\Other-Sensor

                try:
                    os.mkdir(real_pathv2)
    #                 print("dev folder created")
                except OSError as error:
                    pass

                if i_d == 7 :

                    try:
                        os.mkdir(sub_path5m)
    #                     print("id 7 subfolder 5m created")
                    except OSError as error:
                        pass
    #                     print(error)

                    try:
                        os.mkdir(sub_pathother)
    #                     print("id 7 subfolder oth created")
                    except OSError as error:
                        pass
    #                     print(error)     

                    path_name = sub_pathother + "\\" + day + month 

                elif i_d==1 or i_d==2 or i_d==3 or i_d==4 or i_d==5 or i_d==6 :       

                    try:
                        os.mkdir(sub_path1s)
    #                     print("id 1/2/3/4/5/6 subfolder 1s created")
                    except OSError as error:
                        pass
                        #print(error)

                    if (i_d == 1 or i_d == 2 or i_d==3):
                        try:
                            os.mkdir(sub_pathIV)
    #                         print("id 1/2/3/ 1s subfolder IV  created")
                        except OSError as error:
                            pass
                            #print(error)
                        path_name = sub_pathIV + "\\" + day + month 


                    elif (i_d == 4 or i_d == 5 or i_d==6):     
                        try:
                            os.mkdir(sub_pathfreq)
    #                         print("id 4/5/6/ 1s subfolder Freq  created")
                        except OSError as error:
                            pass
                            #print(error)
                        path_name = sub_pathfreq + "\\" + day + month 
                if path_name in path_N:
                    pass
                else :
                    path_N.append(path_name)
                    # print("path appended")
                    # print(path_N)
                    
                if (i_d==1 or i_d==2 or i_d==3):
    #                 print("going inside i_d=1/2/3 keyval")
                    ivryb = "000"
                    keyval = str(DID)+"|"+day+month+"|"+ivryb


                elif (i_d==4 or i_d==5 or i_d==6):
    #                 print("going inside i_d=4/5/6 keyval")
                    fryb = "111"
                    keyval = str(DID)+"|"+day+month+"|"+fryb


                elif (i_d==7):
    #                 print("going inside i_d=7 keyval")
                    oth = "100"
                    keyval = str(DID)+"|"+day+month+"|"+oth

    #             print(keyval)
                if keyval in exists:
    #                 print("keyval is there")
                    r=1
                else:
                    r=0
                    exists.append(keyval)
    #                 print("keyval not there")

    #             print(f"The r iis {r}")
                if (i_d == 1 or i_d == 2 or i_d==3):

                    fields = ['Time','Curr(IR)','Volt(VR)','Curr(IY)','Volt(VY)','Curr(IB)','Volt(VB)']
                    dictIVNULL = {'Time': f"{np.nan}", 'Curr(IR)': f"{np.nan}", 'Volt(VR)': f"{np.nan}",'Curr(IY)': f"{np.nan}", 'Volt(VY)': f"{np.nan}",'Curr(IB)': f"{np.nan}", 'Volt(VB)': f"{np.nan}"}

                    with open(path_name+'.csv', 'a') as f_object:
                        if r==0:
                            fields = ['Time','Curr(IR)','Volt(VR)','Curr(IY)','Volt(VY)','Curr(IB)','Volt(VB)']
                            csvwriter = csv.writer(f_object) 
                            csvwriter.writerow(fields) 

                        if i_d==1:
                            for i in range(58):
                                curr = temp[4+3*i]
                                epoch = int(temp[6+3*i])
                                volt = temp[5+3*i]

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='' ) :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIVNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictIV1 = {'Time': f"{time}", 'Curr(IR)': f'{curr}', 'Volt(VR)': f"{volt}",'Curr(IY)': f'{np.nan}', 'Volt(VY)': f"{np.nan}",'Curr(IB)': f'{np.nan}', 'Volt(VB)': f"{np.nan}"} 
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIV1)


                        elif i_d==2:
                            for i in range(58):
                                curr = temp[4+3*i]
                                epoch = int(temp[6+3*i])
                                volt = temp[5+3*i]

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='') :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIVNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictIV2 = {'Time': f"{time}", 'Curr(IR)': f'{np.nan}', 'Volt(VR)': f"{np.nan}",'Curr(IY)': f'{curr}', 'Volt(VY)': f"{volt}",'Curr(IB)': f'{np.nan}', 'Volt(VB)': f"{np.nan}"} 
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIV2)


                        elif i_d==3:
                            for i in range(58):
                                curr = temp[4+3*i]
                                epoch = int(temp[6+3*i])
                                volt = temp[5+3*i]

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='') :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIVNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictIV3 = {'Time': f"{time}", 'Curr(IR)': f'{np.nan}', 'Volt(VR)': f"{np.nan}",'Curr(IY)': f'{np.nan}', 'Volt(VY)': f"{np.nan}",'Curr(IB)': f'{curr}', 'Volt(VB)': f"{volt}"} 
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIV3)   



                elif (i_d == 4 or i_d == 5 or i_d==6):

                    fields = ['Time','Freq(R)','Freq(Y)','Freq(B)']

                    dictFNULL = {'Time': f"{np.nan}", 'Freq(R)': f"{np.nan}", 'Freq(Y)': f"{np.nan}",'Freq(B)': f"{np.nan}"}
                    with open(path_name+'.csv', 'a') as f_object:

                        if r==0:
                            csvwriter = csv.writer(f_object) 
                            csvwriter.writerow(fields) 

                        if i_d==4:
                            for i in range(58):
                                freq =  temp[4+2*i]
                                epoch = int(temp[5+2*i])

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='') :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictFNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictF4 = {'Time': f"{time}", 'Freq(R)': f"{freq}", 'Freq(Y)': f"{np.nan}",'Freq(B)': f"{np.nan}"}
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictF4)
                        elif i_d==5:
                            for i in range(58):
                                freq =  temp[4+2*i]
                                epoch = int(temp[5+2*i])

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='') :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIVNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictF5 = {'Time': f"{time}", 'Freq(R)': f"{np.nan}", 'Freq(Y)': f"{freq}",'Freq(B)': f"{np.nan}"}
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictF5)
                        elif i_d==6:
                            for i in range(58):
                                freq =  temp[4+2*i]
                                epoch = int(temp[5+2*i])

                                if (epoch=='NaN' or epoch=='NULL' or epoch=='') :
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictIVNULL)

                                else :
                                    date_inst = dt.datetime.fromtimestamp(epoch) 
                                    time = date_inst.strftime("%H:%M:%S")
                                    helpp = str(keyval)+str(time)
                                    tfsss.append(helpp)
                                    dictF6 = {'Time': f"{time}", 'Freq(R)': f"{np.nan}", 'Freq(Y)': f"{np.nan}",'Freq(B)': f"{freq}"}
                                    writer = DictWriter(f_object, fieldnames = fields) 
                                    writer.writerow(dictF6)   

                elif (i_d == 7):

    #                 print("id 7 block activated")

                #         wb = x.Workbook(path_name +".csv")

                #         wks = wb.add_worksheet(sn)
                    fields = ['Time','Temperature','Humidity','Pressure','G value','PIR1','PIR2','Co2','Therm1','Therm2','Therm3']
                    epoch = int(temp[67])
                    dictothNULL={'Time':f'{np.nan}','Temperature':f'{np.nan}','Humidity':f'{np.nan}','Pressure':f'{np.nan}',
                                 'G value':f'{np.nan}','PIR1':f'{np.nan}','PIR2':f'{np.nan}','Co2':f'{np.nan}','Therm1':f'{np.nan}','Therm2':f'{np.nan}','Therm3':f'{np.nan}'}

                    with open(path_name+'.csv', 'a') as f_object:

                        if r==0:
                            csvwriter = csv.writer(f_object) 
                            csvwriter.writerow(fields) 


                        if epoch=='NaN' or epoch=='NULL' or epoch=='':
                            writer = DictWriter(f_object, fieldnames = fields) 
                            writer.writerow(dictothNULL)

                        else :

                            date_inst = dt.datetime.fromtimestamp(epoch) 
                            time = date_inst.strftime("%H:%M:%S")
                            helpp = str(keyval)+str(time)







                            BME280T = temp[54]
                            BME280H = temp[55]
                            BME280P = temp[56]
                            BME680T = temp[57]
                            BME680H = temp[58]
                            BME680P = temp[59]
                            BME680G = temp[60]

                            PIR1 = temp[61]
                            PIR2 = temp[62]
                            CO2 = temp[63]
                            THERM1 = temp[64]
                            THERM2 = temp[65]
                            THERM3 = temp[66]  

                            if not BME280T:
                                temperature = BME680T
                            else :
                                temperature = BME280T

                            if not BME280P:
                                pressure = BME680P
                            else :
                                pressure = BME280P

                            if not BME280H:
                                humidity = BME680H
                            else :
                                humidity = BME280H  
                            dictoth={'Time':f'{time}','Temperature':f'{temperature}','Humidity':f'{humidity}',
                                         'Pressure':f'{pressure}','G value':f'{BME680G}','PIR1':f'{PIR1}','PIR2':f'{PIR2}','Co2':f'{CO2}',
                                         'Therm1':f"{THERM1}",'Therm2':f"{THERM2}",'Therm3':f"{THERM3}"}

                            writer = DictWriter(f_object, fieldnames = fields) 
                            writer.writerow(dictoth)


    #         print(path_name)
    #         print(keyval)

            f_object.close() 
            
    # for k in path_N :
    #     df = pd.read_csv(k+".csv")
    #     upd_df =  df.groupby('Time', as_index=False).first()
    #     upd_df.to_csv(k+'.csv', index=False) 
    
pr=[]
   
if  __name__ == "__main__":
    
    os.chdir("E:\\Raw Sample-20210719T145130Z-001\\Raw Sample")
    o = os.listdir()
    print(len(o))

    nos=int(len(o)/mp.cpu_count())+1
    print("NOS IS ",int(nos))
    i=0
    prc=[]
    
    while i<=len(o) :
        
        print(i)
        p=mp.Process(target=segy,args=(i,i+nos,o))
        prc.append(p)
        print(f"PROCESS {i} OBJ CREATED")
        i+=nos
        i=round(i)
        
    s_time=time.time()
    
    for pro in prc:
        pro.start()
        
    
    for pros in prc:
        pros.join()
        
        
    e_time=time.time()   
    
    print("TIME TAKEN : " , e_time-s_time )
    
    print(prc)
    
    
    
    
    
    
    
    
    # mp.set_start_method('spawn')
    # p1=mp.Process(target=segy,args=(0,2000))
    # p2=mp.Process(target=segy,args=(2001,4000))
    # p3=mp.Process(target=segy,args=(4001,6000))
    # p4=mp.Process(target=segy,args=(6001,8000))
    # p5=mp.Process(target=segy,args=(8001,10000))
    # p6=mp.Process(target=segy,args=(10001,12000))
    # p7=mp.Process(target=segy,args=(12001,14000))
    # p8=mp.Process(target=segy,args=(14001,16000))
    # p9=mp.Process(target=segy,args=(16001,18000))
  

    # # p10=mp.Process(target=segy,args=(21,40))
    
    # pr.append(p1)
    # pr.append(p2)
    # pr.append(p3)
    # pr.append(p4)
    # pr.append(p5)
    # pr.append(p6)
    # pr.append(p7)
    # pr.append(p8)
    # pr.append(p9)

    
    
    # time_start = time.time()
    
    # for pro in pr:
    #     pro.start()
        
        
    # for cro in pr:
    #     cro.join()
    
    # time_end = time.time()
    
    # print(f"Time elapsed: {round(time_end - time_start, 2)}s")
    # print("---------------------------------------------------------")
    # print(kr)
    

