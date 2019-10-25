#coding:utf-8
#---------------------------------------------------------------------
# Name:       Multiprocess_Forge_ShapefileToFeatureClass_uft8.py
# Purpose:    農地の筆界をマルチプロセスでフィーチャクラスに変換
#             自治体コードや自治体名をフィールドに入れる
#             フォルダ内のフィーチャクラスをマージしたフィーチャクラスを作成
# Author:     Kataya @ ESRI Japan
# Created:    xx/10/2019
# Copyright:   (c) ESRI Japan Corporation
# ArcGIS Version:   10.x
# Python Version:   2.7
#---------------------------------------------------------------------
import arcpy,os
import sys
#from multiprocessing import Process
import multiprocessing
import datetime
import traceback

reload(sys)
sys.setdefaultencoding('cp932')

def split_citycode_cityname(wsname):
    '''
    農地筆の自治体コードと自治体名からそれぞれ抽出して返す
    フォルダ名の例）02201青森市2019
                    02202弘前市2019
    '''
    l = len(wsname)
    citycode = "'{0}'".format(wsname[:5])    #自治体コードだけを抽出
    cityname = '"' + "{0}".format(wsname[5:l-4]) + '"' #自治体名だけを抽出
    return citycode, cityname

def multi_run_batch_convert(args):
    '''
    batch_convertのwrapper:
      複数の引数を実行処理に渡すために必用なラッパー
    '''
    return batch_convert(*args)

def batch_convert(inws, outws):
    '''
    1プロセスで実行する処理:
      1) FGDBへの書込みは仕様で複数プロセスで書込みできないため
           1市区町村フォルダ下のシェープ ファイルを
           1市区町村のFGDB下のフィーチャ クラスに変換
	  2) すべてのフィーチャ クラスを統合したとき識別しやすいように
	       フォルダ名から市区町村コードと、自治体名を作成しフィールドに値を格納
    '''
    print(u"Convert: {0} ⇒ {1}\n".format(inws,outws))
    if not arcpy.Exists(outws):
        outfolder = u"{0}".format(os.path.dirname(outws))
        foldername= u"{0}".format(os.path.basename(outws))
        arcpy.CreateFileGDB_management(outfolder, foldername, "CURRENT")
    arcpy.env.workspace = inws
    fieldname1 = "CITYCODE"
    fieldname2 = "CITYNAME"
    fcs = arcpy.ListFeatureClasses()
    for fc in fcs:
        infc=os.path.splitext(fc)[0]
        newfc = u"c_{0}".format(infc) #シェープファイル名が数値ではじまり、FGDBへそのまま変換できないので接頭にc_を入れる
        wsname = os.path.basename(inws)
        citycode, cityname = split_citycode_cityname(wsname) #フォルダ名から自治体コードと自治体名を抽出
        ## 座標系はそのまま変換
        if arcpy.Exists(os.path.join(outws,newfc)):
            outfc=os.path.join(outws,newfc)
            arcpy.Append_management(fc,outfc)
        else:
            arcpy.FeatureClassToFeatureClass_conversion(fc,outws,newfc)
            outfc=os.path.join(outws,newfc) 
            arcpy.AddField_management(outfc,fieldname1,"TEXT",field_length=5)
            arcpy.AddField_management(outfc,fieldname2,"TEXT",field_length=30)
        # citycode, cityname をフィールド値にいれる
        calfc = os.path.join(outws,newfc) 
        arcpy.CalculateField_management(calfc, fieldname1, citycode, "PYTHON_9.3", "#")
        arcpy.CalculateField_management(calfc, fieldname2, cityname, "PYTHON_9.3", "#")
    
    del fcs
    return u"  変換済：{0}".format(outws)

def exec_batch_convert(infolder,outfolder):
    '''
    マルチプロセスでの処理：
    '''
    try:
        start=datetime.datetime.now()
        print "-- Strat: Multiprocess_Forge_ShapefileToFeatureClass --:",start
        cpu_cnt=multiprocessing.cpu_count()
        arcpy.env.workspace = infolder
        inwss = arcpy.ListWorkspaces("*","Folder")
        # 各プロセスに渡すパラメータをリスト化
        params=[]
        for inws in inwss:
            param1=inws # 市区町村フォルダ（シェープファイルが入っている）
            gdbname=u"{0}.gdb".format(os.path.basename(inws))
            param2=os.path.join(outfolder,gdbname) # 市区町村ファイルジオデータベース
            params.append((param1,param2))
        if len(inwss) < cpu_cnt: # 処理フォルダ数CPUコアより少ない場合無駄なプロセスを起動不要
            cpu_cnt=len(inwss)
        pool = multiprocessing.Pool(cpu_cnt) # cpu数分プロセス作成
        results=pool.map(multi_run_batch_convert,params) # 割り当てプロセスで順次実行される
        pool.close()
        pool.join()
        # 各プロセスでの処理結果を出力
        for r in results:
            print(u"{0}".format(r))
        
        # 各プロセスからのマージ版を作成
        arcpy.env.workspace = outfolder
        outwss = arcpy.ListWorkspaces("*","FileGDB")
        foldername="{0}.gdb".format(os.path.basename(outfolder))
        forgefc = "forge"
        print(u"  Mearge to FeatureClass:{1} in FGDB:{0} ".format(foldername, forgefc))
        arcpy.CreateFileGDB_management(outfolder,foldername,"CURRENT")
        forgews = os.path.join(outfolder, foldername)
        for outws in outwss:
            arcpy.env.workspace = outws
            fc = arcpy.ListFeatureClasses()[0] #農地筆は1ファイルしかないので固定
            print(u"    merge: {0} ⇒ {1}".format(fc,forgefc))
            if arcpy.Exists(os.path.join(forgews, forgefc)):
                outfc=os.path.join(forgews,forgefc)
                arcpy.Append_management(fc,outfc)
            else:
                arcpy.FeatureClassToFeatureClass_conversion(fc,forgews,forgefc)
        
        # マージが終わったので後片付け 各市区町村のFGDBを削除 - 必要に応じてコメントアウトを外す
        #for outws in outwss:
        #    arcpy.Delete_management(outws)
        
        fin=datetime.datetime.now()
        print "-- Finish: Multiprocess_Forge_ShapefileToFeatureClass --:",fin
        print "     Elapsed time:", fin-start
    except:
        print traceback.format_exc("{0}".format(sys.exc_info()[2]))

def setup_batch_convert():
    '''
    コマンドプロンプトからの実行パラメータを設定の場合：
      市区町村別のシェープ ファイルが入った都道府県フォルダ :infolder
      例）
        |-02青森県2019
            |-02201青森市2019
            |   02201青森市2019_5.shp
            |-02202弘前市2019
            |   02202弘前市2019_5.shp
            |-02203八戸市2019
            |-02204黒石市2019		
            ･････
      市区町村別のファイル ジオデータベースの作成先フォルダ :outfolder
      例)
        |-02青森県2019_filegdb
            |-02201青森市2019.gdb
            |   c_02201青森市2019_5
            |-02202弘前市2019.gdb
            |   c_02202弘前市2019_5
            |-02203八戸市2019
            |-02204黒石市2019		
            ･････
            |-02青森県2019_filegdb.gdb # 市区町村別のフィーチャ クラスをマージしたフィーチャ クラスを格納するファイル ジオデータベース
            |   forge	
    '''
    infolder=ur"F:\Temp\農地の筆ポリゴン\02青森県2019" #市区町村別のシェープ ファイルが入った都道府県フォルダ（平面直角座標系ごと）
    outfolder=ur"F:\Temp\農地の筆ポリゴン\02青森県2019_filegdb" #市区町村別のファイル ジオデータベースの作成先フォルダ
    exec_batch_convert(infolder,outfolder)


if __name__ == '__main__':
    setup_batch_convert()
