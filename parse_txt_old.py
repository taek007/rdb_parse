#!/usr/bin/env python
# -*- coding: utf-8 -*-
#coding=utf-8

from operator import itemgetter, attrgetter
import re
import sys
import os
reload(sys)
import operator
import commands
import time
import urllib
import urllib2
import requests
from multiprocessing import Pool 

sys.setdefaultencoding('utf-8')

dir = "/tmp/rdb"
dateTime = time.strftime('%Y%m%d',time.localtime(time.time()))

#分割文件后的前缀
prefix = "small_rdb_"

sourceDir = dir + "/" + dateTime + "/source/"
cmd = "mkdir -p " + sourceDir
commands.getstatusoutput(cmd)

tmpDir = dir + "/" + dateTime + "/tmp/"
cmd = "mkdir -p " + tmpDir
commands.getstatusoutput(cmd)

#rdb解析之后的文件
rdbFile = dir + "/" + dateTime + "/rdb_parse.txt" 


def splitFile(fileName):
	cmd = "rm -rf " + sourceDir + "*" 
	commands.getstatusoutput(cmd)
	
	#cmd = "cd " + sourceDir + " && split -l 1000 " +  fileName + " " + prefix
	cmd = "cd " + sourceDir + " && split -b 200m " +  fileName + " " + prefix
	#print(cmd)
	(status, output)  = commands.getstatusoutput(cmd)
	if status !=0:
		print("mkdir " + dir + "/" + dateTime + " error")
		return
	
	fileList = []
	for i in range(97,123):
		suffix = chr(i)
		file = sourceDir + prefix + "a" + suffix
		#print(file)
		if os.path.exists(file):
			fileList.append(file)
	return fileList


def parse(fileName):
	suffixFileName = fileName[fileName.rfind("/")+1:]

	cmd = "rm -rf " + tmpDir + "/*"
	commands.getstatusoutput(cmd)

	tmpFile = tmpDir + "/" + suffixFileName + "_tmp"
	tmpFp = open(tmpFile, "w")
	data = {}
	with open(fileName) as f:
		i=0;

		for line in f:
			arr = line.split(",")
			#res = re.findall(r"(.*[^\d])_\d+", arr[2])
			#res = re.findall(r"(.*[^\d]).*", arr[2])
			#res = re.findall(r"([a-zA-Z_]+[^\d])(\d.*)", arr[2])
			try:
				res = re.findall(r"([a-zA-Z_]+)\d.*", arr[0])
				prefix = res[0]
				
				try:
					key = prefix.rstrip('_')
					val = int(arr[1])
					if data.has_key(key):
						tmp = int(data[key])
						data[key] = tmp + val
					else:
						data[key] = val
					
				except 	ValueError:
					continue
			except IndexError:
				continue

	
	#倒序排列
	data=sorted(data.items(),key=operator.itemgetter(1), reverse= True)
	#print(data)
	i=0
	for key,val in enumerate(data):
		i=i+1
		if i > 100:
			break

		str = "%s,%s\n" % (val[0], val[1])
		tmpFp.write(str)
		
	tmpFp.close()


def composeFile():
	fileList = []
	for i in range(97,123):
		suffix = chr(i)
		file = dir + "/" + dateTime + "/tmp/" + prefix + "a" + suffix + "_tmp"
		#print("被分解多个小文件为:" + file)
		if os.path.exists(file):
			fileList.append(file)
	
	finalFile = dir + "/" + dateTime + "/final.txt"
	composeFile = dir + "/" + dateTime + "/tmp/compose.txt"
	
	tmpFp = open(composeFile, "w")

	data = {}
	tmpFp = open(composeFile, "w")
	for file in fileList:
		with open(file) as f:
			for line in f:
				line = line.rstrip("\n")
				arr = line.split(",")
				key = arr[0]
				val = arr[1]
				str = "%s,%s\n" % (key, val)
				tmpFp.write(str)

	tmpFp.close()
	cmd = "awk -F ',' 'BEGIN {} {arr[$1] = arr[$1]+$2};END { for (i in arr) print i, arr[i]}' " + composeFile + " | sort -nrk 2 |head -n 100 > " + finalFile
	commands.getstatusoutput(cmd)
	#print(cmd)
		
	tmpFp.close()
	
	print("结果保存在" +  finalFile)
	notice("多个小文件聚合结束, 结果保存在" + finalFile)

#利用多进程同时处理多个文件	
def multyParse(fileList):
	if len(fileList) == 0:
		print("请将大文件分割成小文件")
		return
		
	pool = Pool()
	pool.map(parse, fileList)
	pool.close()
	pool.join()



def notice(info):
	url = ""
	HEADERS = {'Content-Type': 'application/json;charset=utf-8'}
	
	data={"msgtype":"text","text":{"content":info, "title":"wm-api自动化结果通知"}}
	try:
		resp = requests.post(url,headers=HEADERS,json=data,timeout=(3,60))
	except:
		print ("Send Message is fail!");
	
if __name__ == '__main__':

	fileName = rdbFile
	notice("python 开始分析文件"+fileName +", 取出top100条 使用内存最大的记录")
	fileList = splitFile(fileName)
	#notice(fileName + " 被分解多个小文件, 开始处理小文件")
	multyParse(fileList)
	composeFile()
			
	

