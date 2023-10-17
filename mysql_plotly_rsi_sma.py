
import mysql.connector
import os
import threading
import sys
import matplotlib.pyplot as plt
import numpy as np
import datetime
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

databasename="crypthonws"
mydb=0

coindict={
"337":"XBT/USD",
"881":"XRP/USD",
"561":"ETH/USD",
"353":"XBT/EUR",
"897":"XRP/EUR"
}

compteurdebug=0

def connectdb(dbname) :
	mydb = mysql.connector.connect(host="localhost",database=dbname,user="...",password="...")
	return mydb

def updateWSLike(coin,t0,v0,v1,v2):
	mycursor = mydb.cursor()
	sql = "UPDATE wslike" + " SET rsimma=%s WHERE coin=%s AND timestamp=%s ;"
	val = (v2,coin,t0)
	mycursor.execute(sql, val)
	mydb.commit()
 
def insertTempData(listToDB):
	mycursor = mydb.cursor()
	sql = "INSERT INTO " + "coin" + listToDB[0] + "( coin , timestamp , weight , price , timea , timeb , timec ,  rsimma , volumea , volumeb , volumec ,  sma7 , varsma7 , signvarsma7 , ranksma7 , varranksma7 , sma8 , varsma8 , signvarsma8 , ranksma8 , varranksma8 , sma9 , varsma9 , signvarsma9 , ranksma9 , varranksma9 , sma20 , varsma20 , signvarsma20 , ranksma20 , varranksma20 , sma50 , varsma50 , signvarsma50 , ranksma50 , varranksma50 , sma200 , varsma200 , signvarsma200 , ranksma200 , varranksma200 , pricetwin , varprice , signvarsmaprice , rankprice , varrankprice ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	val = (listToDB[0], listToDB[1], listToDB[2], listToDB[3], listToDB[4], listToDB[5], listToDB[6], listToDB[7], listToDB[8], listToDB[9], listToDB[10], listToDB[11], listToDB[12], listToDB[13], listToDB[14], listToDB[15], listToDB[16], listToDB[17], listToDB[18], listToDB[19], listToDB[20], listToDB[21], listToDB[22], listToDB[23], listToDB[24], listToDB[25], listToDB[26], listToDB[27], listToDB[28], listToDB[29], listToDB[30], listToDB[31], listToDB[32], listToDB[33], listToDB[34], listToDB[35], listToDB[36], listToDB[37], listToDB[38], listToDB[39], listToDB[40], listToDB[41], listToDB[42], listToDB[43], listToDB[44], listToDB[45])
	mycursor.execute(sql, val)
	mydb.commit()
   
def deleteAllFromOneTable(mydb,table_name) :
	mycursor = mydb.cursor()
	table_name="DELETE FROM "+table_name+" ;"
	mycursor.execute(table_name)
	mydb.commit()

def selectLastTuple(table_name):
	mycursor = mydb.cursor()
	table_name="SELECT * FROM "+table_name+" ORDER BY timestamp DESC;"	#+" WHERE timestamp = MAX(timestamp);"
	mycursor.execute(table_name)
	myresult = mycursor.fetchone()
	print(myresult)
	return myresult

def listdb() :
	print("listdb")
	mydb = mysql.connector.connect(host="localhost",user="...",password="...")
	mycursor = mydb.cursor()
	mycursor.execute("SHOW DATABASES")
	for c in mycursor:
		print(c)
	
def listtable(mydb) :
	print("listtable")
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	s=""
	for c in mycursor:
		print(s.join(c))
		#print(type(c))

def dropAllTables(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	ltable=[]
	s=""
	for c in mycursor:
		print(c)
		#print(type(c))	<class 'tuple'>
		ltable.append(s.join(c))
	for t in ltable:
		sql = "DROP TABLE "+t
		mycursor.execute(sql) 

def dropAllTablesIf(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	ltable=[]
	for c in mycursor:
		s=""
		s=s.join(c)
		if "rsi" in s or "RSI" in s:
			print(s)
			ltable.append(s)
	for t in ltable:
		sql = "DROP TABLE "+t
		mycursor.execute(sql) 
		
def dropOneTable(mydb,t) :
	mycursor = mydb.cursor()
	sql = "DROP TABLE "+t
	mycursor.execute(sql) 	
		
def selectAllFrom1Table(mydb,table_name) :
	mycursor = mydb.cursor()
	table_name="SELECT * FROM "+table_name
	mycursor.execute(table_name)
	myresult = mycursor.fetchall()
	for r in myresult:
  		print(r)

def selectallfromalltables(mydb) :
	mycursor = mydb.cursor()
	mycursor.execute("SHOW TABLES")
	myresult = mycursor.fetchall()
	for r in myresult:
		s=''
		selectAllFrom1Table(mydb,s.join(r))
		
def connect_db() :
	mydb = mysql.connector.connect(host="localhost",user="...",password="...")
	return mydb
	
def createDB(mydatabase) :
	print("createdb")
	mydb = connect_db()
	mycursor = mydb.cursor()
	mydatabase = "CREATE DATABASE " + mydatabase
	print(mydatabase)
	mycursor.execute(mydatabase)


# Définir les fonctions de rappel WebSocket
def ws_message ():
		mycursor = mydb.cursor()
		table_name="SELECT * FROM wslike where coin='337' ;" #order desc ?
		mycursor.execute(table_name)
		myresult = mycursor.fetchall()
		for r in myresult:
			myc= mydb.cursor()	#pas pour la production
			#pas pour la production
			tabl="SELECT * FROM coin"+r[0] + " ;"#where coin='337' ;" #order desc ?
			myc.execute(tabl)	#pas pour la production
			myr = myc.fetchall()	#pas pour la production
			#if len(myr)>14:
			compute(r[0],r[1],r[2],r[3])
			

def insertCoin(listToDB):
	mycursor = mydb.cursor()
	sql = "INSERT INTO " + "coin" + listToDB[0] + "( coin , timestamp , weight , price , timea , timeb , timec ,  rsimma , volumea , volumeb , volumec ,  sma7 , varsma7 , signvarsma7 , ranksma7 , varranksma7 , sma8 , varsma8 , signvarsma8 , ranksma8 , varranksma8 , sma9 , varsma9 , signvarsma9 , ranksma9 , varranksma9 , sma20 , varsma20 , signvarsma20 , ranksma20 , varranksma20 , sma50 , varsma50 , signvarsma50 , ranksma50 , varranksma50 , sma200 , varsma200 , signvarsma200 , ranksma200 , varranksma200 , pricetwin , varprice , signvarsmaprice , rankprice , varrankprice ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
	val = (listToDB[0], listToDB[1], listToDB[2], listToDB[3], listToDB[4], listToDB[5], listToDB[6], listToDB[7], listToDB[8], listToDB[9], listToDB[10], listToDB[11], listToDB[12], listToDB[13], listToDB[14], listToDB[15], listToDB[16], listToDB[17], listToDB[18], listToDB[19], listToDB[20], listToDB[21], listToDB[22], listToDB[23], listToDB[24], listToDB[25], listToDB[26], listToDB[27], listToDB[28], listToDB[29], listToDB[30], listToDB[31], listToDB[32], listToDB[33], listToDB[34], listToDB[35], listToDB[36], listToDB[37], listToDB[38], listToDB[39], listToDB[40], listToDB[41], listToDB[42], listToDB[43], listToDB[44], listToDB[45])
	mycursor.execute(sql, val)
	mydb.commit()

 
def myfunc(result):
	return result[1]
  
def compute(coin,timestamp,weight,price):

	global compteurdebug
	
	
	listToDB=[coin,timestamp,weight,price]
	for i in range(42):
		if i==37:
			listToDB.append(price)
		else:
			listToDB.append(0) # attention normalement 0
	#print (listToDB)
	s=str(200)
	#print(s)
	mycursor = mydb.cursor()
	
	table_name="SELECT * FROM coin"+coin+" ORDER BY timestamp DESC LIMIT "+s+";"
	mycursor.execute(table_name)
	#myres=[(coin,timestamp,weight,price,0,0,0,0,0,0,0)]
	ultat=mycursor.fetchall()
	
	myresult=[]
	myresult.append(tuple(listToDB))
	myresult+=ultat
	
	myresult.sort(key=myfunc)
	
	delta=14
	epsilon=1
	theta=40
	epsilonema21=2

	derncoursh=derncoursb=derncourshtheta=derncoursbtheta=0
	b=bo=False
	mmeh=mmeb=0
	lrsihausses=[]
	lrsibaisses=[]
	lrsihaussestheta=[]
	lrsibaissestheta=[]
	rsi=rsitheta=0
	m=0

	ema21=mma9=mma8=mma7=mma20=mma50=mma200=0
	
	if len(myresult)>=200:
		for x in myresult:
				#print(x)
				if m>0:
					mma200+=float(x[3])
				
				if m>151:
					mma50+=float(x[3])
					
				nrsihh=0
				nrsibb=0
				nrsih=nrsib=mhb=rsi=0
				nrsihtheta=nrsibtheta=mhbtheta=rsitheta=0
								
				if m>(200-theta):
					nrsitheta=float(x[3])-float(y[3])	#x[3] price
					#print("hausse ou baisse : "+str(nrsitheta))
					if nrsitheta>=0:
						lrsihaussestheta.append(nrsitheta)
						lrsibaissestheta.append(0)
					else:
						lrsibaissestheta.append(-nrsitheta)
						lrsihaussestheta.append(0)
						
				if m>179:
					ema21+=float(x[3])
					
				if m>180:
					mma20+=float(x[3])
					
				if m>(200-delta):
					nrsi=float(x[3])-float(y[3])	#x[3] price
					#print("hausse ou baisse : "+str(nrsi))
					if nrsi>=0:
						lrsihausses.append(nrsi)
						lrsibaisses.append(0)
					else:
						lrsibaisses.append(-nrsi)
						lrsihausses.append(0)
				
				if m>191:
					mma9+=float(x[3])
				if m>192:
					mma8+=float(x[3])
				if m>193:
					mma7+=float(x[3])
						
				if m>=(200):
					nrsih=nrsib=0
					for mm in range(len(lrsihausses)):	#range(m-delta,m):
						nrsih=float(lrsihausses[mm])+nrsih					
						nrsib=float(lrsibaisses[mm])+nrsib					
						derncoursh=lrsihausses[mm]
						derncoursb=lrsibaisses[mm]
					#nrsih=nrsih/14
					#mme=(mme*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*dercours)
					if b==False:
						b=True
						mmeh=nrsih=nrsih/delta
						mmeb=nrsib=nrsib/delta
					else:
						mmeh=nrsih=(mmeh*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*derncoursh)
						mmeb=nrsib=(mmeb*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*derncoursb)
					mhb=0
					if nrsib!=0:
						mhb=nrsih/nrsib
					rsi=100-(100/(1+mhb))
					#print("coin:",x[0]," date/heure:",x[1]," cours:",x[3]," rsi:",rsi)
					#pause=input("pause")

					
					nrsihtheta=nrsibtheta=0
					for mmtheta in range(len(lrsihaussestheta)):	#range(m-delta,m):
						nrsihtheta=float(lrsihaussestheta[mmtheta])+nrsihtheta					
						nrsibtheta=float(lrsibaissestheta[mmtheta])+nrsibtheta					
						derncourshtheta=lrsihaussestheta[mmtheta]
						derncoursbtheta=lrsibaissestheta[mmtheta]
					#nrsih=nrsih/14
					#mme=(mme*(1-(epsilon/(delta+epsilon-1))))+((epsilon/(delta+epsilon-1))*dercours)
					if bo==False:
						bo=True
						mmehtheta=nrsihtheta=nrsihtheta/theta
						mmebtheta=nrsibtheta=nrsibtheta/theta
					else:
						mmehtheta=nrsihtheta=(mmehtheta*(1-(epsilon/(theta+epsilon-1))))+((epsilon/(theta+epsilon-1))*derncourshtheta)
						mmebtheta=nrsibtheta=(mmebtheta*(1-(epsilon/(theta+epsilon-1))))+((epsilon/(theta+epsilon-1))*derncoursbtheta)
					mhbtheta=0
					if nrsibtheta!=0:
						mhbtheta=nrsihtheta/nrsibtheta
					rsitheta=100-(100/(1+mhbtheta))
					#print("coin:",x[0]," date/heure:",x[1]," cours:",x[3]," rsi:",rsi)
					#pause=input("pause")


				y=x
				m+=1


	mma9/=9
	mma8/=8
	mma7/=7
	mma20/=20
	mma50/=50
	mma200/=200
	ema21=(ema21+float(listToDB[41])*(epsilonema21-1))/(21+epsilonema21-1)
	listToDB[9]=round(ema21,6)

	
	listToDB[21]=str(round(mma9,6))
	listToDB[16]=str(round(mma8,6))
	listToDB[11]=str(round(mma7,6))
	listToDB[26]=str(round(mma20,6))
	listToDB[31]=str(round(mma50,6))
	listToDB[36]=str(round(mma200,6))
	listToDB[7]=str(round(rsi,6))
	listToDB[8]=round(rsitheta,6)
	
	try:
		listToDB[10]=round((float(listToDB[8])-float(ultat[0][8])),6)
	except IndexError:
		listToDB[10]=round(float(listToDB[8]),6)
		
	rankdict={}
	# calcul des variations n - n-1
	for i in range(26,42,5):
		try:
			listToDB[i+1]=round(float(listToDB[i])-float(ultat[0][i]),6)
		except IndexError:
			listToDB[i+1]=round(float(listToDB[i]),6)
		#print(i,"listToDB[i+1]",round(float(listToDB[i]),6))
		# calcul du rank
		rankdict.update({ i+3 : listToDB[i] })
		# calcul signe now / before
		try:
			if np.sign(float(listToDB[i+1])) != np.sign(float(ultat[0][i+1])):
				listToDB[i+2]=int(np.sign(float(listToDB[i+1])))
			else:
				listToDB[i+2]=0
		except IndexError:
			listToDB[i+2]=0
		

	rankdict_=dict(sorted(rankdict.items(), key=lambda item: float(item[1])))
	#print(rankdict_)
	j=0
	for x in rankdict_:
		listToDB[x]=int(j)
		#print(x,listToDB[x])
		j+=1
		
	seuil=66
	compteurdebug+=1

	if compteurdebug>200:	# pour les tests...sinon faire tourner à vide puis après 200 enregistr. lancer tests
	
		#fp=open("journal.csv", 'a')
		
		print("évènement déclencheur ACHAT , retournement du cours : ",listToDB[43]," au-dessus de toutes les mm : ",listToDB[44]," descente sma7 : ",listToDB[13])
		
		print("évènement déclencheur VENTE , rsi40>66 : ",listToDB[8]," descendant : ",listToDB[10])

		listlevel2=[]
		listlevel2.append(tuple(listToDB))
		listlevel2+=ultat
		
		if listToDB[43] > 0 and listToDB[44] == 3 and listToDB[13] == 0:
			#print(listToDB[3])
			print("\nACHAT coin : ",listToDB[0])	# on passe de 0 ou - à + = le cours monte

			#fp.write("200;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
			title="Entrée - coin : "+coindict[listToDB[0]]+" timestamp : "+str(listToDB[1])
			listlevel21 = listlevel2.copy()
			t1=threading.Thread(target=graph,args=(listlevel21,title,))
			t1.start()
			#analyse(listlevel2)
			#pause=input("pause")
		if listToDB[8] > seuil and listToDB[10] <= 0:
			#print(listToDB[3])
			print("\n___________________________VENTE coin : ",listToDB[0])	# on passe de 0 ou - à + = le cours monte

			#fp.write("600;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
			title="Sortie - coin : "+coindict[listToDB[0]]+" timestamp : "+str(listToDB[1])
			listlevel22 = listlevel2.copy()
			t2=threading.Thread(target=graph,args=(listlevel22,title,))
			t2.start()
			#analyse(listlevel2)
			#pause=input("pause")
		#else:
			#fp.write("400;"+listToDB[3]+";"+str(listToDB[8])+";"+str(listToDB[9])+"\n")
		#pause=input("pause")
	#print(listToDB)
	insertCoin(listToDB)


def graph(l,title):
	lcours=[]
	lsma7=[]
	lsma20=[]
	lsma50=[]
	lsma200=[]
	l.sort(key=myfunc)
	ltime=[]
	i=0
	s=""
	for x in l:
		if i >= 150:
			lcours.append(round(float(x[3])))
			#s+=str(x[3])+" ; "
			lsma7.append(round(float(x[11])))
			#s+=str(x[11])+" ; "
			lsma20.append(round(float(x[26])))
			#s+=str(x[26])+" ; "
			lsma50.append(round(float(x[31])))
			#s+=str(x[31])+" ; "
			lsma200.append(round(float(x[36])))
			#s+=str(x[36])+" \n "
			ltime.append(i-200)
		i+=1
		#print(s)	
		#pause=input("pause")
	#s=""
	f1 = go.Figure(data = [go.Scatter(x=ltime, y=lcours, name="cours"), go.Scatter(x=ltime, y=lsma7, name="sma7"), go.Scatter(x=ltime, y=lsma20, name="sma20"),  go.Scatter(x=ltime, y=lsma50, name="sma50"),  go.Scatter(x=ltime, y=lsma200, name="sma200")], layout = {"xaxis":{"title": "temps"},"yaxis":{"title": "valeurs"},"title":title})
	f1
	current_time = datetime.datetime.now().timestamp()
	s='lgraph-'+str(current_time)+".png"
	f1.write_image(s)
    

mydb=connectdb(databasename) 					
w=0
while w<1:
	about_db = input("\n[0 connect MySQL] \n[1 deleteAllCollections} \n[2 prod] \n[3 selectLastTuple] \n[4 deleteAllDocuments] \n[5 compute] \n[6 ] \n[7 listdb] \n[8 listtable] \n[9 selectallfromalltables] \n[10 dropAllTables] \n[11 dropOneTable] \n[12 dropAllTablesIf] \n[13 selectAllFromOneTable] \n[14 exportRSI] \n[15 createDB]")

	match about_db:
		case "0":
			dbname="RelativeStrengthIndex"
			print(connectdb(dbname))
		case "1":
			print(deleteAllCollections())
		case "2":
			ws_message()
		case "3":
			tablename=input("tablename?")
			selectLastTuple(tablename)
			
		case "4":
			colToDelete=input("Collection To Delete ? ")
			deleteAllDocuments(colToDelete)
		case "5":
			compute(tablename)
		case "6":
			mydb=connectdb(databasename)
			print(mydb)

		case "7":
			listdb()
		case "8":
			dbname=databasename	#input("dbname : ")
			mydb=connectdb(dbname)
			listtable(mydb)
		case "9":
			selectallfromalltables(mydb)
		case "10":
			dropAllTables(mydb)
		case "11":
			tableName=input("tableName ? ")
			dropOneTable(mydb,tableName)
		case "12":
			mydb=connectdb(databasename)
			dropAllTablesIf(mydb)
		case "13":
			tableName=input("tableName ? ")
			selectAllFrom1Table(mydb,tableName)
		case "14":
			exportRSI(mydb)
		case "15":
			mydatabase=input("DB name ? ")
			databasename=mydatabase
			createDB(mydatabase) 
		case _:
	   		w += 1



