# coding: utf-8

#Algoritmo para teste do Serviço Myria
#Professores: Vanessa e Victor
#Aluno: Frank Willian
#Programa de Pós-Graduãção IC/UFF/NI-RJ-BR

import json
import subprocess as subp
import os
import time
import datetime
import sys

#funcao para escrever em json
def writeJson(path,js):
	file = open(path, 'w')
	file.write(json.dumps(js,indent = 3,sort_keys=True))
	file.close()

#funcao para alterar o json da consulta
def alter_join(wn,path,fileQuery):
	for file in fileQuery:
		path_ = path+'jsonQueries/triangles_twitter/'+file+'.json'
		with open(path_) as query:
			data = json.load(query)
		for frag in data['fragments']:
			for op in frag['operators']:
				if (op['opType'] == 'CollectConsumer'):
					break
				elif (op['opType'] == 'TableScan'):
				        try:                                                                                                                                    
                				if data['workers']:                                                                                                             
			                	        del data['workers']                                                                                                     
			       		except KeyError: print ""
					break
				else:
					frag['workers'] = wn
				break
		writeJson(path_,data)

#funcao para alterar o json do ingest
def alter_ingest(schema,path,fileDataset,shards,filesIngest):
	#for file in filesIngest:
	file = filesIngest
	path_ = path+'jsonQueries/triangles_twitter/'+file+'.json'
	with open(path_) as ingest:
		data = json.load(ingest)
	try:
		if data['numShards']:
			del data['numShards']
	except KeyError: print ""
	try:
		if data['repFactor']:
			del data['repFactor']
	except KeyError: print ""
	try:
		if data['shards']:
			del data['shards']
	except KeyError: print ""
	try:
		if data['workers']:
			del data['workers']
	except KeyError: print ""
	data['shards'] = shards
	data['source']['filename'] = path+'jsonQueries/triangles_twitter/'+fileDataset
	writeJson(path_,data)

#funcao que alterar os json de ingest e query de acordo
#com o cenario
def ingest_and_query(schema,path,fileQuery,fileDataset,filesIngest):
	#schema = ns, rf, wn
	#ingest
	shards = []
	i = 1
	for x in range(1,schema['ns']+1):
		shards.append(list(range(i,i+schema['rf'])))
		i = i + schema['rf']
	alter_ingest(schema,path,fileDataset,shards,filesIngest)
	#join
	dn = schema['ns']*schema['rf']
	wn = list(range(1,dn+schema['wn']+1))
	alter_join(wn,path,fileQuery)

#funcao que executa chama o ingest e retorna um json
def callIngest(cmd):
	pipe = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True,universal_newlines=True)
	while pipe.poll() is None:
		time.sleep(3)
	outPipe = pipe.stdout.read()
	if 'Created' in outPipe:
		while 'relationKey' not in outPipe:
			time.sleep(3)
			outPipe = pipe.stdout.read()
			print(outPipe)
		out = json.loads(outPipe[outPipe.find('{',0):])
		return out
	else:
		return outPipe

#funcao que executa executa a query e retorna um json
def callQuery(cmd):
	pipe = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True,universal_newlines=True)
	time.sleep(5)
	if not pipe.poll() is None:
		outPipe = pipe.stdout.read()
		if isinstance(outPipe,str):
			if '{' in outPipe:
				outPipe = json.loads(outPipe[outPipe.find('{',0):])
				return 'ok', outPipe
			else:
				return 'error', outPipe
		else:
			return 'error', outPipe
	else:
		return 'error','NONE'

#função para capturar a informação inicial
def getInfo(pwd):
	lMaq = [line.rstrip('\n') for line in open(pwd+'listMaq.txt')]
	listDN = list(set(lMaq))

	#Define a primeira maquina da lista como master
	master = lMaq[0]

	#Remove o hostname (master) da lista de nós
	while master in lMaq:
		lMaq.remove(master)
	listDN.remove(master)

	#define o path myria_home
	path = pwd+'myria/'

	return listDN, lMaq, master, path

#funcao que le o arquivo de entrada e retorna
#o path e a lista de workers
def prepareDeploy(path, master,listDeploy,port):
	#Captura arquivo de deploy
	file = open(path+'myriadeploy/deployment.cfg', 'r')
	lines = file.readlines()
	file.close()

	#altera master
	del lines [lines.index('[master]\n')+1:lines.index('[workers]\n')-1]
	lines.insert(lines.index('[master]\n')+1,'0 = '+master+':'+str(port)+'\n')

	#altera workers
	del lines [lines.index('[workers]\n')+1:len(lines)]
	x = 1
	for w in listDeploy:
		lines.insert(x+lines.index('[workers]\n'),w+'\n')
		x += 1

	#Grava arquivo de deploy
	file = open(path+'myriadeploy/deployment.cfg', 'w')
	file.write(''.join(lines))
	file.close()

def getSchemas(qtdM,cores):
        # schema = Xm_Ywn_Zns_Krf
        schemas = []
        m = 8
        while m <= qtdM:
            ns = 2
            rf = 1
            while ((ns*rf) <= m):
                wn = 0
                t = 4
                while t < ns * rf:
                        t *= 2
                while(t <= (m*cores)):
                        #if ((ns*rf) == 6) and (t == 8):
                                #data = {'m':m,'ns':ns,'rf':rf,'wn':0}
                        wn = t - (ns*rf)
                        data = {'m':m,'ns':ns,'rf':rf,'wn':wn}
                        schemas.append(data)
                        t *= 2
                rf += 1
            m *= 2
        return schemas

def deployMyria(stopAll, cleanAll, setupCluster, startDeploy, path, master, listDN, s):

	setup_cluster = ['./setup_cluster.py deployment.cfg']
	deploy = ['./launch_cluster.sh deployment.cfg']
	walive = ['curl '+master+':8753/workers/alive']

	if stopAll:
		#mata processos Myria
		os.chdir(path+"myriadeploy/")
		cmd = ['./stop_all_by_force.py deployment.cfg']
		killProcess = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
		while killProcess.poll() is None:
			print("Matando processos Myria...")
			time.sleep(3)

	if cleanAll:
		#Limpa diretório tmp do master e de todos os nós
		cmd = ['ssh '+master+' \'rm -rf /var/usuarios/frankwrs/*\'']
		cleanMaster = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
		while cleanMaster.poll() is None:
			print("Apagando arquivos temporários master...")
			time.sleep(3)

	        	for node in listDN[0:s['schema']['m']]:
			#Limpa diretório tmp do master e de todos os nós
				cmd = ['ssh '+node+' \'rm -rf /var/usuarios/frankwrs/*\'']
				cleanNodes = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
				while cleanNodes.poll() is None:
	        			print("Apagando arquivos temporários worker "+node+"...")
	        			time.sleep(3)

	if setupCluster:
		#seta o diretorio de deploy
		os.chdir(path+"myriadeploy/")
		#configura as máquinas para o deploy
		setupMyria = subp.Popen(setup_cluster, stdout=subp.PIPE,stderr=subp.PIPE, shell=True)
		while setupMyria.poll() is None:
			print("Setup_cluster working "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss"))+"...")
			time.sleep(5)
			saida = setupMyria.stdout.read()

	if startDeploy:
		#Inicio do Deploy
		myriaDeploy = subp.Popen(deploy, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
		while myriaDeploy.poll() is None:
			print("Deploy working "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss"))+"...")
			time.sleep(3)

	#captura os workers ativos
	time.sleep(5)
	ws = subp.Popen(walive, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True, shell=True)
	workers = ws.stdout.read()
	#print(workers)
	#testa se os workers estao todos ativos
	w = 0
	nosTotais = (s['schema']['ns']*s['schema']['rf'])+s['schema']['wn']
	for i in range(1,nosTotais+1):
		if str(i) not in workers:
			print("Worker ",i," not alive only ",workers)
		else:
			w += 1

	if w == nosTotais:
		errorDeploy = False
		print("Workers alive: "+workers)
	else:
		errorDeploy = True

	return errorDeploy

def main(fileDataset):

	#Obtem lista de maquinas, hostname e path
	pwd = '/home_nfs/frankwrs/'
	listDN, listMaq, master, path = getInfo(pwd)
	print("Path: ",path)
	print("Master: ",master)
	print("ListDN: ",listDN)

	#Define nome dos arquivos de consulta e dataset
	if fileDataset == "full":
		fileDataset = 'twitter.csv'
		ds = 'full'
	else:
		fileDataset = 'twitter_small.csv'
		ds = 'small'
	pathDN = '/var/usuarios/frankwrs/myria-files/DN'

    	#gera cenarios
    	sample = 'REPLICATION-WORLKLOAD'
    	#schemas = getSchemas(len(listDN),len(listMaq)/len(listDN))
	schemas = [{'m':8,'rf':1,'ns':2,'wn':0}]

	#Define nome do arquivo com resultados
	nameFileResult = pwd+'Experiment_replication/Results/'+sample+'_'+str(time.strftime("%d%b%Y-%Hh%Mm%Ss"))+'_DS-'+ds+'.json'

	#Constroi os cenarios
	#Faz deploy do serviço para cada cenario
	#Executa e faz media de tempo das consultas

	avgTime = []
	for s in schemas:
		avgTime.append({'schema': s,'steps': []})

	for x in range(1,6):

		for s in avgTime:

			print(s)

			port = 17001

			#Gera lista de nós/nucleos para o deploy
			listDeploy = []
			dn = 1
			nosTotais = (s['schema']['ns']*s['schema']['rf'])+s['schema']['wn']
			for i in range(0,nosTotais):
				if dn <= (s['schema']['ns']*s['schema']['rf']):
					node = str(i+1)+' = '+str(listDN[i%len(listDN[0:s['schema']['m']])])+':'+str(port)+':'+pathDN
					dn += 1
				else:
					node = str(i+1)+' = '+str(listDN[i%len(listDN[0:s['schema']['m']])])+':'+str(port)
				listDeploy.append(node)
				port+=1

			#prepara o deploy
			print("ListDeploy: ",listDeploy)
			prepareDeploy(path, master,listDeploy,17000)

			#Faz deploy do myria
			errorDeploy = True
			while errorDeploy:

				#Deploy Myria
				errorDeploy = deployMyria(True, True, True, True, path, master, listDN,s)

				if not errorDeploy:


					#seta o diretorio do ingest e query
					os.chdir(path+"jsonQueries/triangles_twitter/")

					#Altera os arquivos json de ingest e join
					#filesIngest = ['ingest_twitter_C1','ingest_twitter_C2']
					fileIngest = 'ingest_twitter'
					filesQuery = ['C1','C2']
					ingest_and_query(s['schema'],path,filesQuery,fileDataset,fileIngest)

					print "Start ingest"
					#for f in filesIngest:
					#pipe Ingest
					ingest = ['curl -i -XPOST '+master+':8753/dataset -H \"Content-type: application/json\" -d @./'+fileIngest+'.json']
					outIngest = callIngest(ingest)
					while isinstance(outIngest,str):
						print("ERRO INGEST\n"+outIngest)
						outIngest = callIngest(ingest)
					#rint('Ingest completo: '+f)
					print "Finish ingest"

					#Inicio da submissão de consulta
					qID = []
					for f in filesQuery:
						print("Submit query "+f+" - "+str(x))
						queryERROR = True
						while queryERROR:
							cmd = ['curl -i -XPOST '+master+':8753/query -H \"Content-type: application/json\" -d @./'+f+'.json']
							#print(cmd)
							sq, outQuery = callQuery(cmd)
							if sq == 'ok':
								if outQuery["status"] == "ACCEPTED":
									qID.append({'query': f, 'id':outQuery['queryId']})
									queryERROR = False
									print(f+": "+outQuery['status']+" - ID: "+str(outQuery['queryId']))
								else:
									print("QUERY NOT SUCESS: "+outQuery['status'])
							else:
								print("QUERY ERROR: \n"+outQuery)

					if not queryERROR:
						outQ = []
						for id in qID:
							#coletando tempo de consulta
							print "Get query time "+id['query']+" step "+str(x)
							#pipe query time
							getQuery = 'curl -i -XGET '+master+':8753/query/query-'+str(id['id'])
							#print(getQuery)
							sq, outGetQuery = callQuery(getQuery)
							getStart = datetime.datetime.now()
							#outQ[i]=outGetQuery	
							#print(outGetQuery)
							#st = datetime.datetime.now()
							if sq == 'ok':
								if (outGetQuery['status'] == 'SUCCESS'):
									print(id['query']+": "+outGetQuery['status'])
                                                                        #print(outGetQuery)
                                                                        outQ.append({'query':id['query'],'out':outGetQuery})
                                                                        queryERROR = False
								else:
									while (outGetQuery['status']):
										print(id['query']+": "+outGetQuery['status']+" - "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss")))
										if outGetQuery['status'] == 'SUCCESS':
											print(id['query']+": "+outGetQuery['status'])
											#print(outGetQuery)
                                                                        		outQ.append({'query':id['query'],'out':outGetQuery})
											queryERROR = False
											break
										elif outGetQuery['status'] == 'RUNNING':
											a = (datetime.datetime.now() - getStart).seconds
											if (a > 600):
												queryERROR = True
												print("Sotping query "+id['query']+" by outtime: "+str(a))
												break
											else:
												time.sleep(10)
												sq, outGetQuery = callQuery(getQuery)
												while sq == 'error':
													#totaltime = (datetime.datetime.now() - st).total_seconds()
													print("Bug Myria, sleeping...  - "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss")))
													time.sleep(720)
													#Deploy Myria
													errorDeploy = True
													while errorDeploy:
														errorDeploy = deployMyria(True, False, False, True, path, master, listDN,s)
													sq, outGetQuery = callQuery(getQuery)
										else:
											print(id['query']+": "+outGetQuery['status'])
											queryERROR = True
											errorDeploy = True
											break
							else:
								queryERROR = True
								errorDeploy = True
								print("QUERY ERROR: \n"+str(outGetQuery))
					#print(outQ)

					if (not queryERROR) and (not errorDeploy):
						timeQuery=[]
						for outT in outQ:
							#Subtração dos tempos de start e finish da query
							#st = outT['out']['startTime'][outT['out']['startTime'].find('T')+1:outT['out']['startTime'].find('-',10)]
							#st = datetime.datetime.strptime(st,"%H:%M:%S.%f")
							st = datetime.datetime.strptime(outT['out']['startTime'][:outT['out']['startTime'].find('-',12)],"%Y-%m-%dT%H:%M:%S.%f")
							#ft = outT['out']['finishTime'][outT['out']['finishTime'].find('T')+1:outT['out']['finishTime'].find('-',10)]
							#ft = datetime.datetime.strptime(ft,"%H:%M:%S.%f")
							ft = datetime.datetime.strptime(outT['out']['finishTime'][:outT['out']['finishTime'].find('-',12)],"%Y-%m-%dT%H:%M:%S.%f")
							timeQuery.append({'query':outT['query'],'start':st.strftime("%Y-%m-%dT%H:%M:%S.%f"), 'end':ft.strftime("%Y-%m-%dT%H:%M:%S.%f"),'time': (ft - st).total_seconds()})

					if (not queryERROR) and (not errorDeploy):
						s['steps'].append({'id':x,'timeQuery':timeQuery,'secondsTotal':(max(datetime.datetime.strptime(t['end'],"%Y-%m-%dT%H:%M:%S.%f") for t in timeQuery) - min(datetime.datetime.strptime(t['start'],"%Y-%m-%dT%H:%M:%S.%f") for t in timeQuery)).total_seconds(),'finishTime':time.strftime("%Hh%Mm%Ss")})
						#Salva resultados no cenário no arquivo
						writeJson(nameFileResult,avgTime)

	#calcula média das consultas para cada esquema
	for s in avgTime:
		timeTotal = [t['secondsTotal'] for t in s['steps']]
    		s.update({'avgTime': float("{0:.3f}".format((sum(timeTotal)-max(timeTotal)-min(timeTotal))/(len(timeTotal)-2)))})

	writeJson(nameFileResult,avgTime)

if __name__ == "__main__": main(sys.argv[1])
