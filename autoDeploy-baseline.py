#Algoritmo para teste do Serviço Myria
#Professores: Vanessa e Victor
#Aluno: Frank Willian
#Programa de Pós-Graduãção IC/UFF/NI-RJ-BR

import json
import subprocess as subp
import os
import time
import datetime

#funcao para escrever em json
def writeJson(path,js):
	file = open(path, 'w')
	file.write(json.dumps(js,indent = 3,sort_keys=True))
	file.close()

#funcao para alterar o json da consulta
def alter_join(dn,wn,path,fileQuery):
	path = path+'jsonQueries/triangles_twitter/'+fileQuery
	ingest = open(path,'r')
	data = json.load(ingest)
	ingest.close()
	for frag in data['fragments']:
		for op in frag['operators']:
			if (op['opType'] == 'CollectConsumer'):
				break
			elif (op['opType'] == 'TableScan'):
				frag['workers'] = dn
				break
			else:
				frag['workers'] = wn
			break
	writeJson(path,data)

#funcao para alterar o json do ingest
def alter_ingest(dn,path,fileDataset):
	path_ = path+'jsonQueries/triangles_twitter/ingest_twitter.json'
	ingest = open(path_,'r')
	data = json.load(ingest)
	ingest.close()
	data['workers'] = dn
	data['source']['filename'] = path+'jsonQueries/triangles_twitter/'+fileDataset
	writeJson(path_,data)

#funcao que alterar os json de ingest e query de acordo
#com o cenario
def ingest_and_query(schema,path,fileQuery,fileDataset):
	#ingest
	dn = list(range(1,schema['dn']+1))
	alter_ingest(dn,path,fileDataset)
	#join
	if (schema['wn']==0):
		wn = dn
	else:
		wn = list(range(1,schema['dn']+schema['wn']+1))
	alter_join(dn,wn,path,fileQuery)

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
	while pipe.poll() is None:
		time.sleep(3)
	outPipe = pipe.stdout.read()
	if '{' in outPipe:
		out = json.loads(outPipe[outPipe.find('{',0):])
		return out
	else:
		return outPipe

#função para capturar a informação inicial
def getInfo():
	pwd = '/home_nfs/frankwrs/'
	lMaq = [line.rstrip('\n') for line in open(pwd+'listMaq.txt')]
	listDN = list(set(lMaq))

	#Define a primeira maquina da lista como master
	master = lMaq[0]

	#Remove o hostname (master) da lista de nós
	while master in lMaq:
		lMaq.remove(master)
	listDN.remove(master)

	#define o path myria_home
	path =pwd+'myria/'

	return listDN, lMaq, master, path, pwd

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

def getBaselineSchemas(qtdM):
	schemas = []
	x = 2
	while x <= qtdM:
	    dn = 2
	    while ((dn <= x) and (dn <= qtdM)):
	        data = {'wn':x-dn,'dn':dn,'m':x}
	        schemas.append(data)
	        dn = dn * 2
	    x = x * 2
	return schemas

def main():

	#Obtem lista de maquinas, hostname e path
	listDN, listMaq, master, path, pwd = getInfo()
	print("Path: ",path)
	print("Master: ",master)
	print("ListDN: ",listDN)

	#Dfine nome dos arquivos de consulta e dataset
	fileQuery = 'twitter_selfjoin-count.json'
	fileDataset = 'twitter_small.csv'

	#Define nome do arquivo com resultados
	nameFileResult = pwd+'Results/baseline_'+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss"))+'_nodes'+str(len(listDN))+'-cpn'+str(len(listMaq)/len(listDN))+'_DS-'+fileDataset.strip('.csv')+'_Q-'+fileQuery.strip('.json')+'.json'

	###Variaveis com comandos
	setup_cluster = ['./setup_cluster.py deployment.cfg']
	deploy = ['./launch_cluster.sh deployment.cfg']
	walive = ['curl '+master+':8753/workers/alive']
	ingest = ['curl -i -XPOST '+master+':8753/dataset -H \"Content-type: application/json\" -d @./ingest_twitter.json']
	query = ['curl -i -XPOST '+master+':8753/query -H \"Content-type: application/json\" -d @./'+fileQuery]
	getQuery = ['curl -i -XGET '+master+':8753/query/query-']

	#Constroi os cenarios
	#Faz deploy do serviço para cada cenario
	#Executa e faz media de tempo das consultas

	#gera cenarios
	schemas = getBaselineSchemas(len(listDN))
	avgTime = []
	for s in shemas:
		avgTime.append(({'schema': s,'query': [])

	for x in range(1,6):

		for s in avgTime:

			print(s)

			port = 17001

			#Gera lista de nós/nucleos para o deploy
			listDeploy = []
			t = 0
			for i in range(0,s['schema']['m']):
				listDeploy.append(str(t+1)+' = '+str(listDN[t%len(listDN)])+':'+str(port))
				port+=1

			#prepara o deploy
			print("ListDeploy: ",listDeploy)
			prepareDeploy(path, master,listDeploy,17000)

			#Faz deploy do myria
			errorDeploy = True
			while errorDeploy:
				#Mata processo de deploy e java levantados
	        	os.chdir(path+"myriadeploy/")
	        	cmd = ['./stop_all_by_force.py deployment.cfg']
		        killProcess = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
	            while killProcess.poll() is None:
	                print("Matando processos Myria...")
	                time.sleep(3)

				#Limpa diretório tmp do master e de todos os nós
				cmd = ['ssh '+master+' \'rm -rf /var/usuarios/frankwrs/*\'']
				cleanMaster = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
				while cleanMaster.poll() is None:
					print("Apagando arquivos temporários master...")
					time.sleep(3)

	            for node in listDN:
					#Limpa diretório tmp do master e de todos os nós
	        		cmd = ['ssh '+node+' \'rm -rf /var/usuarios/frankwrs/*\'']
	        		cleanNodes = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
	        		while cleanNodes.poll() is None:
	                		print("Apagando arquivos temporários worker "+node+"...")
	                		time.sleep(3)

				#seta o diretorio de deploy
				os.chdir(path+"myriadeploy/")
				#configura as máquinas para o deploy
				setupMyria = subp.Popen(setup_cluster, stdout=subp.PIPE,stderr=subp.PIPE, shell=True)
				while setupMyria.poll() is None:
					print("Setup_cluster working "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss"))+"...")
					time.sleep(5)
					saida = setupMyria.stdout.read()

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
				for i in range(1,n+1):
					if str(i) not in workers:
						print("Worker ",i," not alive only ",workers)
					else:
						w += 1

				if w == n: errorDeploy = False

			print("Workers alive: "+workers)

			#seta o diretorio do ingest e query
			os.chdir(path+"jsonQueries/triangles_twitter/")

			#Altera os arquivos json de ingest e join
			ingest_and_query(s,path,fileQuery,fileDataset)

			print "Start ingest"
			#pipe Ingest
			outIngest = callIngest(ingest)
			while isinstance(outIngest,str):
				print("ERRO INGEST\n"+outIngest)
				outIngest = callIngest(ingest)
			print "Finish ingest"

			#pipe query
			queryID = []
			for t in range(1,3):
				#Inicio da submissão de consulta
				print "Submit query ",x,t
				queryERROR = True
				while queryERROR:
					outQuery = callQuery(query)
					#print(outQuery)
					if not isinstance(outQuery,str):
						if outQuery["status"] == "ACCEPTED":
							#print(outQuery['status'])
							queryID.append(outQuery['queryId'])
							queryERROR = False
						else:
							print("QUERY NOT SUCESS: "+outQuery['status'])
					else:
						print("QUERY ERROR: \n"+outQuery)

			t = 1
			timeQ = {}
			for q in queryID:
				#Inicio da submissão de consulta
				print "get query time ",x,t
				queryERROR = True
				while queryERROR:
					#pipe query time
					getQuery = 'curl -i -XGET '+master+':8753/query/query-'+q
					outGetQuery = callQuery(getQuery)
					#print(outGetQuery)

					while (outGetQuery['status'] != 'SUCCESS'):
						print("Status query: "+outGetQuery['status']+" - "+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss")))
						if outGetQuery['status'] == 'SUCCESS':
							queryERROR = False
						elif outGetQuery['status'] == 'RUNNING':
							time.sleep(5)
							outGetQuery = callQuery(getQuery)
						elif outGetQuery['status'] == 'ERROR':
							queryERROR = True
							break

					if not queryERROR:
						#Subtração dos tempos de start e finish da query
						st = outGetQuery['startTime'][outGetQuery['startTime'].find('T')+1:outGetQuery['startTime'].find('-',10)]
						st = datetime.datetime.strptime(st,"%H:%M:%S.%f")
						ft = outGetQuery['finishTime'][outGetQuery['finishTime'].find('T')+1:outGetQuery['finishTime'].find('-',10)]
						ft = datetime.datetime.strptime(ft,"%H:%M:%S.%f")
						if t > 1:
							timeQ['cache'] = (ft - st).total_seconds()
						else:
							timeQ['nocache'] = (ft - st).total_seconds()
				t+=1

			s['query'].append({'id':x,'time':timeQ})

			#Mata processo de deploy e java levantados
        	os.chdir(path+"myriadeploy/")
        	cmd = ['./stop_all_by_force.py deployment.cfg']
        	killProcess = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
        	while killProcess.poll() is None:
        	        print("Matando processos Myria...")
        	        time.sleep(3)

			#lista com a média de tempo das consultas pra cada cenário

			#Salva resultados no cenário no arquivo
			writeJson(nameFileResult,avgTime)

if __name__ == "__main__": main()
