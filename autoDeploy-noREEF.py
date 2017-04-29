
# coding: utf-8

# In[ ]:

#Algoritmo para teste do Serviço Myria
#Professores: Vanessa e Victor
#Aluno: Frank Willian
#Programa de Pós-Graduãção IC/UFF/NI-RJ-BR

import json
import subprocess as subp
import os
import time
import datetime
import matplotlib.pyplot as plt


# In[ ]:

#funcao para escrever em json
def writeJson(path,js):
	file = open(path, 'w')
	file.write(json.dumps(js,indent = 3))
	file.close()


# In[ ]:

#funcao para alterar o json da consulta
def alter_join(wn,cn,path):
	path = path+'jsonQueries/triangles_twitter/triangle_join.json'
	ingest = open(path,'r')
	data = json.load(ingest)
	ingest.close()
	for frag in data['fragments']:
		for op in frag['operators']:
			if (op['opType'] == 'TableScan'):
				frag['workers'] = wn
				break
			else:
				frag['workers'] = cn
			break
	writeJson(path,data)


# In[ ]:

#funcao para alterar o json do ingest
def alter_ingest(wn,path):
	path_ = path+'jsonQueries/triangles_twitter/ingest_twitter.json'
	ingest = open(path_,'r')
	data = json.load(ingest)
	ingest.close()
	data['workers'] = wn
	data['source']['filename'] = path+'jsonQueries/triangles_twitter/twitter_small.csv'
	writeJson(path_,data)


# In[ ]:

#funcao que alterar os json de ingest e query de acordo
#com o cenario
def ingest_and_query(schema,path):
	#ingest
	wn = list(range(1,schema['wn']+1))
	alter_ingest(wn,path)
	#join
	if (schema['cn']==0):
		cn = wn
	else:
		cn = list(range(1,schema['wn']+schema['cn']+1))
	alter_join(wn,cn,path)


# In[ ]:

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

# In[ ]:

#funcao que executa executa a query e retorna um json
def callQuery(cmd):
	pipe = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True,universal_newlines=True)
	while pipe.poll() is None:
		time.sleep(3)
	outPipe = pipe.stdout.read()
	out = json.loads(outPipe[outPipe.find('{',0):])
	return out

# In[ ]:

#função para capturar a informação inicial
def getInfo():
	#Captura lista de maquinas
	#getListMaq ='cat $PBS_NODEFILE'
	#machines = subp.Popen(getListMaq, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True,shell=True)
	#lMaq = machines.stdout.read()
	#lMaq = lMaq.replace('\n',' ').split()
	pwd = '/home_nfs/frankwrs/'
	lMaq = [line.rstrip('\n') for line in open(pwd+'listMaq.txt')]
	listWN = list(set(lMaq))

	#Define a primeira maquina da lista como master
	master = lMaq[0]

	#Remove o hostname (master) da lista de nós
	while master in lMaq:
		lMaq.remove(master)
	listWN.remove(master)

	#define o path myria_home
	path =pwd+'myria/'

	return listWN, lMaq, master, path, pwd


# In[ ]:

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

# In[ ]:

def plot(file):
    #Coletando json do arquivo de entrada
    resultOscar = open(file,'r')
    data = json.load(resultOscar)
    resultOscar.close()

    #Plot individual de cada cenario
    for schema in data:
        listTime = [t['time'] for t in schema['query']]
        listQuery = [t['queryId'] for t in schema['query']]
        fig = plt.figure(figsize=(8, 4))
        plt.ylabel('ID consulta')
        plt.xlabel('Tempo médio da consulta')
        plt.title('Cenário '+str(schema['schema'])+' - '+file)
        plt.barh(range(len(listTime)), listTime, color="green",align='center',height=0.3)
        plt.yticks(range(len(listTime)), listQuery)
        plt.xlim([min(listTime) - 0.5, max(listTime) + 0.3])
        plt.tight_layout()
        fig.savefig(file+'_wn-'+str(schema['schema']['wn'])+'cn-'+str(schema['schema']['cn'])+'.png',bbox_inches='tight')
        #plt.show()


    #Listas com tempos e esquemas
    listTime = [t['avgTime'] for t in data]
    listSchema = [s['schema'] for s in data]

    #Plot com todos cenários
    fig = plt.figure(figsize=(8, 4))
    plt.ylabel('Cenários')
    plt.xlabel('Tempo médio de 10 consultas')
    plt.title(file)
    plt.barh(range(len(listTime)), listTime, color="green",align='center',height=0.3)
    plt.yticks(range(len(listTime)), listSchema)
    plt.xlim([min(listTime) - 0.5, max(listTime) + 0.3])
    plt.tight_layout()
    fig.savefig(file+'.png',bbox_inches='tight')
    #plt.show()

# In[ ]:

def main():

	#Obtem lista de maquinas, hostname e path
	listWN, listMaq, master, path, pwd = getInfo()

	#Constroi os cenarios
	#Faz deploy do serviço para cada cenarios
	#Executa e faz media de tempo das consultas
	n = 2
	while n<=len(listMaq):

		port = 17001

		#Gera lista de nós/nucleos para o deploy
		listDeploy = []
		x = 0
		for x in range(0,n):
			listDeploy.append(str(x+1)+' = '+str(listWN[x%len(listWN)])+':'+str(port))
			port+=1

		#prepara o deploy
		prepareDeploy(path, master,listDeploy,17000)
		print("Path: ",path)
		print("Master: ",master)
		print("ListDeploy: ",listDeploy)
		print("ListWN: ",listWN)

		#gera cenarios
		wn = 2
		schemas = []
		while ((wn <= n) and (wn <= len(listWN))):
			data = {'cn':n-wn,'wn':wn}
			schemas.append(data)
			wn = wn * 2

		###Variaveis com comandos
		setup_cluster = ['./setup_cluster.py deployment.cfg']
		deploy = ['./launch_cluster.sh deployment.cfg']
		walive = ['curl '+master+':8753/workers/alive']
		ingest = ['curl -i -XPOST '+master+':8753/dataset -H \"Content-type: application/json\" -d @./ingest_twitter.json']
		query = ['curl -i -XPOST '+master+':8753/query -H \"Content-type: application/json\"  -d @./global_join.json']
		getQuery = ['curl -i -XGET '+master+':8753/query/query-']
		avgTime = []

		#para cada cenário gerado
		for s in schemas:

			#Limpa diretório tmp do master e de todos os nós
			subp.call('ssh '+master+' \'rm -rf /var/usuarios/frankwrs/myria-files/*\'', shell=True)
			for node in listWN:
				subp.call('ssh '+node+' \'rm -rf /var/usuarios/frankwrs/myria-files/*\'', shell=True)
			print(s)
			#seta o diretorio de deploy
			os.chdir(path+"myriadeploy/")
			#configura as máquinas para o deploy
			setupMyria = subp.Popen(setup_cluster, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True, shell=True)
			while setupMyria.poll() is None:
				print("Setup_cluster working...")
				time.sleep(5)

			#Faz deploy do myria com a quantidade de nós passada como argumento
			myriaDeploy = subp.Popen(deploy, stdout=subp.PIPE,stderr=subp.PIPE,shell=True)
			while myriaDeploy.poll() is None:
				print("Deploy working...")
				time.sleep(3);

			#captura os workers ativos
			ws = subp.Popen(walive, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True, shell=True)
			workers = ws.stdout.read()
			#print(workers)
			#testa se os workers estao todos ativos
			for x in range(1,n+1):
				if str(x) not in workers:
					print(workers)
					ws = subp.Popen(walive, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True,shell=True)
					workers = ws.stdout.read()

			print(workers)

			#seta o diretorio do ingest e query
			os.chdir(path+"jsonQueries/triangles_twitter/")

			#Altera os arquivos json de ingest e join
			ingest_and_query(s,path)

			#pipe Ingest
			outIngest = callIngest(ingest)
			print(outIngest)

			#pipe query
			queryResult = []
			while x < 1:
				outQuery = callQuery(query)
				#print(outQuery['status'])
				if outQuery['status'] == 'ACCEPTED':
					print(outQuery['status'])
					queryResult.append({'queryId': outQuery['queryId']})
					x+=1
			print(queryResult)
			'''
			#pipe query time
			for q in queryResult:
				getQuery = 'curl -i -XGET '+master+':8753/query/query-'+str(q['queryId'])
				outGetQuery = callQuery(getQuery)
				while (outGetQuery['status'] != 'SUCCESS'):
					print(outGetQuery)
					outGetQuery = callQuery(getQuery)
				#Subtração dos tempos de start e finish da query
				st = outGetQuery['startTime'][outGetQuery['startTime'].find('T')+1:].replace('Z','')
				st = datetime.datetime.strptime(st,"%H:%M:%S.%f")
				ft = outGetQuery['finishTime'][outGetQuery['finishTime'].find('T')+1:].replace('Z','')
				ft = datetime.datetime.strptime(ft,"%H:%M:%S.%f")
				q['time'] = (ft - st).total_seconds()

			#lista com a média de tempo das consultas pra cada cenário
			avgTime.append({'schema': s, 'query': queryResult, 'avgTime': float("{0:.3f}".format(sum(q['time'] for q in queryResult) / len(queryResult)))})

			#Mata processo de deploy e java levantados
			myriaDeploy.terminate()

		'''

		os.chdir(path+"myriadeploy/")
		subp.call('./stop_all_by_force.py deployment.cfg', shell=True)

		n = n * 2

		#n = len(listMaq)+1

	#imprime a lista de cenários com a média
	#de tempo das rodadas de consultas para
	#cada cenário
	#print(avgTime)

	#salva resultado em arquivo json
	#nameFileResult = pwd+'Results/resulOscar_'+str(time.strftime("%d-%b-%Y-%Hh%Mm%Ss")+'.json'
	#writeJson(nameFileResult,avgTime)
	#plot(nameFileResult)


# In[ ]:

if __name__ == "__main__": main()
