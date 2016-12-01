#Algoritmo para teste do Serviço Myria
#Professores: Vanessa e Victor
#Aluno: Frank Willian
#Programa de Pós-Graduãção IC/UFF-NI/RJ-BR

import json
import subprocess as subp
import os 
import time
import datetime

#funcao para escrever em json
def writeJson(path,js):
    file = open(path, 'w')
    file.write(json.dumps(js,indent = 3))
    file.close()

#funcao para alterar o json da consulta
def alter_join(dn,wn,path):
    path = path+'jsonQueries/triangles_twitter/global_join.json'
    ingest = open(path,'r')
    data = json.load(ingest)
    ingest.close()
    for frag in data['plan']['fragments']:
        for op in frag['operators']:
            if (op['opType'] == 'TableScan'):
                frag['workers'] = dn
                break
            else:
                frag['workers'] = wn
                break
    writeJson(path,data)

#funcao para alterar o json do ingest
def alter_ingest(dn,path):
    path_ = path+'jsonQueries/triangles_twitter/ingest_twitter.json'
    ingest = open(path_,'r')
    data = json.load(ingest)
    ingest.close()
    data['workers'] = dn
    data['source']['filename'] = path+'jsonQueries/triangles_twitter/twitter_small.csv'
    writeJson(path_,data)

#funcao que alterar os json de ingest e query de acordo
#com o cenario
def ingest_and_query(schema,path):
	#ingest
	dn = list(range(1,schema['dn']+1))
	alter_ingest(dn,path)
	#join
	if (schema['wn']==0):
		wn = dn
	else: 
		wn = list(range(1,schema['dn']+schema['wn']+1))

	alter_join(dn,wn,path)        

#funcao que executa um comando shell e retorna um json
def callPopen(cmd):
    pipe = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True,universal_newlines=True)
    time.sleep(10)
    outPipe = pipe.stdout.read()
    out = json.loads(outPipe[outPipe.find('{',0):])
    return out

#função para capturar a informação inicial
def getInfo():
	#Captura lista de maquinas
	getListMaq ='cat $PBS_NODEFILE'
	machines = subp.Popen(getListMaq, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True,shell=True)
	lMaq = machines.stdout.read()
	lMaq = lMaq.replace('\n',' ').split()
	listDN = list(set(lMaq))

	#captura o nome do host
	hostname ='hostname'
	getHost = subp.Popen(hostname, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True,shell=True)
	hostname = getHost.stdout.read()
	hostname = hostname.replace('\n','')

	#Remove o hostname (master) da lista de nós
	while hostname in lMaq:
		lMaq.remove(hostname)
	listDN.remove(hostname)

	#captura o path
	path ='pwd'
	getPath = subp.Popen(path, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True,shell=True)
	path = getPath.stdout.read()
	path = path.replace('\n','')+'/myria/'

	return lMaq, listDN, hostname, path

#funcao que le o arquivo de entrada e retorna 
#o path e a lista de workers
def prepareDeploy(path, master,listDeploy,port):
	#Captura arquivo de deploy	
	file = open(path+'myriadeploy/deployment.cfg.local', 'r')
	lines = file.readlines()
	file.close()

	#altera master
	del lines [lines.index('[master]\n')+1:lines.index('[workers]\n')-1]
	lines.insert(lines.index('[master]\n')+1,'0 = '+master+':'+str(port)+'\n')

	#altera workers
	del lines [lines.index('[workers]\n')+1:lines.index('[runtime]\n')-1]
	x = 1
	for w in listDeploy:
		lines.insert(x+lines.index('[workers]\n'),w+'\n')
		x += 1

	#Grava arquivo de deploy
	file = open(path+'myriadeploy/deployment.cfg.local', 'w')
	file.write(''.join(lines))
	file.close()

def main():

	#Obtem lista de maquinas, hostname e path
	listDN, listMaq, hostname, path = getInfo()

	#Constroi os cenarios
	#Faz deploy do serviço para cada cenarios
	#Executa e faz media de tempo das consultas
	n = 2
	while n<=len(listMaq):
		
		port = 17001

		#Gera lista de nós/nucleos para o deploy
		listDeploy = []
		x = 0
		while (x<n):
			listDeploy.append(str(x+1)+' = '+str(listDN[x%len(listDN)])+':'+str(port))            
			x+=1
			port+=1	

		#prepara o deploy
		prepareDeploy(path, hostname,listDeploy,17000)

		#gera cenarios 
		dn = 2
		schemas = []
		while (dn <= n):
			data = {'wn':n-dn,'dn':dn}
			schemas.append(data)
			dn = dn * 2

		###Variaveis com comandos
		deploy = ['./launch_local_cluster',str(n)]
		walive = ['curl', 'localhost:8753/workers/alive']
		ingest = ['curl -i -XPOST localhost:8753/dataset -H \"Content-type: application/json\" -d @./ingest_twitter.json']
		query = ['curl -i -XPOST localhost:8753/query -H \"Content-type: application/json\"  -d @./global_join.json']
		getQuery = ['curl -i -XGET localhost:8753/query/query-']
		avgTime = []

		#para cada cenário gerado
		for s in schemas:
			
			#Limpa diretório tmp do master e de todos os nós
			subp.call('rm -Rf /tmp/myria/*', shell=True)
			for n in listDN:
				subp.call('ssh '+n+' \'rm -Rf /tmp/myria/*\'', shell=True)

			#seta o diretorio de deploy
			os.chdir(path+"myriadeploy/")
			#Faz deploy do myria com a quantidade de nós passada como argumento
			myriaDeploy = subp.Popen(deploy, stdout=subp.PIPE,stderr=subp.PIPE)
			#tempo de espera do deploy
			time.sleep(40)
			#captura os workers ativos
			ws = subp.Popen(walive, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True)
			workers = ws.stdout.read()
			w = str(list(range(1,n+1))).replace(' ','')
			#testa se os workers estao todos ativos
			while (w not in str(workers)):
				ws = subp.Popen(walive, stdout=subp.PIPE,stderr=subp.PIPE,universal_newlines=True)
				workers = ws.stdout.read()
			#seta o diretorio do ingest e query
			os.chdir(path+"jsonQueries/triangles_twitter/")

			#Altera os arquivos json de ingest e join
			ingest_and_query(s,path)

			#pipe Ingest
			outIngest = callPopen(ingest)

			#pipe query
			x = 0
			queryResult = []
			while x < 5:
				outQuery = callPopen(query)
				#print(outQuery['status'])
				if outQuery['status'] == 'ACCEPTED':
					queryResult.append({'queryId': outQuery['queryId']})
					x+=1

			#pipe query time
			for q in queryResult:
				getQuery = "curl -i -XGET localhost:8753/query/query-"+str(q['queryId'])
				outGetQuery = callPopen(getQuery)

				while (outGetQuery['status'] != 'SUCCESS'):
					outGetQuery = callPopen(getQuery)
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
			subp.call('killall java', shell=True)

		n = n * 2

	#imprime a lista de cenários com a média
	#de tempo das rodadas de consultas para
	#cada cenário
	print(avgTime)

	#salva resultado em arquivo json
	writeJson(path+'result.json',avgTime)


if __name__ == "__main__": main()

