
# coding: utf-8

import json
import subprocess as subp
import os 
import time
import datetime
import argparse
import signal

#funcao para escrever em json
def writeJson(path,js):
    file = open(path, 'w')
    file.write(json.dumps(js,indent = 3))
    file.close()

#funcao para alterar o json da consulta
def alter_join(tb,w,path):
    path = path+'jsonQueries/triangles_twitter/global_join.json'
    ingest = open(path,'r')
    data = json.load(ingest)
    ingest.close()
    for frag in data['plan']['fragments']:
        for op in frag['operators']:
            if (op['opType'] == 'TableScan'):
                frag['workers'] = tb
                break
            else:
                frag['workers'] = w
                break
    writeJson(path,data)

#funcao para alterar o json do ingest
def alter_ingest(w,path):
    path_ = path+'jsonQueries/triangles_twitter/ingest_twitter.json'
    ingest = open(path_,'r')
    data = json.load(ingest)
    ingest.close()
    data['workers'] = w
    data['source']['filename'] = path+'jsonQueries/triangles_twitter/twitter_small.csv'
    writeJson(path_,data)

#funcao para gerar os cenarios de acordo com a
#quantidade de nós
def get_schema(n):
    schemas = []
    wd = 2
    while(wd <= n):
        data = {}
        data['w'],data['wd'] = n,wd
        schemas.append(data)
        wd = wd * 2
    return schemas

#funcao que alterar os json de ingest e query de acordo
#com o cenario
def ingest_and_query(schema,path):
    #ingest
    wd = list(range(1,schema['wd']+1))
    alter_ingest(wd,path)
    #worker
    w = list(range(1,schema['w']+1))
    alter_join(wd,w,path)        

#funcao que executa um comando shell e retorna um json
def callPopen(cmd):
    pipe = subp.Popen(cmd, stdout=subp.PIPE,stderr=subp.PIPE,shell=True,universal_newlines=True)
    time.sleep(10)
    outPipe = pipe.stdout.read()
    out = json.loads(outPipe[outPipe.find('{',0):])
    return out

#funcao que le o arquivo de entrada e retorna 
#o path e a lista de workers
def prepareDeploy(nfile):
    entrada = []
    with open('entrada.txt','r') as file:
        for line in file:
            entrada.append(line.replace('\n',''))

    path = entrada[0]

    file = open(path+'myriadeploy/deployment.cfg.local', 'r')
    lines = file.readlines()
    file.close()
    
    del lines [lines.index('[workers]\n')+1:lines.index('[runtime]\n')-1]
    
    for l in entrada[1:]:
        lines.insert(entrada.index(l)+lines.index('[workers]\n'),l+'\n')

    file = open(path+'myriadeploy/deployment.cfg.local', 'w')
    file.write(''.join(lines))
    file.close()
    
    return path,len(entrada)-1

def main():
    #Captura argumento de entrada
	parser = argparse.ArgumentParser()
	parser.add_argument("inputFile")
	args = parser.parse_args()

	#testa se entrada existe
	if os.path.isfile(args.inputFile):
		#define comandos e variaveis
	    path, n = prepareDeploy(args.inputFile)
	    deploy = ['./launch_local_cluster',str(n)]
	    walive = ['curl', 'localhost:8753/workers/alive']
	    ingest = ['curl -i -XPOST localhost:8753/dataset -H \"Content-type: application/json\" -d @./ingest_twitter.json']
	    query = ['curl -i -XPOST localhost:8753/query -H \"Content-type: application/json\"  -d @./global_join.json']
	    getQuery = ['curl -i -XGET localhost:8753/query/query-']
	    #gera cenarios
	    schemas = get_schema(n)
	    avgTime = []
	    #para cada cenário gerado
	    for s in schemas:
	    	#limpa a pasta temp do myria
	        subp.call('rm -Rf /tmp/myria/*', shell=True)
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
	            #Adiciona lista com os tempos de cada consulta
	            avgTime.append({'schema': s, 'query': q})

	        #lista com a média de tempo das consultas pra cada cenário
	        avgTime.append({'schema': s,'avgTime:': float("{0:.3f}".format(sum(q['time'] for q in queryResult) / len(queryResult)))})

	        #Mata processo de deploy e java levantados
	        myriaDeploy.terminate()
	        subp.call('killall java', shell=True)

	    #imprime a lista de cenários com a média
	    #de tempo das rodadas de consultas para
	    #cada cenário
	    print(avgTime)
	    
	    #salva resultado em arquivo
	    file = open(path+'result.txt', 'w')
	    file.write(''.join(str(line)+'\n' for line in avgTime))
	    file.close()
	else:
		print('Arquivo de entrada não encontrado!')

if __name__ == "__main__": main()

