import pymysql as mysql
from Produto import Produto as prod
from Variaveis import Variaveis as var
import logging
from datetime import date
agora = date.today()
logging.basicConfig(filename='example2.log',
level=logging.DEBUG)
def main():
	conn = mysql.connect(host=var.host, port=var.port, user=var.user, passwd=var.passwd, db=var.db)
	sqlUpdateProdutoStatus = """UPDATE produto_detalhes set sincronizado='S' where produto_codigo=%s and empresa_id=%s"""
	cur = conn.cursor()
	result=[]
	with conn.cursor() as cur:
		cur.execute("SELECT * FROM empresa")
		for row in cur:
			result.append(list(row))
		for row in result:
			cur.execute('SELECT * FROM vw_webservice where empresa_id='+str(row[0]))
			for row2 in cur:
				prod.codproduto = int(str(row2[0])+""+str(row2[1]))
				prod.descricao = row2[2]
				prod.compl1=row2[3]
				prod.compl2=row2[4]
				prod.princAtivo=prod.codproduto
				prod.tipoProduto=row2[6]
				prod.bloqueado=row2[7]
				prod.dtCadastro=row2[10]
				prod.ncm=row2[15]
				prod.marca=row2[16]
				prod.unidCompra=row2[21]
				prod.unidConsumo=row2[22]
				prod.ccusto=row2[23]
				prod.grupo=row2[24]
				prod.ordproducao=row2[25]
				prod.opEntrada=row2[26]
				prod.empresaChb=row2[32]
				prod.empresaCCusto=row2[32]
				prod.empresaGrupo=row2[32]
				prod.tipoConversao="S"
				prod.codComprador=row2[37]
				prod.empresaFornecedor=row2[32]
				prod.dtCadastro=row2[10]
				prod.empresaChb=row2[32]
				prod.descricaoCompleta=row2[2]+" "+row2[3]+" "+row2[4]
				prod.co13rastre="N"
				prod.genero=row2[36]
				prod.movconsumoautomatico=row2[28]
				prod.docconsumoautomatico=row2[29]
				prod.codmaquina=0
				prod.produtogen=str("N")
				prod.pedidolib=12345
				empresaDatabase = str(row2[31])
				print("EMPRESA: "+row[3] + " - "+row2[2])
				logging.debug('%s' % (prod.inserirProduto(empresaDatabase,prod)))#PASSA O NOME DA EMPRESA COMO PARAMETRO PARA A CONDI??O IF
				cur2 = conn.cursor()
				cur2.execute(sqlUpdateProdutoStatus,(row2[20],row2[19]))		
				conn.commit()
				cur2.close()
				print("PRODUTO: "+str(row2[19]))
		conn.commit()
		cur.close()
	conn.close()

if __name__ == '__main__':
	main()