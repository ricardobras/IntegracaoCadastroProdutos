import cx_Oracle
from datetime import date
from Variaveis import Variaveis as var

def connectOracleCooper():
	 
	return cx_Oracle.connect(var.dbOracleCooper)
def connectOracleCrv():
 
	return cx_Oracle.connect(var.dbOracleCrv)

class Produto:
	codproduto=0
	descricao=''
	compl1=''
	compl2=''
	princAtivo=''
	tipoProduto=''
	bloqueado=''
	dtCadastro=''
	ncm=''
	marca=''
	unidCompra=''
	unidConsumo=''
	ccusto=''
	grupo=''
	ordproducao=''
	opEntrada=''
	empresaChb=''
	empresaCCusto=''
	empresaGrupo=''
	tipoConversao='' #TIPO DE CONVERS?O (S=SEM CONVERS?O)
	codComprador='' #VARIA POR EMPRESA
	empresaFornecedor='' #na base CRV = 1, na COOPER cada um tem a sua
	descricaoCompleta=None
	co13rastre='N'
	genero=''
	movconsumoautomatico=0
	docconsumoautomatico=None
	pedidolib=str(1)

	def inserirProduto(empresa, prod):
		##define a empresa com base no sistema de cadastro web
		if empresa == "CRV":
			connection=connectOracleCrv()
		if empresa == "COOPER":
			connection=connectOracleCooper()

		cursor = connection.cursor()#Execute Query
		
		#VERIFIA A EXISTENCIA DO PRODUTO NO BD ORACLE
		sqlChb = "SELECT * FROM co13t WHERE co13emp06 = :1 AND co13codpro = :2"
		consultaCHB = cursor.execute(sqlChb,(prod.empresaChb,prod.codproduto))
		qtdExistente = len(consultaCHB.fetchall())
		sqlProduto = ""
		msg=""
		print("produto: "+str(prod.codproduto)+" empresa:"+str(prod.empresaChb))
		if qtdExistente > 0:
			print ("Produto ja existe, ser? Atualizado no bd")
			sqlProduto = """UPDATE co13t set co13codpro=:1,co13descri=:2,co13desc01=:3,co13desc02=:4,
			co13tippro=:5,co13bloq=:6,co13dtacad=:7,co13nbm=:8,
			co13codmar=:9,co13undcom=:10,co13undcon=:11,
			co13c_cust=:12,co13grupo=:13,co13ordpro=:14,
			co13codent=:15,co13emp06=:16,co13emp01=:17,
			co13emp06a=:18,co13tipcal=:19,co13codcom=:20,
			co13empfor=:21,co13dtinse=:22,co13empgra=:23,
			co13descrx=:24,co13rastre=:25,co13desetq=:26,
			co13portam=:27,co13genero=:28,co13codcns=:29,
			co13tipdoc=:30,co13codmaq=:31,co13progen=:32,co13pedlib=:33
			where co13emp01=:17 and co13codpro=:1"""
			
			msg+="produto "+str(prod.codproduto)+" Atualizado "+str(date.today())+"\n"

		else:
			print("Produto n?o existe, ser? inserido no bd")
			sqlProduto = """INSERT INTO co13t
			(co13codpro,co13descri,co13desc01,co13desc02,
			co13tippro,co13bloq,co13dtacad,co13nbm,
			co13codmar,co13undcom,co13undcon,
			co13c_cust,co13grupo,co13ordpro,
			co13codent,co13emp06,co13emp01,
			co13emp06a,co13tipcal,co13codcom,
			co13empfor,co13dtinse,co13empgra,
			co13descrx,co13rastre,co13desetq,
			co13portam,co13genero,co13codcns,
			co13tipdoc,co13codmaq,co13progen,co13pedlib) 
			 VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,
			:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,
			:29,:30,:31,:32,:33)"""
			
			msg+="produto "+str(prod.codproduto)+" inserido "+str(date.today())+"\n"
			
			#Inserir grade se o produto nao existir na base
			if qtdExistente==0:
				prod.inserirGrade(prod,cursor)
			
			
			 
		cursor.execute(sqlProduto,(
						prod.codproduto, 
						prod.descricao, 
						prod.compl1,
						prod.compl2,
						prod.tipoProduto,
						prod.bloqueado,
						prod.dtCadastro,
						prod.ncm,
						prod.marca,
						prod.unidCompra,
						prod.unidConsumo,
						prod.ccusto,
						prod.grupo,
						prod.ordproducao,
						prod.opEntrada,
						prod.empresaChb,
						prod.empresaCCusto,
						prod.empresaGrupo,
						prod.tipoConversao, 
						prod.codComprador, 
						prod.empresaFornecedor,
						prod.dtCadastro,
						prod.empresaChb,
						prod.descricaoCompleta,
						prod.co13rastre,
						' ',
						'N',
						prod.genero,
						prod.movconsumoautomatico,
						Produto.docconsumoautomatico,
						0,
						'N',
						prod.pedidolib))

		connection.commit()
		#Close Cursor and Connection
		cursor.close()
		connection.close()
		return msg

	def inserirGrade(prodt,cursor):
	#inserir os dados na tabela de grade
		SqlInsertGrade="""INSERT INTO co13t3 (co13emp06,
										co13codpro,
										co13codemp,
										co13tamanh,
										co13barra,
										co13qtltet,
										co13qtltct,
										co13qtmax,
										co13peso,
										co13ponrep,
										co13qtdmis,
										co13qtdmas,
										co13qtdmie,
										co13qtdmae,
										co13qtdprs,
										co13qtdpre)
 		VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)"""

		cursor.execute(SqlInsertGrade,(prodt.empresaChb,prodt.codproduto,prodt.empresaChb,0,0,0,0,0,0,0,0,0,0,0,0,0))
