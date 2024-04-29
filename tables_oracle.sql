create table qs_funcionarios(
 numfunc                   varchar2(40)  
,numvinc                   varchar2(40)  
,mesanoreferencia          date          
,categoria                 varchar2(20)  
,nomecategoria             varchar2(60)  
,cargahoraria              varchar2(20)  
,nomecargahoraria          varchar2(50)  
,cargo                     varchar2(40)  
,nomecargo                 varchar2(60)  
,estadocivil               varchar2(60)  
,faixaetaria               varchar2(40)  
,descrfaixaetaria          varchar2(60)  
,salario                   number        
,faixasalarial             varchar2(40)  
,descrfaixasalarial        varchar2(60)  
,genero                    varchar2(20)  
,grauinstrucao             varchar2(20)  
,descrgrausinstrucao       varchar2(100) 
,grupograuinstrucao        varchar2(60)  
,setor                     varchar2(15)  
,nomesetor                 varchar2(50)  
,nacionalidade             varchar2(100) 
,naturalidade              varchar2(3)   
,empresa                   varchar2(40)  
,nomeempresa               varchar2(60)  
,subempresa                varchar2(40)  
,nomesubempresa            varchar2(50)  
,referencia                varchar2(10)  
,regimejuridico            varchar2(20)  
,nomeregimejuridico        varchar2(60)  
,subcategoria              varchar2(20)  
,nomesubcategoria          varchar2(60)  
,situacao                  varchar2(60)  
,tipovinculo               varchar2(20)  
,nometipovinculo           varchar2(60)  
,dtexerc                   date          
,dtvac                     date          
,dtaposent                 date          
,idade                     varchar2(40)  
,raca                      varchar2(100) 
,deficiente                varchar2(1)   
,tipodefic                 varchar2(2)   
,descrtipodefic            varchar2(100) 
,formavac                  varchar2(20)  
,descrformavac             varchar2(60)  
,guid_subempresa           varchar2(32)  
,codigonomesetor           varchar2(68)  
,codigonomecargo           varchar2(103) 
,admitido                  char(1)       
,aposentado                char(1)
,tenantid                  number) 
/
CREATE INDEX idx_tenantid ON qs_funcionarios(tenantid)
/
create table qs_freq_afast(    
 numfunc                number(9)    
,numvinc                number(8)    
,mesanoreferencia       date         
,origem                 varchar2(30) 
,dtini                  date         
,dtfim                  date         
,qtd_dias               number       
,quantidade             number       
,aberto                 char(1)      
,tipofreq               varchar2(20) 
,nometipofreq           varchar2(60) 
,codfreq                number(3)    
,nomecodfreq            varchar2(60) 
,mnemonicofreq          varchar2(3)
,tenantid               number)
/
CREATE INDEX idx_qs_freq_afast ON qs_freq_afast(tenantid)
/
create table qs_pagamento_cabec( 
 ficha             number(14)   
,numfunc           number(9)    
,numvinc           number(8)    
,numpens           number(2)    
,folha             number(8)    
,mesanofolha       date         
,tipofolha         varchar2(20)
,tenantid          number)
/
CREATE INDEX idx_qs_pagamento_cabec ON qs_pagamento_cabec(tenantid)
/
create table qs_pagamento_rubricas( 
 ficha                      number(14)   
,fator                      varchar2(25) 
,sinal                      number(4)    
,fat_vant                   number(4)    
,classificacaorubrica       varchar2(20) 
,mesanoreferencia           date         
,rubrica                    number(8)    
,mnemonicorubrica           varchar2(20) 
,nomerubrica                varchar2(60) 
,rub                        varchar2(10) 
,tiporubrica                varchar2(20) 
,valorrubrica               number(11,2) 
,valorrubricacalc           number(11,2)
,tenantid                   number)
/
CREATE INDEX idx_qs_pagamento_rubricas ON qs_pagamento_rubricas(tenantid)
/
